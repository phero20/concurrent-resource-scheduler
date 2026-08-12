package scheduler

import (
	"hash/fnv"

	"github.com/phero20/concurrent-resource-scheduler/acquire"
	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
)

// Acquire retrieves the highest-priority active resource from the scheduler.
//
// The configured [acquire.AcquireStrategy] selects the starting Heap Shard.
// If that shard is empty or its top resource is not available, Acquire
// tries remaining shards in sequential order, wrapping around if necessary,
// until every shard has been visited exactly once. It returns
// [errors.ErrNoResourceAvailable] if all shards are empty.
//
// Under [config.Shared] policy, the resource remains ACTIVE after Acquire;
// concurrent callers may acquire the same resource simultaneously. Under
// [config.Exclusive] policy, the resource is atomically moved to the Inactive
// Store and cannot be acquired again until [Scheduler.Release] is called.
//
// Acquire returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
// It returns [errors.ErrInvalidAcquireStrategy] if the configured strategy
// returns an out-of-range shard index.
//
// # Concurrency
//
// Acquire is safe for concurrent use by multiple goroutines. It locks at most
// one Heap Shard at a time, distributing contention across shards.
//
// Complexity: O(H) for Shared policy; O(log N + H) for Exclusive policy,
// where H is HeapCount and N is resources per shard.
func (s *Scheduler[T, ID]) Acquire() (T, error) {
	var zero T
	if s.closed.Load() {
		return zero, errors.ErrSchedulerClosed
	}

	view := shardView[T, ID]{s: s}

	// AcquireStrategy selects the starting shard.
	startShardID := s.cfg.AcquireStrategy.Select(view)
	if startShardID < 0 || startShardID >= len(s.shards) {
		return zero, errors.ErrInvalidAcquireStrategy
	}

	return s.acquireFromStartShard(startShardID)
}

// AcquireByAffinity retrieves a resource deterministically based on an affinity
// identifier, bypassing the configured [acquire.AcquireStrategy].
//
// AcquireByAffinity uses an internal [acquire.ConsistentHashRing] to map the
// identifier to a specific starting Heap Shard. The same identifier always
// maps to the same starting shard, enabling sticky-session routing. If that
// shard is empty, AcquireByAffinity falls back to adjacent shards in order,
// identical to [Scheduler.Acquire]'s fallback behavior.
//
// AcquireByAffinity returns [errors.ErrNilAffinityIdentifier] if key is nil.
// It returns [errors.ErrNoResourceAvailable] if all shards are empty.
// It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// AcquireByAffinity is safe for concurrent use by multiple goroutines.
// The ConsistentHashRing lookup is allocation-free and read-optimized;
// only the target Heap Shard is locked during the operation.
//
// Complexity: O(log V + log N) where V is virtual node count and N is
// resources per shard.
func (s *Scheduler[T, ID]) AcquireByAffinity(key acquire.AffinityIdentifier) (T, error) {
	var zero T
	if key == nil {
		return zero, errors.ErrNilAffinityIdentifier
	}
	if s.closed.Load() {
		return zero, errors.ErrSchedulerClosed
	}

	var buf [64]byte // Stack allocated buffer
	b := key.AppendAffinityBytes(buf[:0])

	h := fnv.New64a()
	_, _ = h.Write(b) // hash/fnv Write never returns an error
	affinityHash := h.Sum64()

	startShardID := s.affinityRing.GetShard(affinityHash)

	return s.acquireFromStartShard(startShardID)
}

// acquireFromStartShard is the internal acquisition loop shared by all public acquire methods.
func (s *Scheduler[T, ID]) acquireFromStartShard(startShardID int) (T, error) {
	var zero T

	// Try up to len(s.shards) times sequentially, ensuring every shard is visited exactly once.
	for i := 0; i < len(s.shards); i++ {
		shardID := (startShardID + i) % len(s.shards)

		shard := s.shards[shardID]
		shard.Lock()

		n := shard.Peek()
		if n == nil {
			shard.Unlock()
			continue // Empty shard, fallback to next
		}

		var res T
		var acquiredID ID
		if s.cfg.AcquirePolicy == config.Shared {
			res = n.Value
			acquiredID = n.Key
		} else {
			// Exclusive policy
			n = shard.Pop()
			n.IsActive = false // Conceptually in the Inactive Store
			res = n.Value
			acquiredID = n.Key
		}

		shard.Unlock()

		s.emit(events.EventAcquire, acquiredID)

		return res, nil
	}

	return zero, errors.ErrNoResourceAvailable
}

// Release returns a previously exclusively-acquired resource to its Heap Shard,
// restoring it to ACTIVE state and making it eligible for future acquisition.
//
// Release is only valid when the scheduler is configured with
// [config.Exclusive] policy. It returns [errors.ErrNotExclusive] for
// [config.Shared] schedulers.
//
// Release returns [errors.ErrResourceNotFound] if no resource with the given
// key exists (it may have been removed while held). It returns
// [errors.ErrResourceNotInactive] if the resource is already ACTIVE (it may
// have been re-included by a concurrent [Scheduler.Include] call).
// It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Release is safe for concurrent use by multiple goroutines. It performs an
// O(1) lookup followed by a single-shard lock.
//
// Complexity: O(log N) where N is the number of resources in the target shard.
func (s *Scheduler[T, ID]) Release(id ID) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
	}

	if s.cfg.AcquirePolicy != config.Exclusive {
		return errors.ErrNotExclusive
	}

	n := s.registry.Get(id)
	if n == nil {
		return errors.ErrResourceNotFound
	}

	shard := s.shards[n.ShardID]

	shard.Lock()

	if n.IsDeleted {
		shard.Unlock()
		return errors.ErrResourceNotFound
	}

	if n.IsActive {
		shard.Unlock()
		return errors.ErrResourceNotInactive
	}

	n.IsActive = true
	shard.Push(n)
	shard.Unlock()

	s.emit(events.EventRelease, id)

	return nil
}
