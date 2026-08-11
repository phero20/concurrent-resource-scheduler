package scheduler

import (
	"hash/fnv"

	"github.com/phero20/concurrent-resource-scheduler/acquire"
	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
)

// Acquire retrieves the highest priority resource from the scheduler.
// It delegates to the configured AcquireStrategy to select a candidate Heap Shard,
// checking shards sequentially until an available resource is found. If the policy
// is Exclusive, the resource is atomically moved to the Inactive Store.
//
// Concurrency Guarantees:
// Thread-safe. It only locks one Heap Shard at a time, distributing contention across shards.
// scalability without global lock contention.
//
// Complexity:
// O(H) for Shared policy, O(log N + H) for Exclusive policy, where H is HeapCount and N is resources per shard.
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

// AcquireByAffinity retrieves a resource deterministically based on an affinity identifier.
// It bypasses the global AcquireStrategy, using an internal Consistent Hash Ring
// to route the request to a specific shard. This is ideal for sticky sessions.
//
// Concurrency Guarantees:
// Thread-safe. The Consistent Hash Ring lookup is allocation-free and read-optimized, and only the target
// Heap Shard is locked during the operation.
//
// Complexity:
// O(log V + log N) where V is the number of virtual nodes and N is resources per shard.
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

// Release returns a previously exclusively acquired resource back to the scheduler.
// It restores the resource to its native Heap Shard and makes it ACTIVE again.
// It returns ErrNotExclusive if the scheduler is not using the Exclusive policy.
//
// Concurrency Guarantees:
// Thread-safe. It performs an O(1) concurrent-safe lookup followed by a single-shard lock.
//
// Complexity:
// O(log N) where N is the number of resources in the target shard.
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
