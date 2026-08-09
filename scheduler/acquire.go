package scheduler

import (
	"hash/fnv"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/placement"
)

// Acquire returns the highest priority available resource.
// It is the ONLY operation that uses AcquireStrategy to select a candidate Heap Shard.
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

// AcquireByAffinity returns the highest priority available resource by deterministically
// routing to a specific Heap Shard based on the provided affinity identifier.
// The scheduler internally hashes the bytes to select the target shard.
// If the selected shard is empty, it falls back to inspecting the remaining shards.
func (s *Scheduler[T, ID]) AcquireByAffinity(key placement.AffinityIdentifier) (T, error) {
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

// Release returns an Exclusive resource to its native Heap Shard.
// It returns an error if the policy is not Exclusive, the key is not found,
// or the resource is not currently inactive.
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
