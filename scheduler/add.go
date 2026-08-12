package scheduler

import (
	"sync/atomic"

	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
	"github.com/phero20/concurrent-resource-scheduler/internal/node"
)

// Add inserts a single resource into the scheduler.
//
// An internal Round Robin counter assigns the resource to a Heap Shard,
// distributing resources evenly across shards without a global lock. The
// assignment is permanent: a resource always returns to its original shard
// after Release or Include.
//
// Add returns [errors.ErrNilResource] if res is nil. It returns
// [errors.ErrDuplicateKey] if a resource with the same key (as returned by
// [config.KeyFunc]) already exists in the scheduler. It returns
// [errors.ErrSchedulerClosed] after [Scheduler.Shutdown] has been called.
//
// # Concurrency
//
// Add is safe for concurrent use by multiple goroutines. The atomic Round
// Robin counter ensures balanced distribution and insertion locks only the
// target shard.
//
// Complexity: O(log N) where N is the number of resources in the target shard.
func (s *Scheduler[T, ID]) Add(res T) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
	}

	if s.isNil(res) {
		return errors.ErrNilResource
	}

	key := s.cfg.KeyFunc(res)

	shardID := int(atomic.AddUint32(&s.insertionIndex, 1)-1) % len(s.shards)

	n := &node.HeapNode[T, ID]{
		Value:    res,
		Key:      key,
		ShardID:  shardID,
		IsActive: true,
	}

	// 1. Lookup Map handles its own synchronization.
	if err := s.registry.Add(key, n); err != nil {
		return err
	}

	// 2. Heap Shard requires explicit locking.
	s.shards[shardID].Lock()
	s.shards[shardID].Push(n)
	s.shards[shardID].Unlock()

	s.emit(events.EventAdd, key)

	return nil
}

// BatchAdd atomically validates and inserts a slice of resources.
//
// BatchAdd operates in two distinct phases to guarantee all-or-nothing
// insertion semantics:
//
//   - Phase 1 (Validation): every resource is checked for nil values,
//     intra-batch duplicate keys, and keys that already exist in the
//     scheduler. No scheduler state is modified during Phase 1. If any
//     check fails, BatchAdd returns immediately with the relevant error
//     and the scheduler remains unchanged.
//   - Phase 2 (Insertion): all resources are distributed across Heap Shards
//     using the internal Round Robin counter and inserted atomically. A
//     partial insertion never occurs.
//
// BatchAdd returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown] has
// been called. It returns nil immediately if resources is empty.
//
// # Concurrency
//
// BatchAdd is safe for concurrent use by multiple goroutines. Shard locks are
// taken one at a time during Phase 2; no global lock is held.
//
// Complexity: O(B × log N) where B is the batch size and N is resources per shard.
func (s *Scheduler[T, ID]) BatchAdd(resources []T) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
	}

	if len(resources) == 0 {
		return nil
	}

	seen := make(map[ID]struct{}, len(resources))

	// Phase 1: Validation
	for _, res := range resources {
		if s.isNil(res) {
			return errors.ErrNilResource
		}

		key := s.cfg.KeyFunc(res)

		// Check intra-batch duplicate
		if _, exists := seen[key]; exists {
			return errors.ErrDuplicateKey
		}
		seen[key] = struct{}{}

		// Check scheduler duplicate
		if s.registry.Get(key) != nil {
			return errors.ErrDuplicateKey
		}
	}

	// Phase 2: Insertion via internal Round Robin insertion strategy
	nodes := make(map[ID]*node.HeapNode[T, ID], len(resources))
	for _, res := range resources {
		key := s.cfg.KeyFunc(res)
		shardID := int(atomic.AddUint32(&s.insertionIndex, 1)-1) % len(s.shards)

		n := &node.HeapNode[T, ID]{
			Value:    res,
			Key:      key,
			ShardID:  shardID,
			IsActive: true,
		}
		nodes[key] = n
	}

	// 1. Lookup Map handles its own synchronization (Atomic batch insert).
	if err := s.registry.BatchAdd(nodes); err != nil {
		return err
	}

	// 2. Heap Shard requires explicit locking.
	for _, res := range resources {
		key := s.cfg.KeyFunc(res)
		n := nodes[key]
		shard := s.shards[n.ShardID]
		shard.Lock()
		shard.Push(n)
		shard.Unlock()

		s.emit(events.EventAdd, n.Key)
	}

	return nil
}
