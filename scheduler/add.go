package scheduler

import (
	"sync/atomic"

	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
	"github.com/phero20/concurrent-resource-scheduler/internal/node"
)

// Add inserts a single resource into the scheduler.
// It uses an internal Round Robin algorithm to assign the resource to a target
// Heap Shard. It returns ErrDuplicateResource if the resource key already exists.
//
// Concurrency Guarantees:
// Thread-safe. The Round Robin atomic counter ensures balanced distribution
// without locking, and insertion locks only the target shard.
//
// Complexity:
// O(log N) where N is the number of resources in the target shard.
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

// BatchAdd atomically validates and inserts multiple resources.
// It operates in two phases: Phase 1 strictly validates all elements (nil checks,
// internal batch duplicates, and global registry duplicates) without modifying
// any state. Phase 2 safely distributes the valid batch across shards.
//
// Concurrency Guarantees:
// Thread-safe. It maintains consistency by locking shards sequentially, avoiding
// global locks. Partial insertions never occur.
//
// Complexity:
// O(B * log N) where B is batch size and N is resources per shard.
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
