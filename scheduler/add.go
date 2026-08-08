package scheduler

import (
	"sync/atomic"

	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/internal/node"
)

// Add creates an internal HeapNode for a single resource and inserts it.
// It assigns the resource to a Heap Shard using the internal Round Robin insertion strategy.
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

	return nil
}

// BatchAdd performs a bulk resource insertion.
// It validates all resources and adds them to the Lookup Map atomically, ensuring no partial
// registry insertions occur if a duplicate is found. Once registered, resources are iteratively
// pushed to their respective Heap Shards. During this iterative heap insertion, concurrent operations
// like Len() will include the resources, but Acquire() may not discover them until they enter a shard.
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
	for _, n := range nodes {
		shard := s.shards[n.ShardID]
		shard.Lock()
		shard.Push(n)
		shard.Unlock()
	}

	return nil
}
