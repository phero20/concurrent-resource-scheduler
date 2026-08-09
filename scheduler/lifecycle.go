package scheduler

import (
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
)

// Update replaces the value of an existing resource and recalculates its priority.
// If the resource is ACTIVE, it locks the owning shard and triggers a heap fix to
// restore the priority queue invariants. If INACTIVE, it safely updates the value
// in the Inactive Store.
//
// Concurrency Guarantees:
// Thread-safe. It identifies the resource location via O(1) lock-free lookup and
// strictly takes either the target shard lock or the Inactive Store lock.
//
// Complexity:
// O(log N) for ACTIVE resources, O(1) for INACTIVE resources.
func (s *Scheduler[T, ID]) Update(res T) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
	}

	if s.isNil(res) {
		return errors.ErrNilResource
	}

	key := s.cfg.KeyFunc(res)
	n := s.registry.Get(key)
	if n == nil {
		return errors.ErrResourceNotFound
	}

	shard := s.shards[n.ShardID]

	shard.Lock()

	if n.IsDeleted {
		shard.Unlock()
		return errors.ErrResourceNotFound
	}

	n.Value = res

	if n.IsActive {
		shard.Fix(n.Index)
	}

	shard.Unlock()

	s.emit(events.EventUpdate, key)

	return nil
}

// Exclude manually moves an ACTIVE resource to the Inactive Store.
// This is typically used to temporarily remove a faulty resource from circulation
// (e.g., during a cooldown period).
//
// Concurrency Guarantees:
// Thread-safe. It locks the owning shard, removes the node, and places it into
// the locked Inactive Store.
//
// Complexity:
// O(log N) where N is the number of resources in the target shard.
func (s *Scheduler[T, ID]) Exclude(id ID) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
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

	if !n.IsActive {
		shard.Unlock()
		return errors.ErrResourceNotActive
	}

	shard.Remove(n.Index)
	n.IsActive = false

	shard.Unlock()

	s.emit(events.EventExclude, id)

	return nil
}

// Include restores a previously excluded resource back to its ACTIVE state.
// The resource is returned to its original native Heap Shard and becomes available
// for acquisition.
//
// Concurrency Guarantees:
// Thread-safe. It atomically moves the node from the Inactive Store into the
// corresponding locked Heap Shard.
//
// Complexity:
// O(log N) where N is the number of resources in the target shard.
func (s *Scheduler[T, ID]) Include(id ID) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
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

	s.emit(events.EventInclude, id)

	return nil
}

// Remove permanently deletes a resource from the scheduler.
// It works seamlessly whether the resource is ACTIVE in a Heap Shard or
// INACTIVE in the Inactive Store, removing all global lookup references.
//
// Concurrency Guarantees:
// Thread-safe. It locates the node and cleanly locks either the target shard
// or the Inactive Store to perform the removal.
//
// Complexity:
// O(log N) for ACTIVE resources, O(1) for INACTIVE resources.
func (s *Scheduler[T, ID]) Remove(id ID) error {
	if s.closed.Load() {
		return errors.ErrSchedulerClosed
	}

	n := s.registry.Get(id)
	if n == nil {
		return errors.ErrResourceNotFound
	}

	shard := s.shards[n.ShardID]

	// The scheduler locks the shard to safely remove from the heap if active.
	// We do not hold the Lookup Map lock, preserving strictly decoupled locking.
	shard.Lock()
	n.IsDeleted = true
	if n.IsActive {
		shard.Remove(n.Index)
		n.IsActive = false
	}
	shard.Unlock()

	// Lookup Map handles its own synchronization independently.
	s.registry.Remove(id)

	s.emit(events.EventRemove, id)

	return nil
}
