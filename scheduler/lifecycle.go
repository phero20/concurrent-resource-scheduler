package scheduler

import (
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/events"
)

// Update applies a new value to an existing resource.
// The scheduler orchestrates this operation while respecting the strictly
// decoupled locking architecture (querying Lookup Map, then independently locking the Heap Shard).
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

// Exclude forces an ACTIVE resource to the Inactive Store.
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

// Include forces an INACTIVE resource to its native heap.
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

// Remove permanently deletes a resource.
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
