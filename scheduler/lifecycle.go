package scheduler

import (
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
)

// Update replaces the stored value of an existing resource and recalculates its
// priority position within the heap.
//
// The caller supplies the full replacement resource. The scheduler derives the
// key using [config.KeyFunc] and locates the resource in the lookup map. If
// the resource is ACTIVE, the owning shard is locked, the value is replaced,
// and heap.Fix restores the priority-queue invariant in O(log N). If the
// resource is INACTIVE, the value is replaced in the Inactive Store in O(1)
// without touching any Heap Shard.
//
// Resource identity (the key) is immutable. If the replacement resource
// produces a different key from the stored resource, Update returns
// [errors.ErrResourceNotFound]. To change a key, call [Scheduler.Remove]
// followed by [Scheduler.Add].
//
// Update returns [errors.ErrNilResource] if res is nil.
// It returns [errors.ErrResourceNotFound] if the key does not exist.
// It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Update is safe for concurrent use by multiple goroutines.
//
// Complexity: O(log N) for ACTIVE resources; O(1) for INACTIVE resources.
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

// Exclude manually moves an ACTIVE resource to the Inactive Store, making it
// invisible to [Scheduler.Acquire] until [Scheduler.Include] is called.
//
// Typical use cases include temporarily removing a faulty or rate-limited
// resource from circulation (e.g., during a cooldown period).
//
// Exclude is not idempotent: calling Exclude on an already-INACTIVE resource
// returns [errors.ErrResourceNotActive] without modifying scheduler state.
//
// Exclude returns [errors.ErrResourceNotFound] if the key does not exist.
// It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Exclude is safe for concurrent use by multiple goroutines. It locks only
// the owning Heap Shard.
//
// Complexity: O(log N) where N is the number of resources in the target shard.
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

// Include restores a previously excluded (or exclusively-acquired) resource to
// ACTIVE state in its original Heap Shard, making it eligible for acquisition.
//
// Include is not idempotent: calling Include on an already-ACTIVE resource
// returns [errors.ErrResourceNotInactive] without modifying scheduler state.
//
// Include returns [errors.ErrResourceNotFound] if the key does not exist.
// It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Include is safe for concurrent use by multiple goroutines. It locks only
// the target Heap Shard.
//
// Complexity: O(log N) where N is the number of resources in the target shard.
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

// Remove permanently deletes a resource from the scheduler, regardless of
// whether it is currently ACTIVE or INACTIVE.
//
// After Remove, the resource's key is deregistered from the Lookup Map. Any
// subsequent operation referencing the key returns [errors.ErrResourceNotFound].
// Resources held exclusively by a caller (acquired but not yet released) are
// marked deleted; a subsequent [Scheduler.Release] for that key returns
// [errors.ErrResourceNotFound].
//
// Remove returns [errors.ErrResourceNotFound] if no resource with the given
// key exists. It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Remove is safe for concurrent use by multiple goroutines. It locks only
// the owning Heap Shard (not a global lock), so removal does not block
// concurrent acquisitions on other shards.
//
// Complexity: O(log N) for ACTIVE resources; O(1) for INACTIVE resources.
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
