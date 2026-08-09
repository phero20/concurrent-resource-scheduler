package cooldown

import (
	"sync"
	"time"

	"github.com/feroz/concurrent-resource-scheduler/events"
)

// LifecycleController defines the minimal scheduler contract required by the CooldownManager.
// Using this interface prevents the extension from needing to know about the generic resource type T.
type LifecycleController[ID comparable] interface {
	Exclude(id ID) error
	Include(id ID) error
}

// Manager is a reference implementation of an external Observer that applies
// a time-based cooldown to resources after they are released.
type Manager[ID comparable] struct {
	controller LifecycleController[ID]
	duration   time.Duration
	active     sync.Map
}

// NewManager creates a new CooldownManager.
func NewManager[ID comparable](controller LifecycleController[ID], duration time.Duration) *Manager[ID] {
	return &Manager[ID]{
		controller: controller,
		duration:   duration,
	}
}

// OnEvent implements events.Observer.
// It listens for EventRelease and asynchronously applies a cooldown.
func (m *Manager[ID]) OnEvent(e events.Event[ID]) {
	// We only apply cooldowns when a resource is returned to the pool.
	if e.Type != events.EventRelease {
		return
	}

	// Guarantee at most one active cooldown sequence per resource.
	if _, loaded := m.active.LoadOrStore(e.ID, struct{}{}); loaded {
		return
	}

	// Execute asynchronously to strictly honor the non-blocking Observer contract.
	go func(id ID) {
		// Attempt to exclude the resource.
		if err := m.controller.Exclude(id); err == nil {
			// Resource successfully moved to Inactive Store.
			// Schedule it to be included back into the active pool after the duration.
			time.AfterFunc(m.duration, func() {
				_ = m.controller.Include(id)
				m.active.Delete(id)
			})
		} else {
			// Abort cooldown tracking if we failed to exclude
			// (e.g., another goroutine already acquired it, or it was removed).
			m.active.Delete(id)
		}
	}(e.ID)
}

// Validate that Manager implements Observer at compile-time.
var _ events.Observer[string] = (*Manager[string])(nil)
