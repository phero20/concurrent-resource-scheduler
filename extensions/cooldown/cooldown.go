package cooldown

import (
	"sync"
	"time"

	"github.com/phero20/concurrent-resource-scheduler/events"
)

// LifecycleController abstracts the scheduler's manual inclusion/exclusion operations.
//
// Behavior:
// This interface allows the Cooldown Manager to manipulate the Inactive Store
// without introducing a circular package dependency on the scheduler core.
type LifecycleController[ID comparable] interface {
	Exclude(id ID) error
	Include(id ID) error
}

// Manager is an events.Observer that automatically places released resources
// into a temporary cooldown state.
//
// Important:
// Cooldown is implemented as an asynchronous Observer. Resources become
// excluded asynchronously after Release(). There exists a very small
// eventual-consistency window between Release() and Exclude(). This is
// an intentional tradeoff to preserve the scheduler's non-blocking event
// architecture. Applications requiring strict synchronous cooldown
// enforcement should implement cooldown inside scheduler logic instead
// of using the observer extension.
//
// Behavior:
// When a resource is released, the Manager intercepts the event, calls Exclude(),
// and sets a timer to call Include() after the specified duration.
//
// Concurrency Guarantees:
// Thread-safe and non-blocking. It delegates delays to time.AfterFunc.
type Manager[ID comparable] struct {
	controller LifecycleController[ID]
	duration   time.Duration
	active     sync.Map
}

// NewManager initializes a Cooldown Manager with the provided controller and duration.
//
// Lifecycle:
// The returned Manager should be registered in the config.Observers slice
// during scheduler initialization.
func NewManager[ID comparable](controller LifecycleController[ID], duration time.Duration) *Manager[ID] {
	return &Manager[ID]{
		controller: controller,
		duration:   duration,
	}
}

// OnEvent processes scheduler events asynchronously.
//
// Behavior:
// If the event is EventRelease, it triggers a cooldown for the resource.
//
// Concurrency Guarantees:
// Strictly non-blocking. It fires goroutines for timer callbacks.
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
