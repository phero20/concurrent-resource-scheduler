// Package cooldown provides an automatic post-release cooldown extension for
// the Concurrent Resource Scheduler.
//
// # What It Does
//
// The cooldown extension automatically moves a resource to the Inactive Store
// for a configurable duration after it is released under the
// [config.Exclusive] AcquirePolicy. This is useful for implementing back-off
// strategies, rate limiting, or resting periods after a resource is used.
//
// # How It Works
//
// [Manager] implements [events.Observer]. When registered in
// [config.Config.Observers], it intercepts [events.EventRelease] notifications
// and calls [Scheduler.Exclude] on the released resource, then schedules
// [Scheduler.Include] after the configured duration using [time.AfterFunc].
//
// # Eventual-Consistency Window
//
// Cooldown is asynchronous. There is a small window between the Release call
// completing and the corresponding Exclude taking effect. During this window
// the resource is visible to [scheduler.Scheduler.Acquire]. Applications that
// require strict synchronous cooldown enforcement should implement the logic
// inside the scheduler comparator or before calling Release rather than
// relying on this observer.
//
// # Usage
//
//	// Create a wrapper to break the initialization cycle
//	ctrl := &myControllerWrapper{}
//	manager := cooldown.NewManager[string](ctrl, 5*time.Second)
//
//	cfg := config.Config[*MyResource, string]{
//	    Observers: []events.Observer[string]{manager},
//	}
//	sched, _ := scheduler.New(cfg)
//	ctrl.scheduler = sched // Wire the controller
package cooldown

import (
	"sync"
	"time"

	"github.com/phero20/concurrent-resource-scheduler/events"
)

// LifecycleController abstracts the scheduler operations required by the
// cooldown extension.
//
// [*scheduler.Scheduler] satisfies this interface, allowing Manager to call
// Exclude and Include without introducing a package import cycle.
type LifecycleController[ID comparable] interface {
	// Exclude moves the resource with the given key to the Inactive Store.
	Exclude(id ID) error
	// Include restores the resource with the given key to ACTIVE state.
	Include(id ID) error
}

// Manager is an [events.Observer] that automatically places released resources
// into a temporary cooldown state.
//
// When a resource is released under [config.Exclusive] policy, Manager
// intercepts the [events.EventRelease] event, calls [LifecycleController.Exclude]
// to move the resource to the Inactive Store, and schedules
// [LifecycleController.Include] after the configured duration.
//
// At most one active cooldown is tracked per resource. If a resource is
// released while already cooling down (e.g., it was re-included early and
// then acquired and released again), the second cooldown replaces the first.
//
// # Eventual Consistency
//
// Cooldown is applied asynchronously. See the package documentation for the
// implications of the eventual-consistency window.
//
// # Concurrency
//
// Manager is safe for concurrent use by multiple goroutines. It delegates
// timer callbacks to [time.AfterFunc] and uses a [sync.Map] for tracking
// active cooldowns without introducing locks in OnEvent.
type Manager[ID comparable] struct {
	controller LifecycleController[ID]
	duration   time.Duration
	active     sync.Map
}

// NewManager initializes a cooldown Manager.
//
// controller is the [LifecycleController] to call for Exclude and Include;
// pass the *[scheduler.Scheduler] directly. duration is the cooldown period
// applied after each release; it must be positive. A zero or negative duration
// causes Include to be scheduled immediately, effectively disabling the
// cooldown.
//
// The returned Manager must be registered in [config.Config.Observers] before
// [scheduler.New] is called.
//
// The returned Manager is safe for concurrent use by multiple goroutines.
func NewManager[ID comparable](controller LifecycleController[ID], duration time.Duration) *Manager[ID] {
	return &Manager[ID]{
		controller: controller,
		duration:   duration,
	}
}

// OnEvent processes scheduler events and triggers a cooldown on EventRelease.
//
// OnEvent is non-blocking: it spawns a goroutine for the Exclude call and
// uses [time.AfterFunc] for the delayed Include, satisfying the
// [events.Observer] non-blocking contract.
//
// # Concurrency
//
// OnEvent is safe for concurrent use by multiple goroutines.
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
