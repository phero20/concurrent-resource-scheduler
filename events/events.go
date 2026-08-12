// Package events defines the event types, payload structure, and observer
// interface for the Concurrent Resource Scheduler's asynchronous notification
// system.
//
// # Overview
//
// The scheduler emits an [Event] for every significant resource state
// transition (add, acquire, release, exclude, include, remove, update).
// Events are dispatched asynchronously from a dedicated background goroutine
// to a list of [Observer] implementations registered in [config.Config].
//
// # Drop Policy
//
// The internal event channel has a fixed buffer of 4096 events. If the buffer
// is full when the scheduler tries to emit, the event is silently dropped.
// Observers MUST NOT block; a blocking observer stalls the dispatcher and
// causes subsequent events to be dropped.
//
// # Ordering
//
// Events are delivered in the order they are dequeued from the internal
// buffer, which approximates (but does not strictly guarantee) the order of
// the originating scheduler operations.
//
// # Lifecycle
//
// The background dispatcher starts when [scheduler.New] is called with a
// non-empty Observers slice and stops when [scheduler.Scheduler.Shutdown] is
// called. Events still in the buffer at shutdown are discarded.
package events

// EventType identifies the scheduler operation that triggered an event.
type EventType int

const (
	// EventAdd is emitted after a resource is successfully inserted into the
	// scheduler via [scheduler.Scheduler.Add] or [scheduler.Scheduler.BatchAdd].
	EventAdd EventType = iota

	// EventAcquire is emitted after a resource is successfully returned by
	// [scheduler.Scheduler.Acquire] or [scheduler.Scheduler.AcquireByAffinity].
	EventAcquire

	// EventRelease is emitted after a resource is successfully returned to
	// ACTIVE state by [scheduler.Scheduler.Release].
	EventRelease

	// EventExclude is emitted after a resource is successfully moved to the
	// Inactive Store by [scheduler.Scheduler.Exclude].
	EventExclude

	// EventInclude is emitted after a resource is successfully restored to
	// ACTIVE state by [scheduler.Scheduler.Include].
	EventInclude

	// EventRemove is emitted after a resource is permanently deleted by
	// [scheduler.Scheduler.Remove].
	EventRemove

	// EventUpdate is emitted after a resource's value is replaced by
	// [scheduler.Scheduler.Update].
	EventUpdate
)

// String returns a human-readable name for the EventType, suitable for
// logging and debugging.
//
// Complexity: O(1).
func (t EventType) String() string {
	switch t {
	case EventAdd:
		return "Add"
	case EventAcquire:
		return "Acquire"
	case EventRelease:
		return "Release"
	case EventExclude:
		return "Exclude"
	case EventInclude:
		return "Include"
	case EventRemove:
		return "Remove"
	case EventUpdate:
		return "Update"
	default:
		return "Unknown"
	}
}

// Event describes a single scheduler state transition for a specific resource.
// It is the payload delivered to every registered [Observer].
//
// Event intentionally omits a timestamp to avoid time.Now syscall overhead on
// the scheduler's hot path. Observers that need timestamps should record them
// inside their OnEvent implementation.
//
// The resource value itself is not included; only the application-defined ID
// is passed to prevent concurrent mutation races on the caller's data.
type Event[ID comparable] struct {
	// Type identifies which scheduler operation triggered this event.
	Type EventType

	// ID is the application-defined key of the affected resource, as returned
	// by the [config.KeyFunc] supplied during scheduler construction.
	ID ID
}

// Observer is implemented by types that wish to receive asynchronous scheduler
// lifecycle events.
//
// # Contract
//
// OnEvent is called from the scheduler's single background dispatcher
// goroutine. Implementations MUST NOT block. A blocking OnEvent stalls the
// dispatcher, causing the internal event buffer to fill and subsequent events
// to be silently dropped.
//
// Implementations MUST NOT call back into the scheduler from OnEvent; doing so
// can cause deadlocks or unexpected re-entrancy.
//
// # Concurrency
//
// OnEvent is called sequentially by one goroutine, but the scheduler's other
// methods (Acquire, Release, etc.) may run concurrently. If an Observer
// accesses shared state, it is responsible for its own synchronization.
//
// # Ordering
//
// Events are delivered in FIFO order from the internal buffer. Buffer drops
// may produce gaps in the event sequence but will not reorder surviving events.
type Observer[ID comparable] interface {
	OnEvent(e Event[ID])
}
