package events

// EventType represents a distinct lifecycle transition within the scheduler.
//
// Behavior:
// Identifies what operation triggered an event (e.g., EventAcquire, EventRelease).
type EventType int

const (
	EventAdd EventType = iota
	EventAcquire
	EventRelease
	EventExclude
	EventInclude
	EventRemove
	EventUpdate
)

// String returns a human-readable representation of the EventType.
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

// Event describes a single state transition for a specific resource.
//
// Behavior:
// It intentionally omits a timestamp to prevent time.Now() syscall overhead
// in the scheduler's hot path. It passes the resource ID instead of the full
// object to avoid concurrent mutation races.
type Event[ID comparable] struct {
	Type EventType
	ID   ID
}

// Observer provides a contract for external components to passively receive events.
//
// Concurrency Guarantees:
// Implementations MUST NOT block. The scheduler invokes OnEvent from a dedicated
// background goroutine. A blocking observer will cause the internal event channel
// to drop events silently to protect scheduler throughput.
type Observer[ID comparable] interface {
	OnEvent(e Event[ID])
}
