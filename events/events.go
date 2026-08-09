package events

// EventType defines the lifecycle stage that triggered the event.
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

// Event represents a state transition in the scheduler.
// It explicitly omits a timestamp to prevent time.Now() overhead on the hot path.
// The resource ID is passed instead of the full resource to prevent concurrent mutation races.
type Event[ID comparable] struct {
	Type EventType
	ID   ID
}

// Observer defines the contract for external components to receive scheduler events.
// Implementations MUST NOT block the calling goroutine. In the CRS implementation,
// they are called via a non-blocking background dispatcher.
type Observer[ID comparable] interface {
	OnEvent(e Event[ID])
}
