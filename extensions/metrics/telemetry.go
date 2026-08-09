package metrics

import (
	"sync/atomic"

	"github.com/feroz/concurrent-resource-scheduler/events"
)

// TelemetryStats provides a point-in-time snapshot of the aggregate event throughput.
type TelemetryStats struct {
	AddCount     uint64
	AcquireCount uint64
	ReleaseCount uint64
	ExcludeCount uint64
	IncludeCount uint64
	RemoveCount  uint64
	UpdateCount  uint64
}

// TelemetryObserver is a lock-free event observer that aggregates operational
// throughput via atomic counters.
type TelemetryObserver[ID comparable] struct {
	adds     atomic.Uint64
	acquires atomic.Uint64
	releases atomic.Uint64
	excludes atomic.Uint64
	includes atomic.Uint64
	removes  atomic.Uint64
	updates  atomic.Uint64
}

// NewTelemetryObserver creates a new TelemetryObserver.
func NewTelemetryObserver[ID comparable]() *TelemetryObserver[ID] {
	return &TelemetryObserver[ID]{}
}

// OnEvent implements the events.Observer contract.
// It is completely lock-free and updates atomic counters based on the event type.
func (o *TelemetryObserver[ID]) OnEvent(e events.Event[ID]) {
	switch e.Type {
	case events.EventAdd:
		o.adds.Add(1)
	case events.EventAcquire:
		o.acquires.Add(1)
	case events.EventRelease:
		o.releases.Add(1)
	case events.EventExclude:
		o.excludes.Add(1)
	case events.EventInclude:
		o.includes.Add(1)
	case events.EventRemove:
		o.removes.Add(1)
	case events.EventUpdate:
		o.updates.Add(1)
	}
}

// Snapshot returns the current counter values.
func (o *TelemetryObserver[ID]) Snapshot() TelemetryStats {
	return TelemetryStats{
		AddCount:     o.adds.Load(),
		AcquireCount: o.acquires.Load(),
		ReleaseCount: o.releases.Load(),
		ExcludeCount: o.excludes.Load(),
		IncludeCount: o.includes.Load(),
		RemoveCount:  o.removes.Load(),
		UpdateCount:  o.updates.Load(),
	}
}

// Ensure TelemetryObserver implements events.Observer.
var _ events.Observer[string] = (*TelemetryObserver[string])(nil)
