package metrics

import (
	"sync/atomic"

	"github.com/phero20/concurrent-resource-scheduler/events"
)

// TelemetryStats is a point-in-time snapshot of the accumulated event counters.
//
// Behavior:
// Represents the exact number of operations processed since the telemetry
// observer was instantiated.
type TelemetryStats struct {
	AddCount     uint64
	AcquireCount uint64
	ReleaseCount uint64
	ExcludeCount uint64
	IncludeCount uint64
	RemoveCount  uint64
	UpdateCount  uint64
}

// TelemetryObserver is an events.Observer that tracks high-throughput operation rates.
//
// Behavior:
// It aggregates events into atomic counters without locking.
//
// Concurrency Guarantees:
// Atomic and thread-safe. It uses sync/atomic exclusively to prevent lock overhead.
// contention on the background dispatcher thread.
type TelemetryObserver[ID comparable] struct {
	adds     atomic.Uint64
	acquires atomic.Uint64
	releases atomic.Uint64
	excludes atomic.Uint64
	includes atomic.Uint64
	removes  atomic.Uint64
	updates  atomic.Uint64
}

// NewTelemetryObserver creates a new lock-free metrics aggregator.
//
// Lifecycle:
// Pass this to config.Observers. After the scheduler is running, call Snapshot()
// to read real-time metrics.
func NewTelemetryObserver[ID comparable]() *TelemetryObserver[ID] {
	return &TelemetryObserver[ID]{}
}

// OnEvent intercepts scheduler events and increments the corresponding atomic counter.
//
// Concurrency Guarantees:
// Atomic and non-blocking.
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

// Snapshot returns a copy of the current atomic counters.
//
// Concurrency Guarantees:
// Lock-free and thread-safe. Can be called at any rate by metrics scrapers (e.g. Prometheus).
//
// Complexity: O(1).
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
