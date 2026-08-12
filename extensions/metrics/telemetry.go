// Package metrics provides a lock-free throughput telemetry observer for the
// Concurrent Resource Scheduler.
//
// # Overview
//
// [TelemetryObserver] implements [events.Observer] and accumulates per-event
// type counters using atomic integers. It is safe to scrape at any rate with
// zero lock contention.
//
// # Usage
//
//	obs := metrics.NewTelemetryObserver[string]()
//	cfg := config.Config[*MyResource, string]{
//	    // ...
//	    Observers: []events.Observer[string]{obs},
//	}
//	sched, _ := scheduler.New(cfg)
//
//	// In a metrics scraper goroutine:
//	snap := obs.Snapshot()
//	fmt.Println("acquires:", snap.AcquireCount)
//
// # Prometheus Integration
//
// To expose these counters in Prometheus, see the [extensions/prometheus]
// package, which bridges a TelemetryObserver into a prometheus.Collector.
package metrics

import (
	"sync/atomic"

	"github.com/phero20/concurrent-resource-scheduler/events"
)

// TelemetryStats is a point-in-time snapshot of accumulated event counters.
// All counts are monotonically increasing from the moment the
// [TelemetryObserver] was constructed; they are never reset.
type TelemetryStats struct {
	// AddCount is the total number of successful Add and BatchAdd operations
	// since the observer was created.
	AddCount uint64

	// AcquireCount is the total number of successful Acquire and
	// AcquireByAffinity operations since the observer was created.
	AcquireCount uint64

	// ReleaseCount is the total number of successful Release operations
	// since the observer was created.
	ReleaseCount uint64

	// ExcludeCount is the total number of successful Exclude operations
	// since the observer was created.
	ExcludeCount uint64

	// IncludeCount is the total number of successful Include operations
	// since the observer was created.
	IncludeCount uint64

	// RemoveCount is the total number of successful Remove operations
	// since the observer was created.
	RemoveCount uint64

	// UpdateCount is the total number of successful Update operations
	// since the observer was created.
	UpdateCount uint64
}

// TelemetryObserver is an [events.Observer] that accumulates per-operation
// event counts using atomic integers, without locks.
//
// # Concurrency
//
// TelemetryObserver is safe for concurrent use by multiple goroutines.
// All counter increments use [sync/atomic] to prevent lock overhead on the
// background dispatcher thread.
type TelemetryObserver[ID comparable] struct {
	adds     atomic.Uint64
	acquires atomic.Uint64
	releases atomic.Uint64
	excludes atomic.Uint64
	includes atomic.Uint64
	removes  atomic.Uint64
	updates  atomic.Uint64
}

// NewTelemetryObserver creates a new lock-free event counter.
//
// Register the returned observer in [config.Config.Observers] before calling
// [scheduler.New]. All counters start at zero and increase monotonically.
// Call [TelemetryObserver.Snapshot] from any goroutine to read current values.
//
// The returned observer is safe for concurrent use by multiple goroutines.
func NewTelemetryObserver[ID comparable]() *TelemetryObserver[ID] {
	return &TelemetryObserver[ID]{}
}

// OnEvent increments the counter corresponding to the event type.
//
// OnEvent is non-blocking and safe for concurrent use by multiple goroutines.
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

// Snapshot returns a copy of the current event counters as a [TelemetryStats]
// value. All fields in the returned snapshot are independent atomic loads;
// the snapshot is not a single consistent read of all counters simultaneously.
//
// Counters are monotonically increasing and never reset. Subtract two
// snapshots to compute the delta over an interval.
//
// Snapshot is safe for concurrent use by multiple goroutines and can be
// called at any frequency without contention.
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
