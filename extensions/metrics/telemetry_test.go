package metrics_test

import (
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/extensions/metrics"
)

func TestTelemetryObserver_EventCounts(t *testing.T) {
	observer := metrics.NewTelemetryObserver[string]()

	observer.OnEvent(events.Event[string]{Type: events.EventAdd, ID: "1"})
	observer.OnEvent(events.Event[string]{Type: events.EventAcquire, ID: "2"})
	observer.OnEvent(events.Event[string]{Type: events.EventAcquire, ID: "3"})
	observer.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "2"})
	observer.OnEvent(events.Event[string]{Type: events.EventRemove, ID: "3"})
	observer.OnEvent(events.Event[string]{Type: events.EventUpdate, ID: "1"})

	observer.OnEvent(events.Event[string]{Type: events.EventExclude, ID: "1"})
	observer.OnEvent(events.Event[string]{Type: events.EventInclude, ID: "2"})
	observer.OnEvent(events.Event[string]{Type: events.EventType(99), ID: "unknown"}) // Unknown event type

	stats := observer.Snapshot()

	if stats.AddCount != 1 {
		t.Errorf("expected 1 add, got %d", stats.AddCount)
	}
	if stats.AcquireCount != 2 {
		t.Errorf("expected 2 acquires, got %d", stats.AcquireCount)
	}
	if stats.ReleaseCount != 1 {
		t.Errorf("expected 1 release, got %d", stats.ReleaseCount)
	}
	if stats.ExcludeCount != 1 {
		t.Errorf("expected 1 excludes, got %d", stats.ExcludeCount)
	}
	if stats.RemoveCount != 1 {
		t.Errorf("expected 1 remove, got %d", stats.RemoveCount)
	}
	if stats.IncludeCount != 1 {
		t.Errorf("expected 1 include, got %d", stats.IncludeCount)
	}
	if stats.UpdateCount != 1 {
		t.Errorf("expected 1 update, got %d", stats.UpdateCount)
	}
}

func TestTelemetryObserver_Concurrency(t *testing.T) {
	observer := metrics.NewTelemetryObserver[string]()
	var wg sync.WaitGroup

	numGoroutines := 100
	eventsPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				observer.OnEvent(events.Event[string]{Type: events.EventAcquire, ID: "1"})
				observer.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "1"})
				// Call snapshot concurrently to ensure it's safe and doesn't crash
				_ = observer.Snapshot()
			}
		}()
	}

	wg.Wait()
	stats := observer.Snapshot()
	expected := uint64(numGoroutines * eventsPerGoroutine)

	if stats.AcquireCount != expected {
		t.Errorf("expected %d acquires, got %d", expected, stats.AcquireCount)
	}
	if stats.ReleaseCount != expected {
		t.Errorf("expected %d releases, got %d", expected, stats.ReleaseCount)
	}
}
