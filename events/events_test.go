package events_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/events"
)

func TestEventTypeString(t *testing.T) {
	tests := []struct {
		eventType events.EventType
		expected  string
	}{
		{events.EventAdd, "Add"},
		{events.EventAcquire, "Acquire"},
		{events.EventRelease, "Release"},
		{events.EventExclude, "Exclude"},
		{events.EventInclude, "Include"},
		{events.EventRemove, "Remove"},
		{events.EventUpdate, "Update"},
		{events.EventType(999), "Unknown"},
	}

	for _, tc := range tests {
		actual := tc.eventType.String()
		if actual != tc.expected {
			t.Errorf("Expected %q, got %q", tc.expected, actual)
		}
	}
}

// mockObserver ensures that the Observer interface can be correctly implemented.
type mockObserver struct {
	events []events.Event[string]
}

func (m *mockObserver) OnEvent(e events.Event[string]) {
	m.events = append(m.events, e)
}

func TestObserverInterface(t *testing.T) {
	obs := &mockObserver{}
	
	e := events.Event[string]{
		Type: events.EventAcquire,
		ID:   "res-1",
	}

	// This validates compile-time adherence to the interface.
	var _ events.Observer[string] = obs

	obs.OnEvent(e)

	if len(obs.events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(obs.events))
	}
	if obs.events[0].Type != events.EventAcquire {
		t.Errorf("Expected EventAcquire, got %v", obs.events[0].Type)
	}
	if obs.events[0].ID != "res-1" {
		t.Errorf("Expected 'res-1', got %v", obs.events[0].ID)
	}
}
