package cooldown_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phero20/concurrent-resource-scheduler/events"
	"github.com/phero20/concurrent-resource-scheduler/extensions/cooldown"
)

type mockControllerWithError struct {
	mu            sync.Mutex
	excludedCount int
	shouldError   bool
}

func (m *mockControllerWithError) Exclude(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.excludedCount++
	if m.shouldError {
		return errors.New("mock error")
	}
	return nil
}

func (m *mockControllerWithError) Include(id string) error {
	return nil
}

func (m *mockControllerWithError) getExcludedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.excludedCount
}

func TestCooldownManager_ExcludeError(t *testing.T) {
	ctrl := &mockControllerWithError{shouldError: true}
	duration := 10 * time.Millisecond
	manager := cooldown.NewManager[string](ctrl, duration)

	// Fire an event, this should hit the Exclude error path
	manager.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "res-err"})

	// Give the async goroutine time to run and fail
	time.Sleep(20 * time.Millisecond)

	if ctrl.getExcludedCount() != 1 {
		t.Fatalf("Expected exactly 1 exclude attempt, got %d", ctrl.getExcludedCount())
	}

	// Because it failed, the active map should have deleted the ID.
	// If we send it again, it should attempt another Exclude instead of being ignored.
	manager.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "res-err"})
	time.Sleep(20 * time.Millisecond)

	if ctrl.getExcludedCount() != 2 {
		t.Fatalf("Expected exactly 2 exclude attempts (active map was cleared), got %d", ctrl.getExcludedCount())
	}
}
