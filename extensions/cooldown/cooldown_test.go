package cooldown_test

import (
	"sync"
	"testing"
	"time"

	"github.com/phero20/concurrent-resource-scheduler/events"
	"github.com/phero20/concurrent-resource-scheduler/extensions/cooldown"
)

type mockController struct {
	mu            sync.Mutex
	excludedCount int
	includedCount int
}

func (m *mockController) Exclude(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.excludedCount++
	return nil
}

func (m *mockController) Include(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.includedCount++
	return nil
}

func (m *mockController) getCounts() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.excludedCount, m.includedCount
}

func TestCooldownManager(t *testing.T) {
	ctrl := &mockController{}
	duration := 50 * time.Millisecond
	manager := cooldown.NewManager[string](ctrl, duration)

	// Sending a non-Release event should do nothing
	manager.OnEvent(events.Event[string]{Type: events.EventAcquire, ID: "res-1"})
	time.Sleep(10 * time.Millisecond) // Give goroutine time if it spawned (it shouldn't)

	ex, in := ctrl.getCounts()
	if ex != 0 || in != 0 {
		t.Errorf("Expected 0 excludes/includes for non-Release event, got %d ex, %d in", ex, in)
	}

	// Sending a Release event should trigger an Exclude, followed by an Include after the duration
	manager.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "res-2"})

	// Give the initial Exclude goroutine time to run
	time.Sleep(10 * time.Millisecond)

	ex, in = ctrl.getCounts()
	if ex != 1 || in != 0 {
		t.Errorf("Expected 1 exclude and 0 includes immediately after release, got %d ex, %d in", ex, in)
	}

	// Wait for the cooldown duration to pass
	time.Sleep(60 * time.Millisecond)

	ex, in = ctrl.getCounts()
	if ex != 1 || in != 1 {
		t.Errorf("Expected 1 exclude and 1 include after cooldown, got %d ex, %d in", ex, in)
	}

	// Test duplicate release events
	ctrl = &mockController{}
	manager = cooldown.NewManager[string](ctrl, duration)

	// Fire 10 concurrent release events for the same resource
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.OnEvent(events.Event[string]{Type: events.EventRelease, ID: "res-duplicate"})
		}()
	}
	wg.Wait()

	// Give the initial Exclude goroutine time to run
	time.Sleep(10 * time.Millisecond)

	ex, in = ctrl.getCounts()
	if ex != 1 || in != 0 {
		t.Errorf("Expected exactly 1 exclude for duplicate releases, got %d ex", ex)
	}

	// Wait for the cooldown duration to pass
	time.Sleep(60 * time.Millisecond)

	ex, in = ctrl.getCounts()
	if ex != 1 || in != 1 {
		t.Errorf("Expected exactly 1 include for duplicate releases, got %d in", in)
	}
}
