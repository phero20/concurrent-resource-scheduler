package scheduler_test

import (
	"sync"
	"testing"
	"time"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type slowObserver struct {
	mu           sync.Mutex
	received     int
	blockTrigger chan struct{}
}

func (s *slowObserver) OnEvent(e events.Event[string]) {
	<-s.blockTrigger // deliberately block to fill the buffer

	s.mu.Lock()
	defer s.mu.Unlock()
	s.received++
}

func (s *slowObserver) getReceived() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

func TestEventDispatcher_DropPolicy(t *testing.T) {
	obs := &slowObserver{
		blockTrigger: make(chan struct{}),
	}

	cfg := config.Config[string, string]{
		HeapCount:  1,
		Comparator: func(a, b string) int { return 0 },
		KeyFunc:    func(r string) string { return r },
		Observers:  []events.Observer[string]{obs},
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	defer sched.Shutdown()

	// Fill up the buffer (capacity is 4096).
	// We will send 5000 events. The first will get stuck in the observer.
	// The next 4096 will fill the channel.
	// The remaining ~903 should be dropped instantly, proving emit() does not block.
	
	start := time.Now()
	for i := 0; i < 5000; i++ {
		sched.EmitForTesting(events.EventAcquire, "test-id")
	}
	elapsed := time.Since(start)

	// Since emit is non-blocking, it should finish almost instantly, definitely well under 1 second.
	if elapsed > time.Second {
		t.Fatalf("Emit blocked! Took %v for 5000 emits (buffer size is 4096)", elapsed)
	}

	// Unblock the observer so it can finish processing
	close(obs.blockTrigger)
	
	// Allow the background goroutine a moment to process the event
	time.Sleep(10 * time.Millisecond)

	// Verify that at least one event made it through to the slow observer
	if obs.getReceived() == 0 {
		t.Errorf("Observer received 0 events, expected at least 1")
	}
}

func TestEventDispatcher_NoObservers(t *testing.T) {
	cfg := config.Config[string, string]{
		HeapCount:  1,
		Comparator: func(a, b string) int { return 0 },
		KeyFunc:    func(r string) string { return r },
		// No observers
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	
	// Should do nothing and not panic or block
	sched.EmitForTesting(events.EventAcquire, "test-id")
	
	sched.Shutdown()
}

func TestEventDispatcher_NilObserver(t *testing.T) {
	cfg := config.Config[string, string]{
		HeapCount:  1,
		Comparator: func(a, b string) int { return 0 },
		KeyFunc:    func(r string) string { return r },
		Observers:  []events.Observer[string]{nil}, // Nil observer
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	
	// Should gracefully skip the nil observer without panicking
	sched.EmitForTesting(events.EventAcquire, "test-id")
	
	// Allow background loop to process the event
	time.Sleep(10 * time.Millisecond)

	sched.Shutdown()
}
