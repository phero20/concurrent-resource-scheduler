package scheduler_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

func TestShutdown(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1", Priority: 10})

	s.Shutdown()

	if err := s.Add(&Resource{ID: "r2", Priority: 20}); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Add, got %v", err)
	}
	if err := s.BatchAdd([]*Resource{{ID: "r3"}}); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on BatchAdd, got %v", err)
	}
	if _, err := s.Acquire(); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Acquire, got %v", err)
	}
	if err := s.Release("r1"); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Release, got %v", err)
	}
	if err := s.Update(&Resource{ID: "r1"}); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Update, got %v", err)
	}
	if err := s.Remove("r1"); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Remove, got %v", err)
	}
	if err := s.Exclude("r1"); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Exclude, got %v", err)
	}
	if err := s.Include("r1"); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Include, got %v", err)
	}
	if _, err := s.Get("r1"); err != errors.ErrSchedulerClosed {
		t.Fatalf("Expected ErrSchedulerClosed on Get, got %v", err)
	}

	// Len() shouldn't return error
	if s.Len() != 1 {
		t.Fatalf("Expected Len to work after shutdown")
	}

	// Stats() shouldn't return error
	st := s.Stats()
	if st.TotalResources != 1 {
		t.Fatalf("Expected Stats to work after shutdown")
	}
}
