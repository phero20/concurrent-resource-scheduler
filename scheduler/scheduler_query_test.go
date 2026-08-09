package scheduler_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

func TestGet(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1", Priority: 10})

	res, err := s.Get("r1")
	if err != nil {
		t.Fatalf("Expected Get to succeed, got %v", err)
	}
	if res.ID != "r1" {
		t.Fatalf("Expected r1, got %v", res.ID)
	}

	_, err = s.Get("non-existent")
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound, got %v", err)
	}
}

func TestGet_AfterRemove(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})
	s.Remove("r1")

	_, err := s.Get("r1")
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound when getting removed resource, got %v", err)
	}
}

func TestLen(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	if s.Len() != 0 {
		t.Fatalf("Expected Len to be 0, got %v", s.Len())
	}
	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 20})
	if s.Len() != 2 {
		t.Fatalf("Expected Len to be 2, got %v", s.Len())
	}
}

func TestLen_AfterRemove(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})
	s.Add(&Resource{ID: "r2"})

	if s.Len() != 2 {
		t.Fatalf("Expected Len 2, got %d", s.Len())
	}

	s.Remove("r1")
	if s.Len() != 1 {
		t.Fatalf("Expected Len 1 after remove, got %d", s.Len())
	}

	s.Remove("r2")
	if s.Len() != 0 {
		t.Fatalf("Expected Len 0 after second remove, got %d", s.Len())
	}
}

func TestStats(t *testing.T) {
	cfg := validConfig(2)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 20})
	s.Add(&Resource{ID: "r3", Priority: 30})

	// Acquire one, making it Inactive
	s.Acquire()

	st := s.Stats()
	if st.TotalResources != 3 {
		t.Fatalf("Expected 3 total, got %v", st.TotalResources)
	}
	if st.ActiveResources != 2 {
		t.Fatalf("Expected 2 active, got %v", st.ActiveResources)
	}
	if st.InactiveResources != 1 {
		t.Fatalf("Expected 1 inactive, got %v", st.InactiveResources)
	}
	if st.HeapCount != 2 {
		t.Fatalf("Expected 2 heaps, got %v", st.HeapCount)
	}
	if st.AcquirePolicy != "Exclusive" {
		t.Fatalf("Expected Exclusive policy, got %v", st.AcquirePolicy)
	}
	if st.AcquireStrategy != "RoundRobin" {
		t.Fatalf("Expected RoundRobin strategy, got %v", st.AcquireStrategy)
	}
	if len(st.HeapSizes) != 2 {
		t.Fatalf("Expected 2 heap sizes, got %v", len(st.HeapSizes))
	}
	if st.ActiveResources+st.InactiveResources != st.TotalResources {
		t.Fatalf("Expected Active+Inactive == Total")
	}
	if st.EmptyHeaps+st.NonEmptyHeaps != st.HeapCount {
		t.Fatalf("Expected Empty+NonEmpty == HeapCount")
	}
}
