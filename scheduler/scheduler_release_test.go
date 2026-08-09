package scheduler_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

func TestRelease_Exclusive(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	r := &Resource{ID: "r1", Priority: 10}
	s.Add(r)

	_, _ = s.Acquire() // Marks r1 as Inactive

	// Release r1
	err := s.Release("r1")
	if err != nil {
		t.Fatalf("Expected Release to succeed, got %v", err)
	}

	// Now it can be Acquired again
	acq, _ := s.Acquire()
	if acq.ID != "r1" {
		t.Fatalf("Expected r1, got %v", acq.ID)
	}
	// Now it is Inactive again because of Acquire.
	// Let's release it to make it Active.
	_ = s.Release("r1")

	// Releasing an already active resource should fail
	err = s.Release("r1")
	if err != errors.ErrResourceNotInactive {
		t.Fatalf("Expected ErrResourceNotInactive, got %v", err)
	}
}

func TestRelease_SharedError(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1", Priority: 10})

	err := s.Release("r1")
	if err != errors.ErrNotExclusive {
		t.Fatalf("Expected ErrNotExclusive for Shared policy, got %v", err)
	}
}

func TestRelease_NotFound(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	err := s.Release("non-existent")
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound, got %v", err)
	}
}
