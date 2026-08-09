package scheduler_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

func TestUpdate_Active(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 20}) // r2 is higher priority

	// Update r1 to have highest priority
	err := s.Update(&Resource{ID: "r1", Priority: 30})
	if err != nil {
		t.Fatalf("Expected Update to succeed, got %v", err)
	}

	acq, _ := s.Acquire()
	if acq.ID != "r1" {
		t.Fatalf("Expected r1 (now highest priority), got %v", acq.ID)
	}
}

func TestUpdate_Inactive(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Acquire() // r1 is now inactive

	// Update r1's priority
	err := s.Update(&Resource{ID: "r1", Priority: 30})
	if err != nil {
		t.Fatalf("Expected Update to succeed, got %v", err)
	}

	// Release it and ensure it's still Acquirable
	s.Release("r1")
	acq, _ := s.Acquire()
	if acq.Priority != 30 {
		t.Fatalf("Expected priority to be updated, got %d", acq.Priority)
	}
}

func TestUpdate_AfterRemove(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})
	s.Remove("r1")

	err := s.Update(&Resource{ID: "r1", Priority: 50})
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound when updating removed resource, got %v", err)
	}
}

func TestRemove_Active(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 20}) // r2 is highest

	err := s.Remove("r2")
	if err != nil {
		t.Fatalf("Expected Remove to succeed, got %v", err)
	}

	// Acquire should give r1
	acq, _ := s.Acquire()
	if acq.ID != "r1" {
		t.Fatalf("Expected r1, got %v", acq.ID)
	}
}

func TestRemove_Inactive(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Acquire() // r1 is now inactive

	err := s.Remove("r1")
	if err != nil {
		t.Fatalf("Expected Remove to succeed, got %v", err)
	}

	err = s.Release("r1")
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound after Remove, got %v", err)
	}
}

func TestRemove_Twice(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})

	err := s.Remove("r1")
	if err != nil {
		t.Fatalf("Expected first Remove to succeed, got %v", err)
	}

	err = s.Remove("r1")
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound on second Remove, got %v", err)
	}
}

func TestExcludeInclude(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 20}) // r2 is highest

	// Exclude r2
	err := s.Exclude("r2")
	if err != nil {
		t.Fatalf("Expected Exclude to succeed, got %v", err)
	}

	// Now r1 should be acquired
	acq, _ := s.Acquire()
	if acq.ID != "r1" {
		t.Fatalf("Expected r1 because r2 is excluded, got %v", acq.ID)
	}

	// Include r2
	err = s.Include("r2")
	if err != nil {
		t.Fatalf("Expected Include to succeed, got %v", err)
	}

	// Since Shared is used, r2 is now acquirable and has higher priority than r1 (which is still in heap)
	acq2, _ := s.Acquire()
	if acq2.ID != "r2" {
		t.Fatalf("Expected r2 to be acquired after Include, got %v", acq2.ID)
	}
}

func TestExclude_Twice(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})

	err := s.Exclude("r1")
	if err != nil {
		t.Fatalf("Expected first Exclude to succeed, got %v", err)
	}

	err = s.Exclude("r1")
	if err != errors.ErrResourceNotActive {
		t.Fatalf("Expected ErrResourceNotActive on second Exclude, got %v", err)
	}
}

func TestInclude_Twice(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)
	s.Add(&Resource{ID: "r1"})
	s.Acquire() // Makes it inactive

	err := s.Include("r1")
	if err != nil {
		t.Fatalf("Expected first Include to succeed, got %v", err)
	}

	err = s.Include("r1")
	if err != errors.ErrResourceNotInactive {
		t.Fatalf("Expected ErrResourceNotInactive on second Include, got %v", err)
	}
}
