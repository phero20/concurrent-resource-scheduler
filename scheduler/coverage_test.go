package scheduler_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type coverageAffinity string

func (c coverageAffinity) AppendAffinityBytes(dst []byte) []byte {
	return append(dst, string(c)...)
}

func TestAcquireByAffinity_Closed(t *testing.T) {
	sched, _ := scheduler.New(validConfig(1))
	sched.Shutdown()

	_, err := sched.AcquireByAffinity(coverageAffinity("test"))
	if err != errors.ErrSchedulerClosed {
		t.Errorf("Expected ErrSchedulerClosed, got %v", err)
	}
}

func TestBatchAdd_NilResource(t *testing.T) {
	sched, _ := scheduler.New(validConfig(1))

	err := sched.BatchAdd([]*Resource{nil})
	if err != errors.ErrNilResource {
		t.Errorf("Expected ErrNilResource, got %v", err)
	}
}

func TestUpdate_NilResource(t *testing.T) {
	sched, _ := scheduler.New(validConfig(1))

	err := sched.Update(nil)
	if err != errors.ErrNilResource {
		t.Errorf("Expected ErrNilResource, got %v", err)
	}
}

func TestStats_Closed(t *testing.T) {
	sched, _ := scheduler.New(validConfig(1))
	sched.Shutdown()

	stats := sched.Stats()
	if stats.HeapCount != 1 || stats.TotalResources != 0 {
		t.Errorf("Expected empty stats for closed scheduler, got %+v", stats)
	}
}

func TestStats_EmptyHeap(t *testing.T) {
	// Create scheduler with 2 heaps.
	sched, _ := scheduler.New(validConfig(2))

	// Add 1 resource. One heap will have it, one will be empty.
	sched.Add(&Resource{ID: "r1", Priority: 1})

	stats := sched.Stats()
	if stats.EmptyHeaps != 1 {
		t.Errorf("Expected 1 empty heap, got %d", stats.EmptyHeaps)
	}
	if stats.NonEmptyHeaps != 1 {
		t.Errorf("Expected 1 non-empty heap, got %d", stats.NonEmptyHeaps)
	}
}

func TestUtil_IsNil_TypedNil(t *testing.T) {
	sched, _ := scheduler.New(validConfig(1))
	
	// A typed nil pointer passed as an interface is a classic Go gotcha.
	// The internal isNil handles this.
	var typedNil *Resource = nil
	err := sched.Add(typedNil)
	if err != errors.ErrNilResource {
		t.Errorf("Expected ErrNilResource for typed nil, got %v", err)
	}
}
