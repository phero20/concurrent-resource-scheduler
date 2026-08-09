package scheduler_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

func TestAdd_SuccessAndDuplicate(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	r := &Resource{ID: "r1", Priority: 10}

	// First Add should succeed
	err := s.Add(r)
	if err != nil {
		t.Fatalf("Expected Add to succeed, got %v", err)
	}

	// Duplicate Add should fail safely
	err = s.Add(r)
	if err != errors.ErrDuplicateKey {
		t.Fatalf("Expected ErrDuplicateKey, got %v", err)
	}
}

func TestAdd_NilResource(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	var r *Resource = nil
	err := s.Add(r)
	if err != errors.ErrNilResource {
		t.Fatalf("Expected ErrNilResource for typed nil, got %v", err)
	}
}

func TestAdd_Concurrent(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	var wg sync.WaitGroup
	numWorkers := 100
	numOps := 100

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				id := strconv.Itoa(workerID) + "-" + strconv.Itoa(j)
				_ = s.Add(&Resource{ID: id, Priority: j})
			}
		}(i)
	}

	wg.Wait()
	// Success if it didn't panic or deadlock
}

func TestBatchAdd_Success(t *testing.T) {
	s, _ := scheduler.New(validConfig(2))

	batch := []*Resource{
		{ID: "b1", Priority: 10},
		{ID: "b2", Priority: 20},
		{ID: "b3", Priority: 30},
	}

	err := s.BatchAdd(batch)
	if err != nil {
		t.Fatalf("Expected BatchAdd to succeed, got %v", err)
	}
}

func TestBatchAdd_IntraBatchDuplicate(t *testing.T) {
	s, _ := scheduler.New(validConfig(2))

	batch := []*Resource{
		{ID: "b1", Priority: 10},
		{ID: "b2", Priority: 20},
		{ID: "b1", Priority: 30}, // Duplicate inside batch
	}

	err := s.BatchAdd(batch)
	if err != errors.ErrDuplicateKey {
		t.Fatalf("Expected ErrDuplicateKey for intra-batch duplicate, got %v", err)
	}
}

func TestBatchAdd_SchedulerDuplicate(t *testing.T) {
	s, _ := scheduler.New(validConfig(2))
	s.Add(&Resource{ID: "existing", Priority: 10})

	batch := []*Resource{
		{ID: "b1", Priority: 10},
		{ID: "existing", Priority: 20}, // Exists in scheduler
	}

	err := s.BatchAdd(batch)
	if err != errors.ErrDuplicateKey {
		t.Fatalf("Expected ErrDuplicateKey for scheduler duplicate, got %v", err)
	}
}

func TestBatchAdd_Empty(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))

	err := s.BatchAdd(nil)
	if err != nil {
		t.Fatalf("Expected nil BatchAdd to succeed quietly, got %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Expected Len 0, got %d", s.Len())
	}

	err = s.BatchAdd([]*Resource{})
	if err != nil {
		t.Fatalf("Expected empty BatchAdd to succeed quietly, got %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Expected Len 0, got %d", s.Len())
	}
}

func TestBatchAdd_ConcurrentStress(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	numWorkers := 50
	batchSize := 20
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var batch []*Resource
			for j := 0; j < batchSize; j++ {
				id := "w" + strconv.Itoa(workerID) + "-b" + strconv.Itoa(j)
				batch = append(batch, &Resource{ID: id, Priority: j})
			}
			_ = s.BatchAdd(batch)
		}(i)
	}

	wg.Wait()

	expected := numWorkers * batchSize
	if s.Len() != expected {
		t.Fatalf("Expected %d resources, got %d", expected, s.Len())
	}
}
