package scheduler_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

func TestAcquire_Shared(t *testing.T) {
	s, _ := scheduler.New(validConfig(2))

	r := &Resource{ID: "r1", Priority: 10}
	s.Add(r)

	// Acquire should return r1 and leave it in the heap (Shared)
	acqRes, err := s.Acquire()
	if err != nil {
		t.Fatalf("Expected Acquire to succeed, got %v", err)
	}
	if acqRes.ID != "r1" {
		t.Fatalf("Expected r1, got %v", acqRes.ID)
	}

	// Because policy is Shared, a second Acquire should yield r1 again
	acqRes2, _ := s.Acquire()
	if acqRes2.ID != "r1" {
		t.Fatalf("Expected r1 again under Shared policy, got %v", acqRes2.ID)
	}
}

func TestAcquire_Exclusive(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	r1 := &Resource{ID: "r1", Priority: 10}
	r2 := &Resource{ID: "r2", Priority: 20}
	s.BatchAdd([]*Resource{r1, r2})

	// Acquire should return r2 (higher priority)
	acq1, err := s.Acquire()
	if err != nil {
		t.Fatalf("Expected Acquire to succeed, got %v", err)
	}
	if acq1.ID != "r2" {
		t.Fatalf("Expected r2, got %v", acq1.ID)
	}

	// Acquire should return r1 (r2 is Exclusive/Inactive)
	acq2, _ := s.Acquire()
	if acq2.ID != "r1" {
		t.Fatalf("Expected r1, got %v", acq2.ID)
	}

	// Acquire should return ErrNoResourceAvailable (both are Inactive)
	_, err = s.Acquire()
	if err != errors.ErrNoResourceAvailable {
		t.Fatalf("Expected ErrNoResourceAvailable, got %v", err)
	}
}

func TestAcquire_InvalidStrategy(t *testing.T) {
	cfg := validConfig(2)

	// Test -1
	cfg.AcquireStrategy = badStrategy(-1)
	s1, _ := scheduler.New(cfg)
	if _, err := s1.Acquire(); err != errors.ErrInvalidAcquireStrategy {
		t.Fatalf("Expected ErrInvalidAcquireStrategy for -1, got %v", err)
	}

	// Test HeapCount (out of bounds)
	cfg.AcquireStrategy = badStrategy(2)
	s2, _ := scheduler.New(cfg)
	if _, err := s2.Acquire(); err != errors.ErrInvalidAcquireStrategy {
		t.Fatalf("Expected ErrInvalidAcquireStrategy for HeapCount, got %v", err)
	}

	// Test very large index
	cfg.AcquireStrategy = badStrategy(9999)
	s3, _ := scheduler.New(cfg)
	if _, err := s3.Acquire(); err != errors.ErrInvalidAcquireStrategy {
		t.Fatalf("Expected ErrInvalidAcquireStrategy for 9999, got %v", err)
	}
}

func TestAcquire_ConcurrentStress_Shared(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	// Pre-populate
	for i := 0; i < 100; i++ {
		_ = s.Add(&Resource{ID: "r" + strconv.Itoa(i), Priority: i})
	}

	numWorkers := 100
	numOps := 100
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				_, _ = s.Acquire()
			}
		}()
	}

	wg.Wait()
}

func TestAcquire_ConcurrentStress_Exclusive(t *testing.T) {
	cfg := validConfig(4)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_ = s.Add(&Resource{ID: "r" + strconv.Itoa(i), Priority: i})
	}

	numWorkers := 100
	numOps := 100
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				_, _ = s.Acquire()
			}
		}()
	}

	wg.Wait()
}

func TestAcquireByAffinity_NilKey(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	_, err := s.AcquireByAffinity(nil)
	if err != errors.ErrNilAffinityIdentifier {
		t.Fatalf("Expected ErrNilAffinityIdentifier, got %v", err)
	}
}

func TestAcquireByAffinity_Deterministic(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	// Add 4 resources so hopefully they distribute across heaps
	s.Add(&Resource{ID: "r1", Priority: 10})
	s.Add(&Resource{ID: "r2", Priority: 10})
	s.Add(&Resource{ID: "r3", Priority: 10})
	s.Add(&Resource{ID: "r4", Priority: 10})

	// Use a fixed string key. If the state is unmutated (and policy is Shared),
	// the same string key will always route to the same shard and return the same element.
	affinityKey := stringAffinity("session-123")

	res1, _ := s.AcquireByAffinity(affinityKey)
	res2, _ := s.AcquireByAffinity(affinityKey)

	if res1 == nil || res2 == nil {
		t.Fatalf("Expected resource to be acquired")
	}

	if res1.ID != res2.ID {
		t.Fatalf("AcquireByAffinity returned different resources for same affinityKey: %v vs %v", res1.ID, res2.ID)
	}
}

func TestAcquireByAffinity_EmptyFallback(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	// Add only one resource
	s.Add(&Resource{ID: "r1", Priority: 10})

	// Try acquiring with 10 different keys to ensure that even if the
	// hash hits an empty shard, it falls back and finds the resource.
	for i := 0; i < 10; i++ {
		res, err := s.AcquireByAffinity(stringAffinity("key-" + strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if res == nil || res.ID != "r1" {
			t.Fatalf("Expected r1, got %v", res)
		}
	}
}

func TestAcquireByAffinity_Concurrent(t *testing.T) {
	s, _ := scheduler.New(validConfig(4))

	for i := 0; i < 100; i++ {
		s.Add(&Resource{ID: "r" + strconv.Itoa(i), Priority: i})
	}

	var wg sync.WaitGroup
	numWorkers := 100

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = s.AcquireByAffinity(stringAffinity("worker-" + strconv.Itoa(workerID) + "-op-" + strconv.Itoa(j)))
			}
		}(i)
	}

	wg.Wait()
}
