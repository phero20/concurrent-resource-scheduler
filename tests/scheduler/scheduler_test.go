package scheduler_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/placement"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type Resource struct {
	ID       string
	Priority int
}

func keyFunc(r *Resource) string {
	return r.ID
}

func cmpFunc(a, b *Resource) int {
	if a.Priority > b.Priority {
		return -1 // negative ranks a ahead of b
	} else if a.Priority < b.Priority {
		return 1
	}
	return 0
}

func validConfig(heapCount int) config.Config[*Resource, string] {
	return config.Config[*Resource, string]{
		HeapCount:     heapCount,
		KeyFunc:       keyFunc,
		Comparator:    cmpFunc,
		AcquirePolicy: config.Shared,
	}
}

func TestNewScheduler(t *testing.T) {
	s, err := scheduler.New(validConfig(2))
	if err != nil {
		t.Fatalf("Expected nil error on New, got %v", err)
	}
	if s == nil {
		t.Fatalf("Expected scheduler instance, got nil")
	}
}

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

type badStrategy int

func (b badStrategy) Select(view placement.ShardView) int {
	return int(b)
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

func TestUpdate_AfterRemove(t *testing.T) {
	s, _ := scheduler.New(validConfig(1))
	s.Add(&Resource{ID: "r1"})
	s.Remove("r1")

	err := s.Update(&Resource{ID: "r1", Priority: 50})
	if err != errors.ErrResourceNotFound {
		t.Fatalf("Expected ErrResourceNotFound when updating removed resource, got %v", err)
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

func TestScheduler_MixedConcurrentStress(t *testing.T) {
	cfg := validConfig(4)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	numWorkers := 100
	numOps := 100
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				id := "w" + strconv.Itoa(workerID) + "-op" + strconv.Itoa(j)

				// Pick an operation pseudorandomly based on index
				op := (workerID + j) % 8
				switch op {
				case 0:
					_ = s.Add(&Resource{ID: id, Priority: j})
				case 1:
					_, _ = s.Acquire()
				case 2:
					_ = s.Release(id)
				case 3:
					_ = s.Update(&Resource{ID: id, Priority: j + 10})
				case 4:
					_ = s.Remove(id)
				case 5:
					_ = s.Exclude(id)
				case 6:
					_ = s.Include(id)
				case 7:
					_, _ = s.Get(id)
				}
			}
		}(i)
	}

	wg.Wait()
}
