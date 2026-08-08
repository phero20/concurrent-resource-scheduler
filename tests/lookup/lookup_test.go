package lookup_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/internal/lookup"
	"github.com/feroz/concurrent-resource-scheduler/internal/node"
)

func TestLookupAddAndGet(t *testing.T) {
	m := lookup.New[string, int]()

	n := &node.HeapNode[string, int]{
		Value: "resource-1",
		Key:   1,
	}

	err := m.Add(1, n)
	if err != nil {
		t.Fatalf("Expected nil error on first add, got %v", err)
	}

	retrieved := m.Get(1)
	if retrieved != n {
		t.Fatalf("Expected to retrieve identical node pointer, got %v", retrieved)
	}

	if retrieved.Value != "resource-1" {
		t.Errorf("Expected node value 'resource-1', got '%v'", retrieved.Value)
	}

	if m.Get(99) != nil {
		t.Fatalf("Expected Get() on unknown key to return nil")
	}
}

func TestLookupDuplicateRejection(t *testing.T) {
	m := lookup.New[string, int]()

	n1 := &node.HeapNode[string, int]{Value: "first"}
	err := m.Add(1, n1)
	if err != nil {
		t.Fatalf("Expected nil error on first add, got %v", err)
	}

	n2 := &node.HeapNode[string, int]{Value: "second"}
	err = m.Add(1, n2)
	if err != errors.ErrDuplicateKey {
		t.Fatalf("Expected ErrDuplicateKey, got %v", err)
	}

	// Verify the original node was kept
	retrieved := m.Get(1)
	if retrieved != n1 {
		t.Fatalf("Expected original node n1 to remain, got %v", retrieved)
	}
}

func TestLookupRemoveAndLen(t *testing.T) {
	m := lookup.New[string, int]()

	m.Add(1, &node.HeapNode[string, int]{Value: "A"})
	m.Add(2, &node.HeapNode[string, int]{Value: "B"})

	if m.Len() != 2 {
		t.Fatalf("Expected Len() == 2, got %d", m.Len())
	}

	m.Remove(1)

	if m.Len() != 1 {
		t.Fatalf("Expected Len() == 1, got %d", m.Len())
	}

	if m.Get(1) != nil {
		t.Fatalf("Expected Get(1) to be nil after removal")
	}

	// Removing non-existent key shouldn't panic or error
	m.Remove(99)
	if m.Len() != 1 {
		t.Fatalf("Expected Len() == 1 after removing non-existent key, got %d", m.Len())
	}
}

func TestLookupConcurrentAccess(t *testing.T) {
	m := lookup.New[string, string]()
	var wg sync.WaitGroup

	numWorkers := 100
	numOps := 100

	// Concurrent Adds
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := strconv.Itoa(workerID) + "-" + strconv.Itoa(j)
				n := &node.HeapNode[string, string]{Key: key}
				_ = m.Add(key, n)
			}
		}(i)
	}
	wg.Wait()

	expectedLen := numWorkers * numOps
	if m.Len() != expectedLen {
		t.Fatalf("Expected %d elements, got %d", expectedLen, m.Len())
	}

	// Concurrent Gets and Removes
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := strconv.Itoa(workerID) + "-" + strconv.Itoa(j)
				_ = m.Get(key)
				m.Remove(key)
			}
		}(i)
	}
	wg.Wait()

	if m.Len() != 0 {
		t.Fatalf("Expected 0 elements after concurrent remove, got %d", m.Len())
	}
}
