package heap_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/internal/heap"
	"github.com/phero20/concurrent-resource-scheduler/internal/node"
)

// Define dummy types for the test
type dummyResource struct {
	Priority int
}

func dummyComparator(a, b *dummyResource) int {
	if a.Priority > b.Priority {
		return -1
	} else if a.Priority < b.Priority {
		return 1
	}
	return 0
}

func TestHeap_Len(t *testing.T) {
	h := heap.New[*dummyResource, int](dummyComparator)

	if h.Len() != 0 {
		t.Errorf("Expected length 0 for empty heap, got %d", h.Len())
	}

	h.Push(&node.HeapNode[*dummyResource, int]{
		Value: &dummyResource{Priority: 10},
		Key:   1,
	})

	if h.Len() != 1 {
		t.Errorf("Expected length 1 after push, got %d", h.Len())
	}
}
