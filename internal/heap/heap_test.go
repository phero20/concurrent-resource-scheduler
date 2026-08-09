package heap_test

import (
	"sort"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/internal/heap"
	"github.com/phero20/concurrent-resource-scheduler/internal/node"
)

// A simple integer comparator for testing.
// A negative result ranks a ahead of b (min-heap behavior).
func intComparator(a, b int) int {
	return a - b
}

// validateIndexes verifies the internal Index of all remaining active nodes.
// It ensures that they perfectly match 0..N-1 with no duplicates.
func validateIndexes(t *testing.T, nodes []*node.HeapNode[int, string]) {
	t.Helper()
	var activeIndexes []int
	for _, n := range nodes {
		// Only collect nodes that haven't been popped or removed.
		if n.Index != -1 {
			activeIndexes = append(activeIndexes, n.Index)
		}
	}

	sort.Ints(activeIndexes)
	for expected, actual := range activeIndexes {
		if expected != actual {
			t.Fatalf("Index invariant violated: missing or duplicate index %d. Active indices: %v", expected, activeIndexes)
		}
	}
}

func TestHeapSingleElement(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	n := &node.HeapNode[int, string]{Value: 42, Key: "a", IsActive: true}
	h.Push(n)

	if h.Peek() != n {
		t.Fatalf("Expected Peek() to return n, got %v", h.Peek())
	}

	popped := h.Pop()
	if popped != n {
		t.Fatalf("Expected Pop() to return n, got %v", popped)
	}

	if popped.Index != -1 {
		t.Fatalf("Expected popped index to be -1, got %d", popped.Index)
	}

	if h.Peek() != nil {
		t.Fatal("Expected Peek() to be nil on empty heap")
	}
	if h.Pop() != nil {
		t.Fatal("Expected Pop() to be nil on empty heap")
	}
}

func TestHeapPushPop(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	values := []int{5, 3, 7, 1, 4}
	var allNodes []*node.HeapNode[int, string]

	for _, v := range values {
		n := &node.HeapNode[int, string]{Value: v, Key: "k"}
		h.Push(n)
		allNodes = append(allNodes, n)
		validateIndexes(t, allNodes)
	}

	expected := []int{1, 3, 4, 5, 7}
	for _, expectedVal := range expected {
		top := h.Peek()
		if top == nil || top.Value != expectedVal {
			t.Fatalf("Expected Peek() to return %d, got %v", expectedVal, top)
		}

		popped := h.Pop()
		if popped == nil || popped.Value != expectedVal {
			t.Fatalf("Expected Pop() to return %d, got %v", expectedVal, popped)
		}

		if popped.Index != -1 {
			t.Fatalf("Expected popped index to be -1, got %d", popped.Index)
		}
		validateIndexes(t, allNodes)
	}
}

func TestHeapFix(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	n1 := &node.HeapNode[int, string]{Value: 10, Key: "a"}
	n2 := &node.HeapNode[int, string]{Value: 20, Key: "b"}
	n3 := &node.HeapNode[int, string]{Value: 30, Key: "c"}

	allNodes := []*node.HeapNode[int, string]{n1, n2, n3}
	h.Push(n1)
	h.Push(n2)
	h.Push(n3)
	validateIndexes(t, allNodes)

	// Change n3's priority to make it the highest (min-heap)
	n3.Value = 5
	h.Fix(n3.Index)
	validateIndexes(t, allNodes)

	if top := h.Pop(); top != n3 {
		t.Fatalf("Expected n3 to be the top node after Fix, got %v", top)
	}
	validateIndexes(t, allNodes)

	// Change n1's priority to make it worse than n2
	n1.Value = 50
	h.Fix(n1.Index)
	validateIndexes(t, allNodes)

	if top := h.Pop(); top != n2 {
		t.Fatalf("Expected n2 to be the top node after Fix, got %v", top)
	}
}

func TestHeapRemove(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	n1 := &node.HeapNode[int, string]{Value: 10, Key: "a"}
	n2 := &node.HeapNode[int, string]{Value: 20, Key: "b"}
	n3 := &node.HeapNode[int, string]{Value: 30, Key: "c"}
	n4 := &node.HeapNode[int, string]{Value: 40, Key: "d"}

	allNodes := []*node.HeapNode[int, string]{n1, n2, n3, n4}
	h.Push(n1)
	h.Push(n2)
	h.Push(n3)
	h.Push(n4)
	validateIndexes(t, allNodes)

	removed := h.Remove(n2.Index)
	if removed != n2 {
		t.Fatalf("Expected Remove() to return n2, got %v", removed)
	}
	if removed.Index != -1 {
		t.Fatalf("Expected removed index to be -1, got %d", removed.Index)
	}
	validateIndexes(t, allNodes)

	expected := []int{10, 30, 40}
	for _, expectedVal := range expected {
		popped := h.Pop()
		if popped == nil || popped.Value != expectedVal {
			t.Fatalf("Expected Pop() to return %d, got %v", expectedVal, popped)
		}
		validateIndexes(t, allNodes)
	}
}

func TestHeapDuplicatePriorities(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	n1 := &node.HeapNode[int, string]{Value: 10, Key: "a"}
	n2 := &node.HeapNode[int, string]{Value: 10, Key: "b"}
	n3 := &node.HeapNode[int, string]{Value: 10, Key: "c"}
	n4 := &node.HeapNode[int, string]{Value: 5, Key: "d"}

	allNodes := []*node.HeapNode[int, string]{n1, n2, n3, n4}
	h.Push(n1)
	h.Push(n2)
	h.Push(n3)
	h.Push(n4)
	validateIndexes(t, allNodes)

	// We expect the first element to be n4 (value 5)
	if h.Pop() != n4 {
		t.Fatalf("Expected n4 to be popped first")
	}

	// We pop the three duplicates. We do not assert order among them.
	poppedValues := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		p := h.Pop()
		if p == nil {
			t.Fatalf("Expected a node, got nil")
		}
		poppedValues = append(poppedValues, p.Value)
		validateIndexes(t, allNodes)
	}

	for _, v := range poppedValues {
		if v != 10 {
			t.Fatalf("Expected popped value to be 10, got %d", v)
		}
	}

	if h.Peek() != nil {
		t.Fatalf("Expected heap to be empty")
	}
}

func TestHeapPanicsOnInvalidIndex(t *testing.T) {
	h := heap.New[int, string](intComparator)
	h.Lock()
	defer h.Unlock()

	n1 := &node.HeapNode[int, string]{Value: 10, Key: "a"}
	h.Push(n1)

	assertPanic(t, "Fix(-1)", func() { h.Fix(-1) })
	assertPanic(t, "Fix(10)", func() { h.Fix(10) })
	assertPanic(t, "Remove(-1)", func() { h.Remove(-1) })
	assertPanic(t, "Remove(10)", func() { h.Remove(10) })
}

func assertPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic in %s, but did not panic", name)
		}
	}()
	f()
}
