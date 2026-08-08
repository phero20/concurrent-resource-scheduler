package heap

import (
	containerheap "container/heap"
	"sync"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/internal/node"
)

// heapData implements container/heap.Interface.
// It maintains the priority queue invariants using the provided comparator.
// It never performs locking and is strictly an internal implementation detail.
// The scheduler never accesses heapData directly.
type heapData[T any, ID comparable] struct {
	nodes      []*node.HeapNode[T, ID]
	comparator config.Comparator[T]
}

// Len returns the number of nodes in the heap.
func (d *heapData[T, ID]) Len() int {
	return len(d.nodes)
}

// Less reports whether the element with index i should sort before the element with index j.
func (d *heapData[T, ID]) Less(i, j int) bool {
	// A negative result from the comparator ranks a ahead of b.
	return d.comparator(d.nodes[i].Value, d.nodes[j].Value) < 0
}

// Swap swaps the elements with indexes i and j, and updates their stored indices.
func (d *heapData[T, ID]) Swap(i, j int) {
	d.nodes[i], d.nodes[j] = d.nodes[j], d.nodes[i]
	d.nodes[i].Index = i
	d.nodes[j].Index = j
}

// Push adds an element to the heap data structure.
// This is the container/heap.Interface method, which accepts any.
func (d *heapData[T, ID]) Push(x any) {
	n := x.(*node.HeapNode[T, ID])
	n.Index = len(d.nodes)
	d.nodes = append(d.nodes, n)
}

// Pop removes and returns the highest-priority element from the heap data structure.
// This is the container/heap.Interface method, which returns any.
func (d *heapData[T, ID]) Pop() any {
	old := d.nodes
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.Index = -1 // Mark as invalid
	d.nodes = old[0 : n-1]
	return item
}

// Heap represents a single independently locked priority queue shard.
//
// Usage Contracts:
//   - Heap methods maintain heap ordering only.
//   - Heap methods never validate scheduler invariants (e.g., active/inactive state).
//   - Synchronization requirements apply to all wrapper methods: the caller
//     must hold the Heap lock (via Lock/Unlock) before invoking Push, Pop, Peek, Fix, or Remove.
//     Explicit locking is required by the architecture to support compound scheduler operations.
//   - Heap strictly owns synchronization for its internal heapData.
type Heap[T any, ID comparable] struct {
	// mu protects the internal heapData.
	mu   sync.Mutex
	data heapData[T, ID]
}

// New creates a new empty Heap. It assumes the comparator is already validated.
func New[T any, ID comparable](comp config.Comparator[T]) *Heap[T, ID] {
	return &Heap[T, ID]{
		data: heapData[T, ID]{
			nodes:      make([]*node.HeapNode[T, ID], 0),
			comparator: comp,
		},
	}
}

// Lock acquires the exclusive mutex for this Heap Shard.
// The scheduler must lock the shard before calling any safe wrapper methods.
func (h *Heap[T, ID]) Lock() {
	h.mu.Lock()
}

// Unlock releases the exclusive mutex for this Heap Shard.
func (h *Heap[T, ID]) Unlock() {
	h.mu.Unlock()
}

// Peek returns the highest-priority node without removing it, or nil if the heap is empty.
func (h *Heap[T, ID]) Peek() *node.HeapNode[T, ID] {
	if len(h.data.nodes) == 0 {
		return nil
	}
	return h.data.nodes[0]
}

// Push is a type-safe wrapper that adds a node to the heap and restores ordering.
func (h *Heap[T, ID]) Push(n *node.HeapNode[T, ID]) {
	containerheap.Push(&h.data, n)
}

// Pop is a type-safe wrapper that removes and returns the highest-priority node.
// It returns nil if the heap is empty.
func (h *Heap[T, ID]) Pop() *node.HeapNode[T, ID] {
	if len(h.data.nodes) == 0 {
		return nil
	}
	return containerheap.Pop(&h.data).(*node.HeapNode[T, ID])
}

// Fix is a type-safe wrapper that restores heap ordering after the node at the given index has changed.
// It panics if the index is out of bounds, as invalid indexes indicate an internal scheduler error.
func (h *Heap[T, ID]) Fix(index int) {
	if index < 0 || index >= len(h.data.nodes) {
		panic("heap: Fix called with invalid index")
	}
	containerheap.Fix(&h.data, index)
}

// Remove is a type-safe wrapper that removes the node at the given index.
// It panics if the index is out of bounds, as invalid indexes indicate an internal scheduler error.
func (h *Heap[T, ID]) Remove(index int) *node.HeapNode[T, ID] {
	if index < 0 || index >= len(h.data.nodes) {
		panic("heap: Remove called with invalid index")
	}
	return containerheap.Remove(&h.data, index).(*node.HeapNode[T, ID])
}
