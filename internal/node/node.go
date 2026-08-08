package node

// HeapNode is the internal runtime metadata wrapper for a registered resource.
// A HeapNode is allocated exactly once during scheduler.Add() or BatchAdd().
// The same HeapNode pointer is shared between the Heap, the Lookup Map,
// and the Inactive Store. The scheduler never creates duplicate HeapNodes
// for the same resource. Node fields are mutated in place throughout the
// resource's lifetime to guarantee a single canonical object.
//
// Invariants:
// 1. A HeapNode has exactly one Lookup Map entry.
// 2. A HeapNode belongs to exactly one Heap Shard.
// 3. A HeapNode is either ACTIVE (in a Heap Shard) or INACTIVE (in the Inactive Store).
// 4. Key never changes after allocation.
// 5. ShardID is the permanently assigned Heap Shard for the resource and never changes after allocation.
// 6. Index is meaningful only while IsActive is true.
// 7. HeapNodes are internal implementation details and are never exposed through the public API.
type HeapNode[T any, ID comparable] struct {
	// Value stores the user-provided resource directly.
	Value T

	// Key is the cached application key used by the Lookup subsystem.
	Key ID

	// Index is the current array position in the heap.
	// It is meaningful only while IsActive is true. It is maintained by heap.Swap,
	// Push, and Pop to enable O(log n) Fix and Remove.
	Index int

	// ShardID is the permanently assigned Heap Shard for the resource and never changes
	// after allocation. It is used by Release and Include to restore the resource.
	ShardID int

	// IsActive indicates whether the node currently belongs to an ACTIVE Heap Shard
	// (true) or the INACTIVE Store (false). It does not represent application status.
	IsActive bool
}
