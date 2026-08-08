package placement

// ShardView provides a read-only view of available Heap Shards to an Acquire Strategy.
type ShardView interface {
	// ShardCount returns the number of configured Heap Shards.
	ShardCount() int
	// ActiveCount returns the number of ACTIVE resources currently contained in the specified Heap Shard.
	ActiveCount(shard int) int
}

// AcquireStrategy decides which Heap Shard Acquire should query.
//
// An AcquireStrategy:
// - selects only the next candidate Heap Shard,
// - never inspects resources,
// - never modifies scheduler state,
// - never performs priority selection.
//
// Implementations may optionally implement fmt.Stringer to provide a custom,
// stable name for the strategy in scheduler Stats.
type AcquireStrategy interface {
	// Select receives a read-only shard view and returns a zero-based Heap Shard index
	// for the scheduler to query.
	Select(view ShardView) int
}
