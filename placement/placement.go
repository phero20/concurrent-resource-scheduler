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

// AffinityIdentifier allows any application type to provide its identity
// for deterministic routing.
type AffinityIdentifier interface {
	// AppendAffinityBytes appends the identifier's raw byte representation to dst
	// and returns the updated slice. It must be deterministic.
	// The scheduler provides a small stack buffer. Implementations should append
	// to it when possible, but may allocate if their identifier exceeds the 
	// provided capacity. The scheduler never retains the returned slice.
	AppendAffinityBytes(dst []byte) []byte
}
