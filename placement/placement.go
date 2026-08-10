package placement

// ShardView abstracts read-only access to scheduler topology.
//
// Behavior:
// Exposes the number of shards and atomic active resource counts, allowing
// strategies to make load-aware decisions without mutating scheduler internals.
type ShardView interface {
	// ShardCount returns the number of configured Heap Shards.
	ShardCount() int
	// ActiveCount returns the number of ACTIVE resources currently contained in the specified Heap Shard.
	ActiveCount(shard int) int
}

// AcquireStrategy abstracts the target selection algorithm for Acquire operations.
//
// Behavior:
// The strategy decides *where* the scheduler begins its search for an available resource.
//
// Concurrency Guarantees:
// Implementations MUST be strictly thread-safe and non-blocking.
type AcquireStrategy interface {
	// Select receives a read-only shard view and returns a zero-based Heap Shard index
	// for the scheduler to query.
	Select(view ShardView) int
}

// AffinityIdentifier is implemented by application keys requesting Sticky Sessions.
//
// Behavior:
// Exposes bytes for internal hashing to deterministically route requests to
// consistent shards via the internal Consistent Hash Ring.
type AffinityIdentifier interface {
	// AppendAffinityBytes appends the identifier's raw byte representation to dst
	// and returns the updated slice. It must be deterministic.
	// The scheduler provides a small stack buffer. Implementations should append
	// to it when possible, but may allocate if their identifier exceeds the
	// provided capacity. The scheduler never retains the returned slice.
	AppendAffinityBytes(dst []byte) []byte
}
