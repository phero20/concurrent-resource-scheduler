// Package acquire defines the AcquireStrategy interface and provides the
// built-in shard-selection strategies for the Concurrent Resource Scheduler.
//
// # Roles
//
// CRS separates two distinct concerns:
//
//   - Priority ordering within a shard (determined by [config.Comparator]).
//   - Shard selection — which shard to query first (determined by [AcquireStrategy]).
//
// This package owns the second concern. An AcquireStrategy receives a
// read-only [ShardView] of the scheduler's topology and returns the index of
// the shard to try first. The scheduler then walks remaining shards in order
// until it finds an available resource.
//
// # Built-in Strategies
//
//   - [NewRoundRobin]: cycles through shards sequentially. Good for
//     homogeneous resources with similar capacity and latency.
//   - [NewWeightedStrategy]: routes proportionally based on static capacity
//     weights. Good for heterogeneous resources.
//   - [NewAdaptiveStrategy]: uses a power-of-two random choices algorithm to
//     favor less-loaded shards. Good for dynamic workloads.
//
// # Affinity Routing
//
// [AcquireByAffinity] routes requests to a consistent shard using a
// [ConsistentHashRing] rather than the configured AcquireStrategy. Implement
// [AffinityIdentifier] on your key type to enable sticky-session routing.
//
// # Custom Strategies
//
// Implement [AcquireStrategy] to supply a custom selection algorithm.
// The implementation must be safe for concurrent use by multiple goroutines
// and must not block.
package acquire

// ShardView provides read-only access to the scheduler's shard topology.
// It is passed to [AcquireStrategy.Select] on every acquisition and allows
// strategies to make load-aware decisions without accessing scheduler internals.
//
// All methods are non-blocking and safe for concurrent use.
type ShardView interface {
	// ShardCount returns the total number of configured Heap Shards.
	// This value is fixed after scheduler construction.
	ShardCount() int

	// ActiveCount returns the number of ACTIVE resources currently held in
	// the Heap Shard at the given zero-based index. It performs an atomic
	// load and does not acquire any lock.
	//
	// If shard is out of range, ActiveCount returns 0.
	ActiveCount(shard int) int
}

// AcquireStrategy selects the initial Heap Shard for an acquisition attempt.
// The scheduler queries the selected shard first, then falls back to
// subsequent shards in order until an available resource is found.
//
// # Contract
//
// Select must return a valid zero-based shard index in the range
// [0, ShardView.ShardCount()). Returning an out-of-range index causes
// [scheduler.Scheduler.Acquire] to return [errors.ErrInvalidAcquireStrategy].
//
// # Concurrency
//
// Implementations must be safe for concurrent use by multiple goroutines and
// must not block. Select is called on every Acquire operation and is on the
// hot path.
//
// # Implementing a Custom Strategy
//
// To provide a custom selection algorithm, implement this interface and supply
// the value as [config.Config.AcquireStrategy]. The [ShardView] passed to
// Select reflects live scheduler state; use [ShardView.ActiveCount] to make
// load-aware decisions.
type AcquireStrategy interface {
	// Select receives a read-only view of the scheduler's shard topology and
	// returns the zero-based index of the Heap Shard to query first.
	Select(view ShardView) int
}

// AffinityIdentifier is implemented by application-defined key types that
// want to use [scheduler.Scheduler.AcquireByAffinity] for sticky-session
// routing.
//
// The scheduler hashes the bytes returned by AppendAffinityBytes using FNV-64a
// and maps the resulting hash to a Heap Shard via an internal
// [ConsistentHashRing]. The same identifier always maps to the same shard,
// providing session affinity without a global lock.
//
// # Contract
//
// AppendAffinityBytes must be deterministic: the same logical identifier must
// always produce the same byte sequence. It must not block and must not call
// back into the scheduler.
//
// # Concurrency
//
// AppendAffinityBytes may be called concurrently by multiple goroutines.
type AffinityIdentifier interface {
	// AppendAffinityBytes appends the identifier's canonical byte
	// representation to dst and returns the extended slice. Implementations
	// should append to the provided buffer when possible rather than
	// allocating a new slice; the scheduler supplies a 64-byte stack buffer
	// as the initial dst. The scheduler never retains the returned slice
	// beyond the duration of the call.
	AppendAffinityBytes(dst []byte) []byte
}
