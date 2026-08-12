// Package stats defines the Stats snapshot type returned by
// [scheduler.Scheduler.Stats].
//
// A [Stats] value is a point-in-time, read-only snapshot of the scheduler's
// internal bookkeeping. It is safe to read from multiple goroutines because it
// is a plain value type containing no pointers to live scheduler state.
//
// Obtain a snapshot by calling Stats on a running scheduler:
//
//	s := sched.Stats()
//	fmt.Println("active:", s.ActiveResources)
package stats

// Stats provides a read-only, point-in-time snapshot of scheduler state.
//
// Behavior:
// Represents the aggregated internal bookkeeping (active vs inactive resources,
// empty vs non-empty shards). It is completely immutable and decoupled from
// the hot-path telemetry system.
type Stats struct {
	// HeapCount is the configured number of Heap Shards.
	HeapCount int

	// TotalResources is the number of resources registered in the scheduler.
	TotalResources int

	// ActiveResources is the number of resources currently in active Heap Shards.
	ActiveResources int

	// InactiveResources is the number of resources currently in the Inactive Store.
	InactiveResources int

	// EmptyHeaps is the number of Heap Shards that contain exactly 0 active resources.
	EmptyHeaps int

	// NonEmptyHeaps is the number of Heap Shards that contain at least 1 active resource.
	NonEmptyHeaps int

	// AcquirePolicy is a string representation of the configured AcquirePolicy (e.g. "Shared", "Exclusive").
	AcquirePolicy string

	// AcquireStrategy is a string representation of the configured AcquireStrategy (e.g. "RoundRobin").
	AcquireStrategy string

	// HeapSizes contains the number of active resources in each Heap Shard, in order.
	HeapSizes []int
}
