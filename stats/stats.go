package stats

// Stats provides an immutable, point-in-time snapshot of scheduler metrics.
// It is intended for observability and testing, not for driving application logic.
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
