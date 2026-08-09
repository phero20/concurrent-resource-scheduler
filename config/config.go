package config

import (
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/placement"
)

const (
	// DefaultHeapCount is the default number of Heap Shards when none is specified.
	DefaultHeapCount = 1

	// maxHeapCount is the internal maximum allowed Heap Shards to prevent resource exhaustion.
	maxHeapCount = 1024
)

// Comparator defines resource priority. A negative result ranks a ahead of b;
// zero gives no tie-order guarantee; a positive result ranks b ahead.
//
// Behavior & Ownership:
// The caller completely owns the logic. The scheduler uses this function
// to organize resources internally.
//
// Concurrency Guarantees:
// It MUST be deterministic, thread-safe, and non-blocking. It is invoked
// while the scheduler holds internal shard locks, so it MUST NEVER call
// back into the scheduler.
type Comparator[T any] func(a, b T) int

// KeyFunc extracts a unique identity from a generic resource.
//
// Behavior & Ownership:
// The caller completely owns resource identity. The scheduler uses this
// identity to manage the resource's lifecycle across active and inactive states.
//
// Concurrency Guarantees:
// It must be deterministic and pure. It is invoked frequently during O(1)
// lookups and MUST NOT block.
type KeyFunc[T any, ID comparable] func(resource T) ID

// AcquirePolicy determines whether an acquired resource remains active
// or becomes exclusively locked.
//
// Behavior:
// Shared leaves the resource active for concurrent acquires. Exclusive moves
// the resource to the Inactive Store until explicitly released.
type AcquirePolicy uint8

const (
	// Shared returns a resource while leaving it ACTIVE in its Heap Shard.
	Shared AcquirePolicy = iota
	// Exclusive removes a resource from its Heap Shard, places it INACTIVE in the
	// Inactive Store, and requires Release before it can be acquired again.
	Exclusive
)

// Config provides the complete configuration required to instantiate a Scheduler.
//
// Lifecycle:
// A Config is strictly validated during scheduler.New(). After the scheduler
// is created, the configuration becomes immutable and cannot be modified.
type Config[T any, ID comparable] struct {
	// HeapCount is the number of Heap Shards. 1 means one global priority heap.
	HeapCount int
	// Comparator is the non-nil priority ordering function.
	Comparator Comparator[T]
	// KeyFunc is the non-nil application-key extractor.
	KeyFunc KeyFunc[T, ID]
	// AcquirePolicy is Shared or Exclusive; immutable after construction.
	AcquirePolicy AcquirePolicy
	// AcquireStrategy is the optional acquire-shard selection policy.
	AcquireStrategy placement.AcquireStrategy
	// Observers receives read-only lifecycle events.
	Observers []events.Observer[ID]
}

// Validate applies default values and enforces Phase 1 validation rules on the Config.
//
// Behavior:
// It checks for a valid HeapCount, non-nil Comparator, non-nil KeyFunc,
// and a recognized AcquirePolicy.
//
// Complexity: O(1).
func Validate[T any, ID comparable](cfg Config[T, ID]) (Config[T, ID], error) {
	// 1. Apply all default values.
	if cfg.HeapCount == 0 {
		cfg.HeapCount = DefaultHeapCount
	}
	// cfg.AcquirePolicy zero value defaults to Shared automatically.

	// 2. Validate every configuration field.
	if cfg.HeapCount <= 0 || cfg.HeapCount > maxHeapCount {
		return cfg, errors.ErrInvalidHeapCount
	}

	if cfg.Comparator == nil {
		return cfg, errors.ErrNilComparator
	}

	if cfg.KeyFunc == nil {
		return cfg, errors.ErrNilKeyFunc
	}

	if cfg.AcquirePolicy != Shared && cfg.AcquirePolicy != Exclusive {
		return cfg, errors.ErrInvalidAcquirePolicy
	}

	// 3. Return the normalized Config.
	return cfg, nil
}
