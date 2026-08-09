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
// The Comparator must:
// - define a strict weak ordering,
// - be deterministic,
// - be thread-safe,
// - avoid blocking operations,
// - avoid scheduler re-entry,
// - remain consistent for identical inputs.
type Comparator[T any] func(a, b T) int

// KeyFunc returns the application-owned unique key used by the internal Lookup Map.
//
// The KeyFunc:
// - returns the application's unique identifier,
// - must be deterministic,
// - must never change for the lifetime of a resource,
// - is the scheduler's only source of identity.
type KeyFunc[T any, ID comparable] func(resource T) ID

// AcquirePolicy selects immutable acquire behavior for a scheduler.
// The zero value defaults to Shared.
type AcquirePolicy uint8

const (
	// Shared returns a resource while leaving it ACTIVE in its Heap Shard.
	Shared AcquirePolicy = iota
	// Exclusive removes a resource from its Heap Shard, places it INACTIVE in the
	// Inactive Store, and requires Release before it can be acquired again.
	Exclusive
)

// Config supplies the complete scheduler configuration.
// Config becomes immutable after scheduler.New() successfully returns.
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

// Validate applies defaults and validates the configuration according to Phase 1 rules.
// It performs these steps in order:
// 1. Apply all default values.
// 2. Validate every configuration field.
// 3. Return the normalized Config.
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
