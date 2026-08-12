// Package config defines the configuration types, constants, and validation
// logic for the Concurrent Resource Scheduler.
//
// # Overview
//
// A [Config] value is the single required argument to [scheduler.New]. It
// carries the resource type, identity function, priority comparator, and all
// optional scheduling policy fields. Config values are strictly validated
// during scheduler construction; after the scheduler is created, the
// configuration is immutable.
//
// # Minimal Configuration
//
//	cfg := config.Config[*MyResource, string]{
//	    Comparator: myComparator, // required
//	    KeyFunc:    myKeyFunc,    // required
//	}
//
// HeapCount defaults to [DefaultHeapCount] (1) when zero. AcquirePolicy
// defaults to [Shared]. AcquireStrategy defaults to RoundRobin when nil.
package config

import (
	"github.com/phero20/concurrent-resource-scheduler/acquire"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/events"
)

const (
	// DefaultHeapCount is the number of Heap Shards used when HeapCount is
	// zero in a [Config]. A single shard provides global priority ordering
	// with no sharding overhead. Increase HeapCount to reduce lock contention
	// under high concurrency.
	DefaultHeapCount = 1

	// maxHeapCount is the internal upper bound on Heap Shards to prevent
	// excessive memory allocation. It is not exported because callers should
	// not need to reference it; scheduler.New returns [errors.ErrInvalidHeapCount]
	// if this limit is exceeded.
	maxHeapCount = 1024
)

// Comparator defines the priority ordering for resources within a Heap Shard.
// It must return a negative integer when a should be ranked ahead of b, zero
// when order is unspecified, and a positive integer when b should be ranked
// ahead of a.
//
// # Contract
//
// The comparator must implement a strict weak ordering: it must be
// deterministic, transitively consistent, and free of side effects.
//
// # Concurrency
//
// Comparator is invoked while the scheduler holds an internal shard lock.
// It MUST be:
//
//   - Lightweight and non-blocking.
//   - Safe for concurrent use by multiple goroutines.
//   - Free of calls back into the scheduler (doing so causes a deadlock).
//
// Panics inside a Comparator propagate to the caller of the triggering
// scheduler operation; CRS does not recover them.
type Comparator[T any] func(a, b T) int

// KeyFunc extracts the unique, immutable identity key from a resource.
// The returned key is used for O(1) lookup, duplicate detection, and all
// keyed operations (Get, Update, Remove, Release, Exclude, Include).
//
// # Contract
//
// KeyFunc must be pure and deterministic: the same resource must always
// return the same key. The key must uniquely identify the resource within
// the scheduler — two distinct resources must not share a key.
//
// Resource identity is immutable. To change a resource's key, remove the old
// resource with [scheduler.Scheduler.Remove] and add a new one with
// [scheduler.Scheduler.Add].
//
// # Concurrency
//
// KeyFunc is called frequently in the hot path. It MUST be non-blocking and
// safe for concurrent use by multiple goroutines.
type KeyFunc[T any, ID comparable] func(resource T) ID

// AcquirePolicy determines whether an acquired resource remains visible to
// concurrent acquirers or is exclusively held by the caller until released.
//
// The zero value is [Shared].
type AcquirePolicy uint8

const (
	// Shared leaves the resource ACTIVE in its Heap Shard after acquisition.
	// Multiple goroutines may acquire the same resource concurrently.
	// Use Shared for stateless resources such as API keys, read replicas, or
	// HTTP endpoints where concurrent use is safe.
	Shared AcquirePolicy = iota

	// Exclusive removes the resource from its Heap Shard on acquisition and
	// places it in the Inactive Store. The resource is unavailable to other
	// callers until [scheduler.Scheduler.Release] is called with its key.
	// Use Exclusive for resources that cannot be safely shared concurrently,
	// such as GPU workers, exclusive connections, or single-use executors.
	Exclusive
)

// Config provides the complete configuration required to instantiate a
// [scheduler.Scheduler]. All fields except Comparator and KeyFunc have
// sensible defaults applied by [Validate] during construction.
//
// Config values are consumed by [scheduler.New], which validates and freezes
// the configuration. Modifying a Config after passing it to New has no effect
// on the running scheduler.
//
// The zero value of Config is not valid; at minimum Comparator and KeyFunc
// must be set.
type Config[T any, ID comparable] struct {
	// HeapCount is the number of independent Heap Shards.
	// A value of 1 provides a single global priority heap.
	// Larger values reduce lock contention by partitioning resources across
	// independent shards, each with its own mutex.
	//
	// Valid range: 1–1024. Zero is replaced by [DefaultHeapCount] (1).
	// Negative values and values exceeding the internal maximum return
	// [errors.ErrInvalidHeapCount].
	HeapCount int

	// Comparator is the priority ordering function. It is required; a nil
	// value causes [scheduler.New] to return [errors.ErrNilComparator].
	//
	// See [Comparator] for the full contract.
	Comparator Comparator[T]

	// KeyFunc extracts the unique identity key from a resource. It is
	// required; a nil value causes [scheduler.New] to return
	// [errors.ErrNilKeyFunc].
	//
	// See [KeyFunc] for the full contract.
	KeyFunc KeyFunc[T, ID]

	// AcquirePolicy controls whether acquired resources remain active or
	// are exclusively locked until released.
	//
	// The zero value is [Shared], which allows concurrent acquisition of the
	// same resource. Set to [Exclusive] for single-owner semantics.
	AcquirePolicy AcquirePolicy

	// AcquireStrategy selects which Heap Shard to query during Acquire.
	// When nil, [scheduler.New] installs a RoundRobin strategy automatically.
	//
	// Use the built-in strategies in the [acquire] package (NewRoundRobin,
	// NewWeightedStrategy, NewAdaptiveStrategy) or supply a custom
	// implementation of [acquire.AcquireStrategy].
	AcquireStrategy acquire.AcquireStrategy

	// Observers is an optional list of event subscribers that receive
	// read-only lifecycle notifications (add, acquire, release, etc.).
	// When empty, no background dispatcher goroutine is started.
	//
	// See [events.Observer] for the non-blocking contract that all
	// implementations must satisfy.
	Observers []events.Observer[ID]
}

// Validate applies default values and enforces all configuration constraints.
// It is called automatically by [scheduler.New]; callers rarely need to invoke
// it directly.
//
// Defaults applied:
//   - HeapCount 0 → [DefaultHeapCount] (1).
//   - AcquirePolicy zero value → [Shared].
//
// Errors returned:
//   - [errors.ErrInvalidHeapCount] — HeapCount is negative, zero after
//     applying defaults, or exceeds the internal maximum (1024).
//   - [errors.ErrNilComparator]   — Comparator is nil.
//   - [errors.ErrNilKeyFunc]      — KeyFunc is nil.
//   - [errors.ErrInvalidAcquirePolicy] — AcquirePolicy is not a recognized value.
//
// All returned errors are stable sentinel values and can be tested with
// [errors.Is].
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
