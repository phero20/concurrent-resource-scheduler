// Package errors contains every exported CRS error.
// Public APIs return these shared errors, and applications
// may compare them using errors.Is().
package errors

import "errors"

var (
	// ErrInvalidHeapCount is returned by New() when the configured heap count is zero, negative, or exceeds the internal maximum.
	ErrInvalidHeapCount = errors.New("invalid heap count")

	// ErrNilComparator is returned by New() when a required Comparator function was not provided in the configuration.
	ErrNilComparator = errors.New("nil comparator")

	// ErrNilKeyFunc is returned by New() when a required KeyFunc was not provided in the configuration.
	ErrNilKeyFunc = errors.New("nil key function")

	// ErrInvalidAcquirePolicy is returned by New() when the configured AcquirePolicy is not a recognized value.
	ErrInvalidAcquirePolicy = errors.New("invalid acquire policy")

	// ErrInvalidAcquireStrategy is a runtime error returned by Acquire() indicating the configured AcquireStrategy returned an invalid Heap Shard index.
	ErrInvalidAcquireStrategy = errors.New("invalid acquire strategy")

	// ErrNilResource is returned by Add() or BatchAdd() when a nil resource is passed for registration.
	ErrNilResource = errors.New("nil resource")

	// ErrDuplicateKey is returned by Add() or BatchAdd() when an operation repeats an application key that is already registered.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrResourceNotFound is returned when the scheduler cannot find a resource with the specified key.
	ErrResourceNotFound = errors.New("resource not found")

	// ErrNotExclusive is returned by Release() when called on a scheduler that is configured with the Shared AcquirePolicy.
	ErrNotExclusive = errors.New("not an exclusive policy")

	// ErrResourceNotInactive is returned by Release() or Include() when the given key is found, but the corresponding resource is not in the Inactive Store.
	ErrResourceNotInactive = errors.New("resource not inactive")

	// ErrResourceNotActive is returned by Exclude() when the given key is found, but the corresponding resource is not currently in an active Heap Shard.
	ErrResourceNotActive = errors.New("resource not active")

	// ErrNoResourceAvailable is returned by Acquire() when no active resource is available after all Heap Shards have been inspected.
	ErrNoResourceAvailable = errors.New("no resource available")

	// ErrSchedulerClosed is returned by operations (except Stats) after Shutdown() has been called.
	ErrSchedulerClosed = errors.New("scheduler closed")

	// ErrNilAffinityIdentifier is returned by AcquireByAffinity() when a nil identifier is provided.
	ErrNilAffinityIdentifier = errors.New("nil affinity identifier")
)
