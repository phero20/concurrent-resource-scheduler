// Package errors defines all sentinel errors returned by the Concurrent
// Resource Scheduler. Every exported error is a stable package-level variable
// that callers can test with [errors.Is].
//
// # Usage
//
//	res, err := sched.Acquire()
//	if errors.Is(err, crsErrors.ErrNoResourceAvailable) {
//	    // No resource is currently active; retry or wait.
//	}
//
// # Error Categories
//
// Configuration errors (returned by [scheduler.New]):
//   - [ErrInvalidHeapCount]
//   - [ErrNilComparator]
//   - [ErrNilKeyFunc]
//   - [ErrInvalidAcquirePolicy]
//
// Resource mutation errors (returned by Add, BatchAdd, Update, Remove, etc.):
//   - [ErrNilResource]
//   - [ErrDuplicateKey]
//   - [ErrResourceNotFound]
//   - [ErrResourceNotActive]
//   - [ErrResourceNotInactive]
//
// Acquisition errors:
//   - [ErrNoResourceAvailable]
//   - [ErrNilAffinityIdentifier]
//   - [ErrInvalidAcquireStrategy]
//
// Policy errors:
//   - [ErrNotExclusive]
//
// Lifecycle errors:
//   - [ErrSchedulerClosed]
package errors

import "errors"

var (
	// ErrInvalidHeapCount is returned by [scheduler.New] when HeapCount is
	// zero, negative, or exceeds the internal maximum of 1024.
	// This is a permanent configuration error.
	ErrInvalidHeapCount = errors.New("invalid heap count")

	// ErrNilComparator is returned by [scheduler.New] when Comparator is nil.
	// This is a permanent configuration error.
	ErrNilComparator = errors.New("nil comparator")

	// ErrNilKeyFunc is returned by [scheduler.New] when KeyFunc is nil.
	// This is a permanent configuration error.
	ErrNilKeyFunc = errors.New("nil key function")

	// ErrInvalidAcquirePolicy is returned by [scheduler.New] when
	// AcquirePolicy is not [config.Shared] or [config.Exclusive].
	// This is a permanent configuration error.
	ErrInvalidAcquirePolicy = errors.New("invalid acquire policy")

	// ErrInvalidAcquireStrategy is returned by [scheduler.Scheduler.Acquire]
	// when the configured AcquireStrategy returns a shard index outside the
	// valid range. This indicates a bug in a custom AcquireStrategy
	// implementation.
	ErrInvalidAcquireStrategy = errors.New("invalid acquire strategy")

	// ErrNilResource is returned by [scheduler.Scheduler.Add] or
	// [scheduler.Scheduler.BatchAdd] when a nil resource is passed.
	ErrNilResource = errors.New("nil resource")

	// ErrDuplicateKey is returned by [scheduler.Scheduler.Add] or
	// [scheduler.Scheduler.BatchAdd] when a resource's key is already
	// registered in the scheduler, or when two elements within the same
	// batch share the same key.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrResourceNotFound is returned when no resource with the specified key
	// exists in the scheduler (neither ACTIVE nor INACTIVE).
	ErrResourceNotFound = errors.New("resource not found")

	// ErrNotExclusive is returned by [scheduler.Scheduler.Release] when the
	// scheduler is configured with the [config.Shared] AcquirePolicy.
	// Release is only valid under the [config.Exclusive] policy.
	ErrNotExclusive = errors.New("not an exclusive policy")

	// ErrResourceNotInactive is returned by [scheduler.Scheduler.Release] or
	// [scheduler.Scheduler.Include] when the resource exists but is currently
	// ACTIVE (not in the Inactive Store). This can occur in a race between
	// concurrent operations; callers should treat it as a transient condition.
	ErrResourceNotInactive = errors.New("resource not inactive")

	// ErrResourceNotActive is returned by [scheduler.Scheduler.Exclude] when
	// the resource exists but is currently INACTIVE (not in any Heap Shard).
	// Exclude is not idempotent: calling it on an already-inactive resource
	// returns this error.
	ErrResourceNotActive = errors.New("resource not active")

	// ErrNoResourceAvailable is returned by [scheduler.Scheduler.Acquire] or
	// [scheduler.Scheduler.AcquireByAffinity] when all Heap Shards are empty.
	// This is a transient condition: resources may become available after a
	// concurrent [scheduler.Scheduler.Release] or [scheduler.Scheduler.Include].
	ErrNoResourceAvailable = errors.New("no resource available")

	// ErrSchedulerClosed is returned by all operations (except Stats) after
	// [scheduler.Scheduler.Shutdown] has been called. This is a permanent
	// terminal condition; the scheduler cannot be restarted.
	ErrSchedulerClosed = errors.New("scheduler closed")

	// ErrNilAffinityIdentifier is returned by
	// [scheduler.Scheduler.AcquireByAffinity] when a nil identifier is passed.
	ErrNilAffinityIdentifier = errors.New("nil affinity identifier")
)
