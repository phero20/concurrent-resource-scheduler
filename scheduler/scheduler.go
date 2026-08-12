// Package scheduler provides the Scheduler type and all operations for the
// Concurrent Resource Scheduler.
//
// # Overview
//
// A [Scheduler] safely manages a pool of generic resources under heavy
// concurrent load. It uses independently-locked Heap Shards for priority
// ordering, an O(1) lookup map for keyed operations, and a configurable
// [acquire.AcquireStrategy] for shard selection.
//
// # Lifecycle
//
// Create a scheduler with [New], add resources with [Scheduler.Add] or
// [Scheduler.BatchAdd], acquire them with [Scheduler.Acquire] or
// [Scheduler.AcquireByAffinity], and terminate with [Scheduler.Shutdown].
//
//	sched, err := scheduler.New(cfg)
//	if err != nil { log.Fatal(err) }
//	defer sched.Shutdown()
//
//	sched.Add(myResource)
//	res, err := sched.Acquire()
//
// # Concurrency
//
// All exported methods on [Scheduler] are safe for concurrent use by multiple
// goroutines. The scheduler avoids a global mutex by using per-shard locks and
// atomic primitives on the hot path.
//
// # Resource States
//
// Every resource is in exactly one of two states at any moment:
//
//   - ACTIVE:   present in a Heap Shard; eligible for Acquire.
//   - INACTIVE: held in the Inactive Store; invisible to Acquire.
//
// State transitions are atomic relative to the owning shard's lock.
package scheduler

import (
	"reflect"
	"sync/atomic"

	"github.com/phero20/concurrent-resource-scheduler/acquire"
	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/events"
	"github.com/phero20/concurrent-resource-scheduler/internal/heap"
	"github.com/phero20/concurrent-resource-scheduler/internal/lookup"
)

// Scheduler is a highly concurrent, lock-sharded priority queue for generic
// resources of type T identified by keys of type ID.
//
// It orchestrates resource acquisition across multiple internal Heap Shards
// using a configurable [acquire.AcquireStrategy], enforces thread-safe
// mutations, performs atomic ACTIVE/INACTIVE state transitions, and dispatches
// lifecycle events to registered observers without blocking acquisition.
//
// # Concurrency
//
// All exported methods are safe for concurrent use by multiple goroutines.
// The scheduler uses fine-grained per-shard locking and an O(1) concurrent-safe
// lookup map rather than a global mutex.
//
// # Lifecycle
//
// A Scheduler must be created via [New]. It must eventually be stopped via
// [Scheduler.Shutdown] to release the background event-dispatcher goroutine
// (started only when [config.Config.Observers] is non-empty).
//
// The zero value of Scheduler is not valid for use; always use [New].
type Scheduler[T any, ID comparable] struct {
	// cfg holds the validated, immutable configuration (e.g., HeapCount, KeyFunc).
	cfg config.Config[T, ID]

	// shards are the independent priority heaps. Each shard maintains its own Mutex.
	shards []*heap.Heap[T, ID]

	// registry is the internal Lookup Map. It handles its own read/write synchronization.
	registry *lookup.Map[T, ID]

	// affinityRing maps affinity identifiers to shards deterministically.
	affinityRing *acquire.ConsistentHashRing

	// insertionIndex drives the internal Round Robin insertion strategy across shards.
	insertionIndex uint32

	// closed indicates if the scheduler has been shutdown.
	closed atomic.Bool

	// isNillable is computed once during construction to avoid reflection on every Add/Update call.
	isNillable bool

	// eventStream is the bounded non-blocking channel for asynchronous events.
	eventStream chan events.Event[ID]

	// stopDispatcher signals the background dispatcher to shut down.
	stopDispatcher chan struct{}
}

// shardView implements acquire.ShardView for the scheduler.
type shardView[T any, ID comparable] struct {
	s *Scheduler[T, ID]
}

// ShardCount returns the total number of initialized Heap Shards.
// It satisfies the acquire.ShardView interface for acquire strategies.
//
// Concurrency Guarantees:
// This method is non-blocking and entirely thread-safe.
//
// Complexity: O(1).
func (v shardView[T, ID]) ShardCount() int {
	return len(v.s.shards)
}

// ActiveCount returns the number of active resources in the specified Heap Shard.
// It satisfies the acquire.ShardView interface for acquire strategies.
//
// Concurrency Guarantees:
// This method performs a non-blocking atomic load of the target shard's active count, avoiding lock contention.
//
// Complexity: O(1).
func (v shardView[T, ID]) ActiveCount(shard int) int {
	if shard < 0 || shard >= len(v.s.shards) {
		return 0
	}
	return v.s.shards[shard].Len()
}

// New creates, validates, and initializes a Scheduler.
//
// New validates the supplied [config.Config], applies defaults, allocates
// Heap Shards, and starts a background event-dispatcher goroutine if
// [config.Config.Observers] is non-empty.
//
// # Defaults Applied
//
//   - HeapCount 0 → [config.DefaultHeapCount] (1).
//   - AcquireStrategy nil → [acquire.NewRoundRobin].
//   - AcquirePolicy zero value → [config.Shared].
//
// # Errors Returned
//
//   - [errors.ErrInvalidHeapCount]      — invalid HeapCount.
//   - [errors.ErrNilComparator]         — Comparator is nil.
//   - [errors.ErrNilKeyFunc]            — KeyFunc is nil.
//   - [errors.ErrInvalidAcquirePolicy]  — unrecognized AcquirePolicy.
//
// All errors are stable sentinel values testable with [errors.Is].
//
// # Lifecycle
//
// The returned Scheduler is immediately ready for use. Call
// [Scheduler.Shutdown] when the scheduler is no longer needed to release the
// background goroutine and mark the scheduler closed. Failing to call Shutdown
// leaks the goroutine when Observers are registered.
//
// # Concurrency
//
// The returned *Scheduler is safe for concurrent use by multiple goroutines
// immediately after New returns.
//
// Complexity: O(H) where H is the number of Heap Shards.
func New[T any, ID comparable](cfg config.Config[T, ID]) (*Scheduler[T, ID], error) {
	validatedCfg, err := config.Validate(cfg)
	if err != nil {
		return nil, err
	}

	if validatedCfg.AcquireStrategy == nil {
		validatedCfg.AcquireStrategy = acquire.NewRoundRobin()
	}

	shards := make([]*heap.Heap[T, ID], validatedCfg.HeapCount)
	for i := 0; i < validatedCfg.HeapCount; i++ {
		shards[i] = heap.New[T, ID](validatedCfg.Comparator)
	}

	// Determine if the generic type T can ever be nil.
	// For interface types (like `any`), reflect.TypeOf(zero) returns nil.
	// For other types, we check the Kind against nillable Go types.
	isNillable := false
	var zero T
	if reflect.TypeOf(zero) != nil {
		switch reflect.TypeOf(zero).Kind() {
		case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
			isNillable = true
		}
	} else {
		// T is interface{}
		isNillable = true
	}

	s := &Scheduler[T, ID]{
		cfg:            validatedCfg,
		shards:         shards,
		registry:       lookup.New[T, ID](),
		affinityRing:   acquire.NewConsistentHashRing(validatedCfg.HeapCount),
		isNillable:     isNillable,
		eventStream:    make(chan events.Event[ID], 4096),
		stopDispatcher: make(chan struct{}),
	}

	if len(validatedCfg.Observers) > 0 {
		go s.dispatchLoop()
	}

	return s, nil
}
