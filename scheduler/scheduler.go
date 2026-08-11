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

// Scheduler is a highly concurrent, lock-sharded priority queue for generic resources.
// It orchestrates resource acquisition across multiple internal Heap Shards using a
// configurable AcquireStrategy. It enforces thread-safe mutations, atomic state
// transitions between ACTIVE and INACTIVE states, and non-blocking telemetry
// dispatch.
//
// Concurrency Guarantees:
// All exported methods are completely thread-safe. The scheduler avoids global
// mutexes in favor of fine-grained per-shard locking and O(1) concurrent-safe maps.
//
// Lifecycle:
// A Scheduler is created via New() and must eventually be stopped via Shutdown()
// to release background goroutines.
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

// New creates, validates, and initializes a new Scheduler instance.
// It applies default configuration values, validates the constraints (such as
// positive heap counts and non-nil KeyFunc), allocates the internal shard array,
// and starts the background event dispatcher goroutine.
//
// Complexity:
// O(H) where H is the number of Heap Shards.
//
// Lifecycle:
// The returned Scheduler must be terminated with Shutdown() to prevent goroutine leaks.
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
