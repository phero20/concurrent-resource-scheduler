package scheduler

import (
	"reflect"
	"sync/atomic"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/internal/heap"
	"github.com/feroz/concurrent-resource-scheduler/internal/lookup"
	"github.com/feroz/concurrent-resource-scheduler/placement"
)

// Scheduler is the opaque concurrent scheduler.
//
// Architectural Locking Invariants:
// - The Lookup package owns its own synchronization.
// - The Heap package owns its own synchronization.
// - The Scheduler never manually locks the Lookup Map.
// - The Scheduler coordinates operations without violating subsystem boundaries.
type Scheduler[T any, ID comparable] struct {
	// cfg holds the validated, immutable configuration (e.g., HeapCount, KeyFunc).
	cfg config.Config[T, ID]

	// shards are the independent priority heaps. Each shard maintains its own Mutex.
	shards []*heap.Heap[T, ID]

	// registry is the internal Lookup Map. It handles its own read/write synchronization.
	registry *lookup.Map[T, ID]

	// insertionIndex drives the internal Round Robin insertion strategy across shards.
	insertionIndex uint32

	// closed indicates if the scheduler has been shutdown.
	closed atomic.Bool

	// isNillable is computed once during construction to avoid reflection on every Add/Update call.
	isNillable bool
}

// shardView implements placement.ShardView for the scheduler.
type shardView[T any, ID comparable] struct {
	s *Scheduler[T, ID]
}

func (v shardView[T, ID]) ShardCount() int {
	return len(v.s.shards)
}

func (v shardView[T, ID]) ActiveCount(shard int) int {
	if shard < 0 || shard >= len(v.s.shards) {
		return 0
	}
	return v.s.shards[shard].Len()
}

// New validates the configuration and creates an empty ready scheduler.
func New[T any, ID comparable](cfg config.Config[T, ID]) (*Scheduler[T, ID], error) {
	validatedCfg, err := config.Validate(cfg)
	if err != nil {
		return nil, err
	}

	if validatedCfg.AcquireStrategy == nil {
		validatedCfg.AcquireStrategy = placement.NewRoundRobin()
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

	return &Scheduler[T, ID]{
		cfg:        validatedCfg,
		shards:     shards,
		registry:   lookup.New[T, ID](),
		isNillable: isNillable,
	}, nil
}
