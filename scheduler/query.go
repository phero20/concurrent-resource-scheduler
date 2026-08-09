package scheduler

import (
	"fmt"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/stats"
)

// Get retrieves a resource by its unique identifier without altering its state.
// It returns the resource value regardless of whether it is ACTIVE or INACTIVE.
//
// Concurrency Guarantees:
// Thread-safe and completely lock-free. It uses the concurrent global Lookup Map.
//
// Complexity: O(1).
func (s *Scheduler[T, ID]) Get(id ID) (T, error) {
	var zero T
	if s.closed.Load() {
		return zero, errors.ErrSchedulerClosed
	}

	n := s.registry.Get(id)
	if n == nil {
		return zero, errors.ErrResourceNotFound
	}

	shard := s.shards[n.ShardID]
	shard.Lock()
	defer shard.Unlock()

	if n.IsDeleted {
		return zero, errors.ErrResourceNotFound
	}

	return n.Value, nil
}

// Len returns the total number of resources registered in the scheduler.
// This includes both ACTIVE resources and INACTIVE resources.
//
// Concurrency Guarantees:
// Thread-safe and lock-free. It queries the concurrent global Lookup Map.
//
// Complexity: O(1).
func (s *Scheduler[T, ID]) Len() int {
	return s.registry.Len()
}

// Stats returns a point-in-time snapshot of the scheduler's internal metrics.
// It aggregates data across all Heap Shards and the Inactive Store to provide
// deep operational visibility.
//
// Concurrency Guarantees:
// Thread-safe. It acquires short-lived read locks on individual shards iteratively,
// never halting the entire scheduler or starving Acquire operations.
//
// Complexity:
// O(H) where H is the number of Heap Shards.
func (s *Scheduler[T, ID]) Stats() stats.Stats {
	var active, empty, nonEmpty int
	sizes := make([]int, len(s.shards))

	for i, shard := range s.shards {
		shard.Lock()
		count := shard.Len()
		shard.Unlock()

		sizes[i] = count
		active += count
		if count == 0 {
			empty++
		} else {
			nonEmpty++
		}
	}

	total := s.registry.Len()

	policyStr := "Shared"
	if s.cfg.AcquirePolicy == config.Exclusive {
		policyStr = "Exclusive"
	}

	var stratStr string
	if s.cfg.AcquireStrategy != nil {
		if stringer, ok := s.cfg.AcquireStrategy.(fmt.Stringer); ok {
			stratStr = stringer.String()
		} else {
			stratStr = "CustomStrategy"
		}
	}

	return stats.Stats{
		HeapCount:         len(s.shards),
		TotalResources:    total,
		ActiveResources:   active,
		InactiveResources: total - active,
		EmptyHeaps:        empty,
		NonEmptyHeaps:     nonEmpty,
		HeapSizes:         sizes,
		AcquirePolicy:     policyStr,
		AcquireStrategy:   stratStr,
	}
}
