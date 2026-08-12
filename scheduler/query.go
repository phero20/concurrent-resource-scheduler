package scheduler

import (
	"fmt"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/stats"
)

// Get retrieves a copy of a resource's current value by its unique key.
// It returns the value regardless of whether the resource is ACTIVE or INACTIVE.
//
// The returned value is a snapshot of the resource at the time of retrieval.
// A concurrent [Scheduler.Update] may modify the stored value after Get
// returns; callers that need a guaranteed-consistent view should account for
// this.
//
// Get returns [errors.ErrResourceNotFound] if no resource with the given key
// exists. It returns [errors.ErrSchedulerClosed] after [Scheduler.Shutdown].
//
// # Concurrency
//
// Get is safe for concurrent use by multiple goroutines.
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

// Len returns the total number of resources registered in the scheduler,
// including both ACTIVE (in Heap Shards) and INACTIVE (in the Inactive Store)
// resources.
//
// # Concurrency
//
// Len is safe for concurrent use by multiple goroutines.
//
// Complexity: O(1).
func (s *Scheduler[T, ID]) Len() int {
	return s.registry.Len()
}

// Stats returns a point-in-time snapshot of the scheduler's operational state.
//
// The snapshot includes counts of active and inactive resources, per-shard
// sizes, and string representations of the configured policy and strategy.
// Stats is the only method that succeeds after [Scheduler.Shutdown]; it does
// not check the closed flag.
//
// Because Stats takes individual shard locks sequentially, the returned
// snapshot reflects a weakly consistent view: resources may move between
// shards between lock acquisitions. Use Stats for monitoring, not for
// coordination.
//
// # Concurrency
//
// Stats is safe for concurrent use by multiple goroutines. It acquires
// short-lived locks on individual shards without halting the entire scheduler.
//
// Complexity: O(H) where H is the number of Heap Shards.
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
