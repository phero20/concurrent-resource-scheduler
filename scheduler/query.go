package scheduler

import (
	"fmt"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/stats"
)

// Get returns the stored resource for the given key without acquiring it.
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

// Len returns the total number of registered resources in O(1) time.
// It continues to return the correct length even after Shutdown.
func (s *Scheduler[T, ID]) Len() int {
	return s.registry.Len()
}

// Stats returns an immutable point-in-time snapshot of scheduler metrics.
// It calculates the metrics by querying each Heap Shard lock sequentially.
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
