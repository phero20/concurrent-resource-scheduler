package scheduler_test

import (
	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/placement"
)

type Resource struct {
	ID       string
	Priority int
}

func keyFunc(r *Resource) string {
	return r.ID
}

func cmpFunc(a, b *Resource) int {
	if a.Priority > b.Priority {
		return -1 // negative ranks a ahead of b
	} else if a.Priority < b.Priority {
		return 1
	}
	return 0
}

func validConfig(heapCount int) config.Config[*Resource, string] {
	return config.Config[*Resource, string]{
		HeapCount:     heapCount,
		KeyFunc:       keyFunc,
		Comparator:    cmpFunc,
		AcquirePolicy: config.Shared,
	}
}

type stringAffinity string

func (s stringAffinity) AppendAffinityBytes(dst []byte) []byte {
	return append(dst, s...)
}

type badStrategy int

func (b badStrategy) Select(view placement.ShardView) int {
	return int(b)
}
