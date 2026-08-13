package scheduler_test

import (
	"strconv"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

type benchRes struct {
	ID       string
	Priority int
}

func benchComparator(a, b *benchRes) int {
	if a.Priority < b.Priority {
		return -1
	}
	if a.Priority > b.Priority {
		return 1
	}
	return 0
}
func benchKeyFunc(r *benchRes) string { return r.ID }

func newBenchScheduler(b *testing.B, heapCount int, policy config.AcquirePolicy) *scheduler.Scheduler[*benchRes, string] {
	b.Helper()
	cfg := config.Config[*benchRes, string]{
		HeapCount:     heapCount,
		Comparator:    benchComparator,
		KeyFunc:       benchKeyFunc,
		AcquirePolicy: policy,
	}
	s, err := scheduler.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkAdd(b *testing.B) {
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			s := newBenchScheduler(b, hc, config.Shared)
			defer s.Shutdown()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Add(&benchRes{ID: strconv.Itoa(i), Priority: i})
			}
		})
	}
}

func BenchmarkBatchAdd(b *testing.B) {
	const batchSize = 1000
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := newBenchScheduler(b, hc, config.Shared)
				batch := make([]*benchRes, batchSize)
				for j := range batch {
					batch[j] = &benchRes{ID: strconv.Itoa(i*batchSize + j), Priority: j}
				}
				b.StartTimer()
				_ = s.BatchAdd(batch)
				b.StopTimer()
				s.Shutdown()
			}
		})
	}
}

func BenchmarkAcquireShared(b *testing.B) {
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			s := newBenchScheduler(b, hc, config.Shared)
			defer s.Shutdown()
			for i := 0; i < 10000; i++ {
				_ = s.Add(&benchRes{ID: strconv.Itoa(i), Priority: i})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = s.Acquire()
			}
		})
	}
}

func BenchmarkAcquireExclusiveRelease(b *testing.B) {
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			s := newBenchScheduler(b, hc, config.Exclusive)
			defer s.Shutdown()
			for i := 0; i < 10000; i++ {
				_ = s.Add(&benchRes{ID: strconv.Itoa(i), Priority: i})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := s.Acquire()
				if err == nil {
					_ = s.Release(res.ID)
				}
			}
		})
	}
}

func BenchmarkUpdate(b *testing.B) {
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			s := newBenchScheduler(b, hc, config.Shared)
			defer s.Shutdown()
			for i := 0; i < 10000; i++ {
				_ = s.Add(&benchRes{ID: strconv.Itoa(i), Priority: i})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Update(&benchRes{ID: strconv.Itoa(i % 10000), Priority: i})
			}
		})
	}
}

func BenchmarkAcquireSharedParallel(b *testing.B) {
	for _, hc := range []int{1, 8, 32} {
		b.Run("HeapCount="+strconv.Itoa(hc), func(b *testing.B) {
			s := newBenchScheduler(b, hc, config.Shared)
			defer s.Shutdown()
			for i := 0; i < 100000; i++ {
				_ = s.Add(&benchRes{ID: strconv.Itoa(i), Priority: i})
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = s.Acquire()
				}
			})
		})
	}
}
