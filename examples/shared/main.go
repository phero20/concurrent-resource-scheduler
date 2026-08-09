package main

import (
	"fmt"
	"log"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

type Item struct {
	ID       string
	Priority int
}

func main() {
	compare := func(a, b *Item) int {
		if a.Priority < b.Priority {
			return -1
		}
		if a.Priority > b.Priority {
			return 1
		}
		return 0
	}
	keyFunc := func(i *Item) string { return i.ID }

	// 1. Configure the scheduler with Shared policy.
	// Shared means acquired resources stay active in the heap, allowing
	// concurrent callers to acquire the same resource repeatedly.
	cfg := config.Config[*Item, string]{
		HeapCount:     1,
		Comparator:    compare,
		KeyFunc:       keyFunc,
		AcquirePolicy: config.Shared, // Default is Shared
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer sched.Shutdown()

	sched.Add(&Item{ID: "item-1", Priority: 1})

	// 2. Acquire multiple times. Under Shared policy, the same resource
	// is returned since it remains the highest priority in the heap.
	for i := 0; i < 3; i++ {
		res, _ := sched.Acquire()
		fmt.Printf("Acquired %s\n", res.ID)
	}
}
