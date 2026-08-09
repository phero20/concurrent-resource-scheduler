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

	// 1. Configure the scheduler with Exclusive policy.
	// Exclusive means once a resource is acquired, it is hidden from
	// the scheduler until it is explicitly released.
	cfg := config.Config[*Item, string]{
		HeapCount:     1,
		Comparator:    compare,
		KeyFunc:       keyFunc,
		AcquirePolicy: config.Exclusive,
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer sched.Shutdown()

	sched.Add(&Item{ID: "item-1", Priority: 1})
	sched.Add(&Item{ID: "item-2", Priority: 2})

	// 2. Acquire the first resource (item-1). It is now hidden.
	res1, _ := sched.Acquire()
	fmt.Printf("First acquire: %s\n", res1.ID)

	// 3. Acquire again. Since item-1 is exclusive, we get item-2.
	res2, _ := sched.Acquire()
	fmt.Printf("Second acquire: %s\n", res2.ID)

	// 4. Release item-1 back to the pool.
	sched.Release(res1.ID)
	fmt.Printf("Released %s\n", res1.ID)

	// 5. Acquire again. item-1 is back and has the best priority.
	res3, _ := sched.Acquire()
	fmt.Printf("Third acquire: %s\n", res3.ID)
}
