package main

import (
	"fmt"
	"log"
	"time"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/extensions/cooldown"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type Worker struct {
	ID string
}

// controller wrapper breaks the initialization cycle for the Cooldown Manager
type controller struct {
	s *scheduler.Scheduler[*Worker, string]
}

func (c *controller) Exclude(id string) error {
	return c.s.Exclude(id)
}

func (c *controller) Include(id string) error {
	return c.s.Include(id)
}

func main() {
	compare := func(a, b *Worker) int { return 0 }
	keyFunc := func(w *Worker) string { return w.ID }

	ctrl := &controller{}

	// 1. Create the Cooldown Manager extension (e.g., 50ms cooldown).
	cdManager := cooldown.NewManager[string](
		ctrl,
		50*time.Millisecond,
	)

	// 2. Register the observer in the config.
	cfg := config.Config[*Worker, string]{
		HeapCount:     1,
		Comparator:    compare,
		KeyFunc:       keyFunc,
		AcquirePolicy: config.Exclusive,
		Observers:     []events.Observer[string]{cdManager},
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer sched.Shutdown()
	ctrl.s = sched // Wire the controller

	sched.Add(&Worker{ID: "worker-1"})

	// 3. Acquire the worker (Exclusive).
	res, _ := sched.Acquire()
	fmt.Printf("Acquired: %s\n", res.ID)

	// 4. Release the worker.
	// Normally, it would be instantly available.
	// However, the Cooldown Manager observer intercepts the Release event,
	// excludes the resource immediately, and sets a 50ms timer to include it later.
	sched.Release(res.ID)
	fmt.Println("Released worker-1. It is now cooling down...")

	// 5. Try to acquire immediately (will fail).
	_, err = sched.Acquire()
	fmt.Printf("Immediate acquire error: %v\n", err)

	// 6. Wait for cooldown.
	time.Sleep(60 * time.Millisecond)

	// 7. Try again (will succeed).
	res2, _ := sched.Acquire()
	fmt.Printf("Acquired after cooldown: %s\n", res2.ID)
}
