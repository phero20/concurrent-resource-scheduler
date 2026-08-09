package main

import (
	"fmt"
	"log"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/events"
	"github.com/feroz/concurrent-resource-scheduler/extensions/metrics"
	promext "github.com/feroz/concurrent-resource-scheduler/extensions/prometheus"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type Worker struct {
	ID string
}

func main() {
	compare := func(a, b *Worker) int { return 0 }
	keyFunc := func(w *Worker) string { return w.ID }

	// 1. Create a Telemetry Observer to track real-time throughput.
	telemetry := metrics.NewTelemetryObserver[string]()

	// 2. Attach the observer to the scheduler.
	cfg := config.Config[*Worker, string]{
		HeapCount:  2,
		Comparator: compare,
		KeyFunc:    keyFunc,
		Observers:  []events.Observer[string]{telemetry},
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer sched.Shutdown()

	// 3. Create the Prometheus Collector, linking both the scheduler (for O(H) stats)
	// and the telemetry observer (for O(1) atomic throughput).
	collector := promext.NewCollector(sched, telemetry)

	// 4. Register the collector with Prometheus.
	if err := prometheus.Register(collector); err != nil {
		log.Fatalf("Failed to register collector: %v", err)
	}

	// 5. Perform some operations to generate metrics.
	sched.Add(&Worker{ID: "worker-1"})
	sched.Add(&Worker{ID: "worker-2"})
	sched.Acquire()

	fmt.Println("Prometheus collector successfully registered and metrics generated.")
	fmt.Println("In a real application, you would serve /metrics via HTTP.")

	// Print a snapshot of the telemetry to show it works
	snap := telemetry.Snapshot()
	fmt.Printf("Telemetry Snapshot: Adds=%d, Acquires=%d\n", snap.AddCount, snap.AcquireCount)
}
