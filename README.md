# Concurrent Resource Scheduler (CRS)

[![Go Reference](https://pkg.go.dev/badge/github.com/feroz/concurrent-resource-scheduler.svg)](https://pkg.go.dev/github.com/feroz/concurrent-resource-scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/feroz/concurrent-resource-scheduler)](https://goreportcard.com/report/github.com/feroz/concurrent-resource-scheduler)

CRS is a high-performance, domain-agnostic, concurrent Go library for selecting and maintaining prioritized reusable resources under heavy load.

A resource can represent an API key, proxy, worker, GPU, database replica, service instance, or any other reusable work provider. The embedding application owns the resource type, its business state, and its priority policy. CRS owns thread-safe scheduling, lock-free telemetry, and priority-queue maintenance.

## Features

- **Blazing Fast Concurrency**: No global heap lock. Sharded heaps ensure microscopic lock contention, achieving massive throughput even under heavy concurrent loads.
- **Pluggable Placement Strategies**: Includes `RoundRobin`, `Adaptive`, `Weighted`, and `ConsistentHashing` strategies out of the box.
- **Event-Driven Extensions**: A non-blocking telemetry and events system enables custom cooldown managers, metrics aggregators, and Prometheus integrations without polluting the core hot path.
- **Lock-free Internal Hashing**: Deterministic routing for sticky sessions (`AcquireByAffinity`) via virtualized Consistent Hash Rings.
- **100% Statement Coverage**: Built with unwavering production-grade rigor. 0 data races. 

---

## Installation

```sh
go get github.com/feroz/concurrent-resource-scheduler
```

---

## Quick Start

```go
package main

import (
	"log"
	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type Worker struct {
	ID       string
	Priority int
}

func main() {
	// 1. Define how priority is calculated (smaller is higher priority)
	compare := func(a, b *Worker) int {
		if a.Priority < b.Priority { return -1 }
		if a.Priority > b.Priority { return 1 }
		return 0
	}

	// 2. Define how to extract a unique ID
	keyFunc := func(w *Worker) string { return w.ID }

	// 3. Configure the scheduler
	cfg := config.Config[*Worker, string]{
		HeapCount:     4,                    // 4 independent shards for high concurrency
		Comparator:    compare,
		KeyFunc:       keyFunc,
		AcquirePolicy: config.Exclusive,     // Exclusively acquire resources
	}

	// 4. Create the scheduler
	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
	}
	defer sched.Shutdown()

	// 5. Add resources
	sched.Add(&Worker{ID: "worker-1", Priority: 1})
	sched.Add(&Worker{ID: "worker-2", Priority: 2})

	// 6. Acquire the best resource
	worker, err := sched.Acquire()
	if err != nil {
		log.Fatalf("failed to acquire: %v", err)
	}

	log.Printf("Acquired worker: %s", worker.ID)

	// 7. Release it back to the pool
	sched.Release(worker.ID)
}
```

---

## Documentation Directory

To maintain a pristine and scalable codebase, this repository heavily relies on structured documentation. Please read these before contributing or utilizing the library:

- 📖 **[OVERVIEW.md](OVERVIEW.md)** — High-level executive summary of the architecture and guarantees.
- 🏗️ **[ARCHITECTURE.md](ARCHITECTURE.md)** — The definitive guide to the system (internal data structures, flow charts, locking mechanisms, complexity targets).
- 📜 **[API.md](API.md)** — Authoritative reference for the public contract, acquire policies, and error taxonomy.
- 🗺️ **[ROADMAP.md](ROADMAP.md)** — Historical log of the execution phases through v1.0.0.
- 🤝 **[CONTRIBUTING.md](CONTRIBUTING.md)** — Contribution standards, testing mandates, and code review expectations.

---

## Thread Safety & Performance

CRS is designed for **extreme concurrent environments**.

- **Per-shard Locking**: There is no global mutex. Each heap shard is independently locked.
- **O(1) Lookups**: Internal `Lookup Map` guarantees constant time operations for updates and releases.
- **O(log N) Mutations**: Heap operations (`Add`, `Update`, `Remove`) remain highly efficient.
- **Lock-Free Observability**: The Phase 6/7 Event Dispatcher runs entirely asynchronously. Observers (like Cooldown Managers or Prometheus Collectors) process events on a background goroutine, ensuring `Acquire` latencies remain completely untouched.

---

## Status

**v1.0.0 Stable**
CRS is a complete, production-ready v1 library. All core capabilities (Configuration, Heap Shards, Lookup Map, Acquire Strategy, Scheduler Orchestration, Lifecycle, Hooks, and Telemetry) have been implemented, strictly validated through race detection, and rigorously stress-tested to 100% test coverage.

## License
MIT License
