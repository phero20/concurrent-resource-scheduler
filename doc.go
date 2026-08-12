// Package crs is the root package of the Concurrent Resource Scheduler (CRS)
// module. CRS is a high-performance, domain-agnostic Go library for selecting,
// prioritizing, routing, and maintaining reusable resources under heavy
// concurrent load.
//
// # What CRS Does
//
// CRS safely manages a pool of application-defined resources — API keys, HTTP
// proxies, GPU workers, database replicas, connection objects, or any other
// reusable work provider — and answers the question "which resource should
// handle this request?" efficiently and concurrently.
//
// The scheduler does not interpret what a resource means. The application
// supplies the resource type, a key function for identity, and a comparator
// for priority ordering. CRS owns safe concurrent scheduling.
//
// # Core Concepts
//
// Resources are partitioned across independently-locked priority heaps called
// Heap Shards. Each shard maintains its own min-heap ordered by the
// application-supplied comparator, and its own mutex. This allows many
// goroutines to acquire resources concurrently without a single global lock.
//
// An AcquireStrategy (in the [acquire] package) chooses which Heap Shard to
// query first. Priority ordering within the shard is handled separately by
// the comparator. The two concerns are intentionally decoupled.
//
// Resources can be in one of two states:
//
//   - ACTIVE: present in a Heap Shard and eligible for acquisition.
//   - INACTIVE: held in the Inactive Store, unavailable until restored.
//
// # Quick Start
//
// Import the [scheduler] and [config] packages, supply a comparator and key
// function, and call [scheduler.New]:
//
//	cfg := config.Config[*MyResource, string]{
//	    HeapCount:  4,
//	    Comparator: myComparator,
//	    KeyFunc:    myKeyFunc,
//	}
//	sched, err := scheduler.New(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sched.Shutdown()
//
//	res, err := sched.Acquire()
//
// # Concurrency
//
// All exported methods on [scheduler.Scheduler] are safe for concurrent use
// by multiple goroutines. The scheduler uses per-shard locking, an O(1)
// concurrent-safe lookup map, and atomic counters to avoid a global mutex on
// the hot path.
//
// # Lifecycle
//
// A scheduler is created with [scheduler.New] and must be terminated with
// [scheduler.Scheduler.Shutdown] to release the background event-dispatcher
// goroutine.
//
// # Packages
//
//   - [scheduler]: the public Scheduler type and all operations.
//   - [config]: configuration types, AcquirePolicy, and validation.
//   - [acquire]: AcquireStrategy interface and built-in strategies.
//   - [events]: event types and the Observer interface.
//   - [errors]: all sentinel errors returned by the scheduler.
//   - [stats]: the Stats snapshot type.
//   - [extensions/cooldown]: automatic post-release cooldown via Observer.
//   - [extensions/metrics]: lock-free throughput telemetry via Observer.
//   - [extensions/prometheus]: Prometheus collector bridging CRS metrics.
package crs
