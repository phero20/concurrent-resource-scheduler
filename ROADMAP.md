# CRS Roadmap to v1

This roadmap follows the architecture dependency graph, not feature grouping. Each phase implements one core subsystem, is independently buildable and testable, and depends only on prior phases. A phase does not add responsibilities owned by a later subsystem.

```mermaid
flowchart TD
    P1[Phase 1 ✅ Configuration] --> P2[Phase 2 ✅ Heap]
    P2 --> P3[Phase 3 ✅ Lookup]
    P3 --> P4[Phase 4 ✅ Scheduler Orchestration]
```

## Phase 1 ✅ Contracts and configuration subsystem

### Objectives

- Define the minimal v1 public API, `KeyFunc` application identity, comparator contract, `AcquirePolicy`, configuration model, and typed error taxonomy.

### Deliverables

- Go module and public/package documentation.
- `config` validation for `HeapCount`, `Comparator`, `KeyFunc`, and `AcquirePolicy`, following the constructor lifecycle: defaults are resolved first, then the resolved configuration is validated.
  - `HeapCount = 0` defaults to `1`; `AcquireStrategy = nil` defaults to Round Robin; zero `AcquirePolicy` defaults to `Shared`.
  - An internal maximum heap count constant guards against accidental resource exhaustion. Exceeding it returns `ErrInvalidConfig`; the scheduler never silently clamps the caller's value.
- `errors` definitions and documented nil, ownership, and invalid-input behavior.
- Deterministic contract tests.

### Acceptance criteria

- `go test ./...` passes.
- Invalid configuration and public contract boundaries have focused tests.
- Comparator constraints include strict weak ordering, purity, bounded and non-blocking execution, thread safety, no scheduler re-entry, panic propagation, and race-safe caller-owned priority state.
- Contract documentation distinguishes independent Comparator, Acquire Strategy, and AcquirePolicy responsibilities. Acquire Strategy is documented as Acquire-only; internal Round Robin insertion strategy governs shard assignment for `Add` and `BatchAdd`.
- No heap, lookup, selector, lifecycle, or scheduling coordination implementation exists.

## Phase 2 ✅ Indexed heap subsystem

### Objectives

- Implement the private priority heap that maintains comparator order and a heap-local index.

### Deliverables

- `heap` package with push, peek, fix, remove, and index-maintenance behavior.
- Unit tests for ordering, swaps, insertion, removal, fix, and invariant preservation.

### Acceptance criteria

- Every heap mutation preserves the comparator-defined heap property and index correctness.
- Push, fix, and remove are O(log n); peek is O(1).
- The package has no lookup map, round robin, scheduler facade, lifecycle, or domain policy.
- `go test ./...` passes.

## Phase 3 ✅ Lookup subsystem

### Objectives

- Implement KeyFunc-derived application-key to HeapNode membership and the architecture-defined Lookup Map synchronization protocol.

### Deliverables

- `lookup` package with application-key to `*HeapNode` membership operations.
- Tests for duplicate-key prevention, unknown-key behavior, concurrent readers/writers, and HeapNode runtime-location lifecycle.

### Acceptance criteria

- Map membership operations have expected O(1) behavior.
- The package never owns application identity generation, heap ordering, or exposed runtime metadata.
- Its API supports the documented add/update/remove coordination protocol without exposing internals to callers.
- `go test ./...` and `go test -race ./...` pass.

## Phase 4 ✅ Scheduler orchestration

### Objectives

- Compose the completed heap, lookup, and placement packages into concurrent core scheduling operations.
- Implement the generic Acquire Strategy abstraction and the default thread-safe Round Robin strategy for `Acquire`-shard selection.
- Add lifecycle operations and immutable scheduler statistics.
- Establish the repeatable verification assets required to declare the composed v1 scheduler production-ready.

### Deliverables

- Runtime initialization
- Add
- BatchAdd
- Acquire
- Release
- Update
- Remove
- Include
- Exclude
- Get
- Len
- Stats
- Shutdown
- Stress tests
- Race-test validation

### Acceptance criteria

- Normal operations hold no more than one heap mutex and never use a global heap lock.
- `Acquire` uses one global heap when `HeapCount = 1`; with multiple Heap Shards it asks AcquireStrategy for the next candidate shard, locks only that shard, and checks whether it is non-empty. If empty, it unlocks and asks AcquireStrategy for the next candidate, repeating until a non-empty shard is found or every shard has been inspected. When all shards are empty, `Acquire` returns `ErrNoResourceAvailable`.
- `Acquire` locks only the single selected shard per iteration; multiple concurrent `Acquire` calls may operate on different shards simultaneously.
- `Acquire` never modifies resource priority, never rebalances heaps, and never recalculates comparator ordering.
- `Add` and `BatchAdd` assign resources to shards through the internal Round Robin insertion strategy, not through AcquireStrategy. `Shared` leaves a resource active; `Exclusive` moves it to the Inactive Store until `Release`. `Exclude` moves an ACTIVE resource to the Inactive Store until `Include`.
- `Update` never consults AcquireStrategy and never moves a resource between heaps. It locks only the owning shard (ACTIVE path) or the Inactive Store (INACTIVE path); it never holds multiple shard locks.
- `Remove` permanently unregisters a resource from either runtime location and removes its Lookup Map entry.
- `BatchAdd` validates the entire batch before modifying any scheduler state. Phase 2 (insertion) begins only when Phase 1 (validation) passes entirely.
- Statistics collection does not expose internal structures, lists of resources, or mutate scheduler internals. It executes in O(H) where H is the number of heap shards.
- Full tests and race detector pass consistently.
- Stress workloads show no deadlocks, invariant failures, or bookkeeping corruption.

## Phase 5 — Advanced Placement Strategies

Expand the `AcquireStrategy` ecosystem beyond Round Robin to support advanced distributed systems patterns without altering the core scheduler concurrency model.

### Phase 5.1 — Sticky Selection (Affinity Routing)

#### 1. Goal
Implement a mechanism that consistently routes requests with the same session affinity key to the same Heap Shard, without polluting the core scheduler with request contexts.

#### 2. Responsibilities
- Introduce a dedicated, explicit public method `AcquireByAffinity(key placement.AffinityIdentifier)`.
- Use `placement.AffinityIdentifier` which provides an `AppendAffinityBytes(dst []byte) []byte` contract.
- Use a deterministic internal hash function (FNV-1a) to map the bytes directly to a shard index `[0, HeapCount)`.
- Bypass the general `AcquireStrategy` abstraction completely for this method.
- Make it explicit that the scheduler remains type-agnostic (any custom application struct can implement the interface). The scheduler provides a small stack buffer to avoid allocations where possible.

#### 3. Files/packages involved
- `scheduler/acquire.go`
- `scheduler/acquire_test.go`

#### 4. Architectural boundaries
- **Architectural Correction:** `context.Context`, `any`, and `string` are rejected. The scheduler is a generic primitive and must remain independent of request contexts, HTTP, or RPC concepts. Using `context.Value` or `fmt.Sprintf` on `any` is an anti-pattern that causes allocations.
- The `Acquire()` method and `AcquireStrategy` interface remain completely untouched (zero arguments). 
- `AcquireByAffinity(key)` clearly signals targeted routing intent, keeping the general placement ecosystem (`Acquire()`) clean and fully backward compatible (no changes to the `Scheduler[T, ID]` signature).
- The scheduler does not expose internal shard indices to the caller.
- The scheduler completely owns hashing and shard selection.

#### 5. Concurrency considerations
- The internal hashing of the affinity identifier must be lock-free and thread-safe.
- Reuses the existing single-shard locking and `Shared`/`Exclusive` logic inside the new affinity API.

#### 6. Required unit tests
- Verify that the same affinity key deterministically selects the same shard.
- Verify that the new API correctly respects `Shared` and `Exclusive` policies.

#### 7. Required race tests
- Highly concurrent affinity-based acquire calls across many goroutines.

#### 8. Required benchmarks
- Measure the overhead of the internal identifier hashing compared to the standard `Acquire()` strategy lookup.

#### 9. Freeze criteria
- Architecture strictly preserves the generic nature of `Acquire()` and the existing `AcquireStrategy` ecosystem remains unaware of sticky selection.

### Phase 5.2 — Consistent Hashing Strategy

#### 1. Goal
Provide mathematically deterministic resource selection based on consistent hashing to maximize cache hit rates for specific workloads.

#### 2. Responsibilities
- Implement a hash ring mapping hash values to shard indices.
- Handle varying `HeapCount` safely.

#### 3. Files/packages involved
- `placement/consistent_hash.go`
- `placement/consistent_hash_test.go`

#### 4. Architectural boundaries
- Calculates selection entirely via math on the hash input.
- Does not mutate scheduler state or inspect heap contents.

#### 5. Concurrency considerations
- The hash ring structure is immutable after initialization and strictly read-only during `Acquire`, requiring no locks.

#### 6. Required unit tests
- Distribution uniformity checks across shards.
- Deterministic output verification for specific hash inputs.

#### 7. Required race tests
- Concurrent accesses to the hash ring calculation logic.

#### 8. Required benchmarks
- Measure hash function performance and allocation rate.

#### 9. Freeze criteria
- Zero allocations in the hot path. 

### Phase 5.3 — Weighted Selection Strategy

#### 1. Goal
Allow shards to receive traffic proportional to an assigned weight capacity.

#### 2. Responsibilities
- Implement a thread-safe weighted random or weighted round-robin selection algorithm.
- Accept static weight configurations per shard at initialization.

#### 3. Files/packages involved
- `placement/weighted.go`
- `placement/weighted_test.go`

#### 4. Architectural boundaries
- Selection logic operates purely on initialization weights.
- Never inspects individual resources to calculate weights.

#### 5. Concurrency considerations
- Random Number Generators (RNG) used for selection must be safe for concurrent use or use atomic counters for weighted round-robin.

#### 6. Required unit tests
- Statistical distribution tests over millions of iterations to verify weight compliance.

#### 7. Required race tests
- High concurrency testing of the RNG or atomic state variables.

#### 8. Required benchmarks
- Impact of RNG contention or atomic increments at scale.

#### 9. Freeze criteria
- Contention on the strategy's internal state does not degrade overall `Acquire` throughput.

### Phase 5.4 — Adaptive Load Balancing Strategy

#### 1. Goal
Dynamically favor less-contended Heap Shards based on lightweight load metrics without introducing global locking.

#### 2. Responsibilities
- Implement a strategy that uses the existing `ShardView` abstraction to determine which shards are least loaded.
- Select the next candidate shard probabilistically based on inverse load.

#### 3. Files/packages involved
- `placement/adaptive.go`
- `placement/adaptive_test.go`

#### 4. Architectural boundaries
- **Architectural Correction:** `Stats()` is an O(H) operation that aggregates data across all heaps and does not belong on the O(log n) `Acquire` hot path.
- The correct boundary for lightweight load information is the `ShardView.ActiveCount(shard)` method, which is already passed to the strategy and provides O(1) shard-local state without cross-heap locking.
- Must strictly use `ShardView` or strategy-local atomic counters.

#### 5. Concurrency considerations
- Must not introduce a global scheduler lock.
- Read operations for load metadata must be entirely lock-free or strictly scoped to the `ShardView` during the `Acquire` hot path.

#### 6. Required unit tests
- Verify traffic shifts away from heavily loaded shards towards lightly loaded shards.

#### 7. Required race tests
- Concurrent `Acquire` calls verifying that `ActiveCount` reads do not cause lock inversion or contention.

#### 8. Required benchmarks
- Measure the impact of load-calculation overhead on `Acquire` latency.

#### 9. Freeze criteria
- Architecture strictly preserves the O(log n) acquire path and independent heap locking guarantees.

## Phase 6 — Scheduler Hooks & Extension APIs

### Goal
Provide extension points that allow applications to build advanced resource lifecycle policies without introducing business logic into the scheduler core.

### Responsibilities
- Define and implement lifecycle callbacks and scheduler hooks (e.g., OnAcquire, OnRelease, OnExclude).
- Implement event notifications and observer interfaces for state transitions.
- Ensure that the scheduler remains completely generic and ignorant of why a resource became inactive, what 'healthy' means, or what business policy is being enforced.
- Empower applications to build their own cooldown managers, health managers, circuit breakers, API key rotation, and service discovery integrations outside of CRS using public APIs.

### Files/packages involved
- `scheduler/hooks.go`
- `scheduler/events.go`
- `extensions/` (reference implementations provided for users)

### Architectural boundaries
- CRS remains a generic concurrent resource scheduler. It does not implement penalty boxes, cooldowns, or automatic recovery natively.
- Hooks must execute efficiently and should not perform blocking operations while holding scheduler locks.
- Observers receive read-only context or event metadata.

### Concurrency considerations
- Callbacks and hooks must be invoked in a way that respects the decoupled Lookup Map and Heap Shard lock invariants.
- Observers must not cause lock inversion or deadlocks if they subsequently call back into the scheduler API.

### Tests required
- Ensure hooks are fired accurately on all state transitions (Add, Acquire, Release, Exclude, Include).
- Concurrency tests verifying that expensive or blocking hooks do not stall the core scheduler incorrectly if executed asynchronously.

### Freeze criteria
- External packages can successfully build a working cooldown manager exclusively by using the provided hooks and public APIs.

## Phase 7 — Observability & Metrics

### Goal
Expose the scheduler's internal state to industry-standard monitoring systems to provide operators with real-time visibility into capacity and contention.

### Responsibilities
- Implement metrics exporters for systems like Prometheus or OpenTelemetry.
- Export key metrics: active/inactive resource counts, empty heap contention, and acquire latency.

### Files/packages involved
- `stats/prometheus.go`
- `stats/exporter.go`

### Architectural boundaries
- Exporters must be strictly opt-in and live in separate packages to avoid bloating the core library with third-party dependencies.
- Collection must use the existing O(H) `Stats()` snapshot mechanism to ensure metrics collection does not stall the scheduler.

### Concurrency considerations
- Metric scraping must remain lock-free where possible and never block `Acquire` or `Release` hot paths.

### Tests required
- End-to-end tests validating metric formatting and registration.
- Load tests running concurrent operations while aggressively scraping metrics.

### Freeze criteria
- Adding exporters introduces zero measurable overhead to the core scheduling path.
- Core library dependencies remain minimal.
