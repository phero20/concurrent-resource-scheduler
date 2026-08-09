# AGENTS.md

## Project overview

This repository defines a Go **Concurrent Resource Scheduler (CRS)**: a high-performance, thread-safe library that selects and maintains prioritized reusable resources under heavy concurrent load.

The scheduler is domain-agnostic. A resource may be an API key, HTTP proxy, GPU worker, database replica, service instance, or another reusable work provider. The application owns the resource type, priority rules, and all domain state; CRS owns safe concurrent scheduling and priority-queue maintenance.

The core design is a set of independently locked priority heaps. When resources are added, the scheduler distributes them across heaps internally. During Acquire, Round Robin (the default Acquire Strategy) chooses which heap to query; the application-supplied comparator determines the best resource within that heap.

## Goals

- Provide efficient, concurrent priority scheduling: global priority with one Heap Shard and shard-local priority with multiple Heap Shards.
- Scale under contention through multiple heaps with one mutex per heap.
- Keep resource lookup fast through an internal O(1) lookup map.
- Keep the public API compact: `New`, `BatchAdd`, `Add`, `Acquire`, `Release`, `Update`, `Remove`, `Stats`, and `Shutdown`.
- Keep scheduler behavior reusable, predictable, observable, and straightforward to test.
- Preserve target operation costs: O(1) lookup; O(log n) add, update, remove, exclusive acquire, and release within a Heap Shard; shared acquire is a heap peek.

## Non-goals

CRS must not acquire application or provider responsibilities. Do not add:

- Provider-specific or LLM-specific logic (Gemini, OpenAI, Claude, API keys, etc.).
- Networking, HTTP servers or clients, REST APIs, Fiber, authentication, or rate limiting.
- Databases, Redis, persistence, dashboards, or metrics exporters.
- Business updates such as latency, health, load, or resource-quality calculations.
- Premature v1 features such as weighted heap selection, adaptive balancing, health-aware scheduling, cooldowns, sticky selection, consistent hashing, or pluggable policies.

## Architecture principles

- The scheduler owns scheduling only; the caller owns resource contents and business decisions.
- Accept comparator logic from the application. CRS must not embed assumptions about how priority is calculated.
- Partition resources across multiple heaps to reduce lock contention. Do not replace this with a global scheduler mutex.
- `HeapCount = 1` provides one global priority heap. With `HeapCount > 1`, CRS uses intentional sharding; resources are distributed across shards internally when added, and the configured Acquire Strategy chooses which shard `Acquire` queries. Shard-local priority is the scalability trade-off.
- The scheduler must depend only on the Acquire Strategy abstraction, never directly on Round Robin. Round Robin is the default and only built-in strategy in v1.
- Keep responsibilities independent: Comparator orders resources within a Heap Shard; Acquire Strategy chooses which Heap Shard `Acquire` queries; AcquirePolicy controls Shared versus Exclusive acquire behavior. internal Round Robin insertion strategy (not configurable) assigns newly added resources to shards.
- Use application-owned identity through `KeyFunc(T) ID`; never generate, persist, or expose scheduler IDs.
- Keep the Lookup Map internal and map application keys to internal `HeapNode` pointers. Heap/shard IDs and heap indexes are runtime-only metadata.
- Maintain only ACTIVE Heap Shards and one Inactive Store. Never define scheduler business statuses or interpret why a resource is inactive.
- Treat heap internals as scheduler-private. Callers must not receive mutable internal heap structures.
- Prefer explicit, small components over clever abstractions or hidden behavior.
- Use composition and single-purpose packages rather than inheritance-like designs or monolithic files.

## Folder structure conventions

Follow the planned package boundaries as implementation is added:

```
config/       Scheduler configuration and validation
scheduler/    Public scheduler orchestration and lifecycle
heap/         Heap Shards, internal HeapNodes, and heap-local operations
placement/    Acquire Strategy abstraction and built-in Round Robin strategy
lookup/       Internal Lookup Map from application key to HeapNode
stats/        Scheduler statistics types and collection
errors/       Typed, package-level errors
internal/     Non-exported shared implementation helpers
examples/     Small, isolated usage examples
tests/        Black-box or integration tests when separate from packages
benchmarks/   Focused performance benchmarks
```

- Keep package responsibilities narrow; do not place cross-cutting business logic in `scheduler/`.
- ALL test files must be placed inside the `tests/` directory (e.g., `tests/scheduler/`, `tests/placement/`). Never place test files next to the implementation.
- Add new packages only when they establish a real responsibility boundary, not merely to shorten a file.
- Avoid `utils`, `common`, or catch-all packages.

## Modular design rules

- Each exported type and function must have one clear responsibility.
- Keep the public surface minimal. Prefer unexported helpers and types unless callers genuinely need them.
- Configuration must remain scheduler-specific: heap count and comparator are appropriate; application settings are not.
- Define clear ownership for mutable state. Only the scheduler manipulates heap ordering, heap indexes, and lookup bookkeeping.
- Make mutations that affect priority explicit: callers supply the full replacement resource to `Update`; `Update` derives the key via `KeyFunc`, restores heap ordering with `heap.Fix` for ACTIVE resources, and replaces the stored value only for INACTIVE (Inactive Store) resources without touching any heap. Resource identity (the key) is immutable; to change a key, the caller must `Remove` the old resource and `Add` a new one. In `Exclusive` mode, `Release` accepts only the resource key and reinserts the node into the Heap Shard recorded in its internal HeapNode.
- Do not let a package reach into another package's private mutable state to bypass its API.
- Avoid global mutable state and hidden side effects.

## Coding standards

- Follow idiomatic Go and `gofmt` all Go source before committing.
- Write small, cohesive files and functions; split code by responsibility, not by arbitrary size alone.
- Prefer clear control flow and explicit invariants over compact or clever code.
- Document every exported type, function, method, and package-level variable with Go doc comments beginning with its name.
- Keep dependencies minimal, especially in the core scheduling path.
- Do not introduce an external dependency for functionality available in the standard library unless there is a compelling, documented reason.
- Do not alter generated files or public API behavior incidentally while making an internal change.

## Go best practices

- Use interfaces at boundaries where behavior is supplied or needs substitution; do not create interfaces solely to mirror one concrete type.
- Make zero-value behavior safe only when it is intentional and documented; otherwise validate construction through `New`.
- Validate configuration at construction time, including a positive heap count and a non-nil comparator.
- Use typed/sentinel errors for stable caller-visible failure cases and wrap underlying errors with `%w` when context is useful.
- Return values rather than panicking for expected invalid inputs or runtime states. Reserve panics for impossible internal invariant violations, if any.
- Keep allocation and reflection out of hot paths where practical.
- Preserve API compatibility carefully: exported names, error semantics, and concurrency guarantees are part of the contract.

## Naming conventions

- Use standard Go naming: short, meaningful package names; `CamelCase` exported identifiers; `camelCase` unexported identifiers.
- Package names are lowercase, singular where natural, and should not repeat the package name in every identifier.
- Name types and methods after their role or action: `Scheduler`, `Acquire`, `Release`, `Update`, `heapIndex`; avoid vague names such as `Manager`, `Helper`, `Data`, or `Util`.
- Boolean names should read as predicates, such as `closed`, `hasResource`, or `isValid`.
- Include units in numeric names when ambiguity is possible, such as `timeoutMillis` or `heapCount`.
- Keep acronyms consistent with Go style (`ID`, `URL`, `API`).

## Error handling guidelines

- Validate public-method arguments before changing state.
- Return clear, typed errors for expected cases such as invalid configuration, nil resources, duplicate insertion, unknown resource, empty scheduler, or shutdown state, once those semantics are defined.
- Do not silently ignore failed heap, lookup, or lifecycle mutations. Preserve state consistency before returning an error.
- Reject nil resources passed to `Add` or `BatchAdd` immediately with `ErrNilResource` before any scheduler state is read or modified.
- Error messages should identify the failed operation and useful context without leaking application-owned resource details unnecessarily.
- Do not use errors for normal scheduling outcomes if the API can represent them directly and clearly (for example, an empty result contract).
- Ensure a failed operation leaves the scheduler in a documented, internally consistent state.

## Concurrency guidelines

- Each heap has its own mutex. Never serialize all scheduler work behind one global heap mutex.
- Keep critical sections limited to heap or lookup bookkeeping. Acquire the lock, perform the minimal operation, and release it promptly.
- Never execute user callbacks, comparators with unbounded external work, logging hooks, or application code while a scheduler lock is held. The comparator is the narrow ordering exception: it must be lightweight, side-effect free, non-blocking, and must not re-enter CRS.
- Never expose a resource together with mutable scheduler internals that would allow callers to corrupt heap state.
- Avoid taking multiple heap locks. If an operation truly requires more than one, establish and document a deterministic lock order before implementing it.
- Keep lookup-map synchronization consistent with heap mutation so a resource cannot be observed in a stale heap or index.
- CRS locks do not protect caller-owned resource fields. Define a race-free, non-blocking way for the comparator to read priority state, and do not call scheduler methods while holding a caller lock that the comparator may need.
- Make shutdown and all public operations safe under concurrent calls, with clearly documented post-shutdown behavior.
- Run the race detector for all changes that touch shared state.

## Testing guidelines

- Cover heap ordering, comparator behavior, round-robin distribution, batch add, add, shared/exclusive acquire, release, exclude, include, update, remove, get, len, Lookup Map consistency, Inactive Store behavior, stats, and shutdown behavior.
- Test boundary and failure cases: invalid configuration, nil resources passed to `Add` and `BatchAdd`, empty selected shards, duplicate keys, intra-batch duplicate keys, unknown resources, shared concurrent acquisition, exclusive no-reacquisition, and repeated removal or update attempts.
- For `Remove`: test ACTIVE path (locks only owning shard, removes by index, cleans Lookup Map entry, no Comparator call), INACTIVE path (removes from Inactive Store, no shard locked, no Comparator call), `ErrResourceNotFound` for unknown key, atomicity on failure, concurrent `Remove`/`Acquire` race on the same resource, and `AcquirePolicy`-independence (remove works the same under `Shared` and `Exclusive` policy).
- Test `BatchAdd` two-phase atomicity: verify that any Phase 1 failure (nil element, intra-batch duplicate, scheduler duplicate) leaves the scheduler state unchanged and that no partial insertion occurs.
- Add concurrency tests with thousands of goroutines and mixed operations. Assert progress, integrity, and absence of deadlocks rather than relying on timing alone.
- Run `go test ./...` and `go test -race ./...` before considering concurrent behavior complete.
- Add benchmarks for shared/exclusive acquire throughput, release, exclude, include, update, removal, lock contention, varying heap counts, large resource pools, and `BatchAdd` bulk insertion. Keep benchmark setup separate from the timed path.
- Use deterministic test comparators and stable fixtures so tests demonstrate scheduler behavior rather than application policy.

## Performance considerations

- Preserve the intended complexity: heap mutations remain O(log n) and lookup remains O(1).
- Treat lock duration and allocation rate in acquire/release/update/remove paths as performance-sensitive.
- Do not scan all Heap Shards or resources in the normal acquire path unless a documented algorithm change explicitly requires it.
- Avoid network I/O, disk I/O, database calls, and application work inside scheduler operations.
- Benchmark before adopting complexity intended as an optimization; retain the simplest design that meets measured needs.
- Measure contention across several heap counts and resource distributions, since round robin and heap partitioning are central performance assumptions.

## Development phases

Implementation proceeds only in the order below. Each phase must compile, be independently testable, and satisfy its acceptance criteria before work begins on a later phase. A phase may depend only on artifacts and decisions made in earlier phases.

1. **Phase 1 ✅ Configuration subsystem** — define the public API, resource identity, comparator contract, configuration validation, and stable error taxonomy.
2. **Phase 2 ✅ Indexed heap subsystem** — implement and test only the private indexed priority heap and its comparator-based invariants.
3. **Phase 3 ✅ Lookup subsystem** — implement and test KeyFunc-derived application-key to HeapNode registration and Lookup Map synchronization.
4. **Phase 4 ✅ Scheduler orchestration** — compose the completed heap, lookup, and placement subsystems into concurrent operations. This phase encompasses the complete runtime implementation including:
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
5. **Phase 5 (Deferred) — Advanced Placement Strategies** — implement consistent hashing, weighted selection, adaptive load balancing, and a dedicated sticky selection API.
6. **Phase 6 (Deferred) — Scheduler Hooks & Extension APIs** — implement lifecycle callbacks and event notifications so applications can build custom cooldowns, circuit breakers, and health managers externally.
7. **Phase 7 (Deferred) — Observability & Metrics** — implement metrics exporters (e.g., Prometheus) to export O(H) stats snapshots.

Do not mix phase responsibilities. If a proposed feature requires a later phase, document the dependency and defer implementation.

## API evolution and compatibility

- Treat exported identifiers, documented error behavior, complexity expectations, and concurrency guarantees as public API.
- Prefer additive, backwards-compatible changes. Any breaking change requires an explicit versioning and migration decision.
- v1 avoids breaking API changes; reserve breaking changes for a future major version.
- Define ownership, lifetime, nil behavior, and concurrency behavior for every exported API before implementation.
- Do not expose internal heap indexes, locks, or lookup entries to solve an API convenience problem.

## Dependency, documentation, and security policy

- Prefer the Go standard library in the scheduler core. New dependencies require a concrete need, maintenance review, license review, and a reason they do not compromise the hot path.
- Keep `README.md`, `ARCHITECTURE.md`, `ROADMAP.md`, `CONTRIBUTING.md`, and this file consistent whenever architecture, package boundaries, phases, or public contracts change.
- Never commit secrets, real credentials, private endpoints, or production resource metadata. Examples and tests must use synthetic data.
- Report suspected security or concurrency vulnerabilities privately to maintainers rather than publishing an exploit or sensitive reproduction in an issue.

## Change and review discipline

- Start shared-state changes by writing down the affected invariants and lock ownership.
- Keep pull requests single-purpose and include tests for behavior changes; include a benchmark when a hot path, allocation pattern, or lock duration changes.
- A comparator may be called while its heap is locked during heap mutation. It must define a strict weak ordering and be deterministic, thread-safe, side-effect free, non-blocking, and unable to call back into the scheduler. Comparator panics propagate to the caller; CRS does not recover them.
- Update the relevant architecture flow and complexity documentation when an operation's behavior changes.

## Strict Implementation Rules

### Implementation Principles
The architecture is now frozen. Every implementation decision must respect the finalized API and architecture. If an implementation problem is encountered:
1. First attempt to solve it internally.
2. Do NOT modify the public API.
3. Do NOT redesign the architecture.
4. Only propose an architecture change if there is absolutely no reasonable internal solution.

### Phase-Based Development
The scheduler must be implemented strictly in phases. Never implement the entire scheduler in one step. Complete one phase before beginning the next. Every phase must be internally complete, reviewed, tested, documented, and stable before moving to the next phase.

### Layered Architecture
Implementation must follow the layered architecture already documented. Examples include Configuration Layer, Validation Layer, Core Types, Internal Data Structures, Heap Layer, Lookup Layer, Inactive Store Layer, Acquire Strategy Layer, Scheduler Core, Public APIs, and Testing Layer. Lower layers must never depend on higher layers. Dependencies should always flow from higher-level components toward lower-level reusable components.

### Modular Project Structure
The project must be highly modular. Every responsibility should have its own package/file whenever practical. Avoid large files containing unrelated functionality. Examples: `/config`, `/errors`, `/internal/heap`, `/internal/node`, `/internal/lookup`, `/internal/inactive`, `/internal/placement`, `/internal/validation`, `/internal/scheduler`, `/types`, `/interfaces`, `/utils`. Every reusable component should have a single implementation shared across the entire project. Never duplicate logic.

### Error Handling
All exported scheduler errors must be defined in one dedicated package/file. Every package should reuse those shared errors. Never redefine identical errors in multiple files.

### Shared Types
All common enums, constants, interfaces, helper types, and reusable structures should live in dedicated shared packages. Avoid duplicated type definitions.

### Small Responsibilities
Every package, file, and type should have one clear responsibility. Follow the Single Responsibility Principle. Avoid "god files" and "god structs."

### Code Style
Write production-quality Go. Prioritize readability, maintainability, simplicity, consistency, idiomatic Go, low coupling, and high cohesion. Prefer composition over unnecessary abstraction. Avoid clever implementations that reduce readability.

### Comments
Every exported type, function, interface, constant, and package must include proper GoDoc comments. Complex algorithms should include implementation comments explaining why the code exists, why the algorithm works, important concurrency guarantees, locking strategy, complexity, and important invariants. Avoid obvious comments. Write comments that help future maintainers understand the design.

### Concurrency
Concurrency-sensitive code should be clearly documented. Explain lock ownership, lock scope, why locking is required, and what invariants are protected. Avoid hidden concurrency assumptions.

### Testability
Every implementation decision should make future unit testing straightforward. Keep components loosely coupled. Prefer dependency injection where appropriate. Avoid tightly coupled implementations.

### Consistency
Naming conventions, file organization, package structure, and documentation should remain consistent across the entire project.

### Phase Transitions
Before starting any new implementation phase, review the architecture documents, review the implementation rules, verify the previous phase is complete, and ensure no public API has changed. Never begin a new phase without validating the previous one.

### Long-Term Maintainability
Assume this project will be maintained by engineers who have never seen the code before. Optimize for easy navigation, easy debugging, easy extension, easy testing, easy code review, minimal duplication, and long-term maintainability. Every implementation decision should favor clarity over short-term convenience. The final codebase should feel like a production-quality open-source Go library rather than a prototype.

## Rules for future AI agents

- Read `README.md` and this file before proposing or implementing architectural changes.
- Keep changes within the scheduler's scope. Reject or explicitly isolate provider, API, storage, authentication, and business-policy concerns.
- Do not add speculative future extensions to v1 without an explicit request and a clear compatibility/performance rationale.
- Preserve package boundaries, per-heap locking, caller-owned comparator logic, and the small public API.
- Before changing shared-state code, identify the invariants for HeapNode runtime location, heap index, Lookup Map pointer, Inactive Store membership, and lock ownership; then add or update race-tested coverage.
- Do not call user-controlled code while holding a scheduler lock.
- Prefer focused, reviewable changes. Do not refactor unrelated packages or alter source code when the task only asks for documentation.
- Update this file when a deliberate architectural decision, public API contract, or repository convention changes.
