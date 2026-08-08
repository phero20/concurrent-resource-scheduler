# CRS v1 Public API

This is the authoritative public contract for package `scheduler`. CRS exposes application resources and policy functions; Heap Shards, `HeapNode`, heap indexes, runtime shard IDs, mutexes, the Lookup Map, and the Inactive Store are internal.

## Semantics

- `Config` contains only `HeapCount`, `Comparator`, `KeyFunc`, `AcquirePolicy`, and optional `AcquireStrategy`.
- The application owns identity. `KeyFunc(T) ID` returns a unique, stable, comparable application key. CRS never generates, persists, or exposes resource IDs. Runtime shard IDs are reconstructed at every startup and never leave the scheduler.
- `HeapCount = 1` uses one global priority heap. `HeapCount > 1` uses Heap Shards; the scheduler distributes resources internally when they are added. Acquire Strategy is consulted only during `Acquire` to select the next candidate shard. The scheduler itself determines whether that shard has available resources; an empty shard causes the scheduler to ask Acquire Strategy for the next candidate, repeating until a resource is found or all shards have been checked. A multi-shard acquire returns the selected shard's highest-priority resource, not necessarily a global best.
- Round Robin is the default and only built-in Acquire Strategy in v1. Custom strategies can be supplied through Config; future random, least-loaded, and consistent-hash strategies require no scheduler-core change.
- `AcquirePolicy` is immutable after `New`: `Shared` leaves an acquired resource active; `Exclusive` removes a resource from its Heap Shard and places it in the Inactive Store until `Release` restores it to the shard recorded in its internal `HeapNode`.
- CRS defines runtime location only: **ACTIVE** resources are in Heap Shards; **INACTIVE** resources are in the Inactive Store. The Inactive Store contains every resource that is temporarily unavailable (e.g., removed by `Exclusive` acquire or `Exclude`). It simply stores inactive resources together with the information required to restore them to their original heap. The scheduler does not need to know why a resource became inactive, nor does it maintain a "Reason" or "Status". Moving a resource between an active heap and the Inactive Store is always atomic.
- All public methods are safe for concurrent use. CRS does not synchronize caller-owned fields that `Comparator` or `KeyFunc` reads.

## Exported types

### `type Comparator[T any] func(a, b T) int`

**Purpose:** Defines resource priority. A negative result ranks `a` ahead of `b`; zero gives no tie-order guarantee; a positive result ranks `b` ahead.

**Parameters:** `a` and `b` are registered resources.

**Return values:** Signed ordering result.

**Errors:** None. A comparator panic propagates to the caller; CRS does not recover it.

**Thread safety:** CRS invokes the comparator while holding an owning Heap Shard lock. It must define a strict weak ordering and be deterministic, thread-safe, pure, fast, non-blocking, race-safe for caller-owned state, and must not re-enter the scheduler.

**Complexity:** Its cost is part of heap mutation and must be O(1) in practice.

**Usage example:**

```go
compare := scheduler.Comparator[*Worker](func(a, b *Worker) int {
    if a.Priority < b.Priority { return -1 }
    if a.Priority > b.Priority { return 1 }
    return 0
})
```

**Design rationale:** Application-owned comparison keeps CRS generic and free of provider or business scoring.

### `type KeyFunc[T any, ID comparable] func(resource T) ID`

**Purpose:** Returns the application-owned unique key used by the internal Lookup Map.

**Parameters:** A resource value.

**Return values:** Its stable comparable application key.

**Errors:** None expected. If `KeyFunc` panics, the panic propagates to the caller; CRS does not recover it.

**Thread safety:** Must be deterministic, thread-safe, pure, fast, non-blocking, and must not re-enter CRS. A key must not change while its resource is registered.

**Complexity:** O(1) in practice.

**Usage example:**

```go
key := scheduler.KeyFunc[*Worker, string](func(w *Worker) string { return w.Key })
```

**Design rationale:** A function works with existing application models without reflection, required fields, or scheduler-generated IDs.

### `type AcquirePolicy uint8`

**Purpose:** Selects immutable acquire behavior for a scheduler.

**Parameters:** Set through `Config.AcquirePolicy`.

**Return values:** None.

**Errors:** `New` returns `ErrInvalidHeapCount (or other specific validation error)` for an unsupported value.

**Thread safety:** Immutable after construction and safe to read concurrently through the scheduler's behavior.

**Complexity:** O(1).

**Usage example:**

```go
cfg.AcquirePolicy = scheduler.Exclusive
```

**Design rationale:** A closed enum makes shared versus exclusive scheduling explicit without business statuses or mutable mode changes.

### `type ShardView interface`

**Purpose:** Provides a read-only view of available Heap Shards to a Acquire Strategy.

**Parameters:** Implemented internally and supplied only to Acquire Strategy calls.

**Return values:** `ShardCount() int` returns the number of configured Heap Shards; `ActiveCount(shard int) int` returns a shard's ACTIVE resource count.

**Errors:** None. A strategy must return an index in `[0, ShardCount())`.

**Thread safety:** The view is valid only for the strategy call and is safe to read during that call. It exposes neither HeapNodes, locks, indexes, nor mutable Heap Shard state.

**Complexity:** `ShardCount` is O(1); a strategy determines the cost of any `ActiveCount` calls.

**Usage example:**

```go
count := shards.ShardCount()
```

**Design rationale:** A narrow view permits future least-loaded strategies without leaking scheduler internals or coupling the scheduler to a policy. Because Acquire Strategy is only involved in `Acquire`, `ActiveCount` gives strategies enough information to make load-aware decisions.

### `type AcquireStrategy[ID comparable] interface`

**Purpose:** Decides which Heap Shard `Acquire` should query. Acquire Strategy is invoked only during acquire; it is never consulted when resources are added or removed.

**Parameters:** `Acquire(shards ShardView) int` receives a read-only shard view and returns a zero-based Heap Shard index for the scheduler to query.

**Return values:** A valid zero-based Heap Shard index.

**Errors:** None. An invalid index causes `Acquire` to fail with `ErrInvalidAcquireStrategy`.

**Thread safety:** `Acquire` may be called concurrently and must be thread-safe, deterministic for any semantics the strategy promises, non-blocking, and unable to re-enter the scheduler.

**Complexity:** Strategy-defined. The v1 default Round Robin implementation is O(1).

**Usage example:**

```go
type myPlacement struct{}
func (myPlacement) Acquire(shards scheduler.ShardView) int { return 0 }
```

**Design rationale:** Scheduler core asks this abstraction only for the next candidate shard to inspect during `Acquire`. The strategy does not know whether a shard is empty, inspect heap contents, evaluate resource priority, or observe any other scheduler state. It only selects the next candidate. The scheduler is responsible for determining whether the returned shard is usable. Shard assignment for `Add` and `BatchAdd` is handled by the scheduler's internal balanced distribution and is not part of this interface. The scheduler has no dependency on Round Robin and can support custom or future built-in policies.

### `const Shared AcquirePolicy` and `const Exclusive AcquirePolicy`

**Purpose:** `Shared` returns a resource while leaving it ACTIVE in its Heap Shard. `Exclusive` removes a resource from its Heap Shard, places it INACTIVE in the Inactive Store, and requires `Release` before it can be acquired again.

**Parameters:** None.

**Return values:** Constants used in `Config`.

**Errors:** None.

**Thread safety:** Immutable constants.

**Complexity:** `Shared` acquire is a heap peek; `Exclusive` acquire is a heap removal and release is a heap insertion.

**Usage example:**

```go
cfg.AcquirePolicy = scheduler.Shared
```

**Design rationale:** These are scheduler mechanics, not application business status.

### `type Config[T any, ID comparable] struct`

**Purpose:** Supplies the complete immutable scheduler configuration.

**Parameters:**

| Field | Meaning | Default |
| --- | --- | --- |
| `HeapCount int` | Number of Heap Shards. `1` means one global priority heap; greater values enable sharding. | `0` is defaulted to `1` by `New`. |
| `Comparator Comparator[T]` | Non-nil priority ordering function. | Required. `nil` causes `New` to return `ErrInvalidHeapCount (or other specific validation error)`. |
| `KeyFunc KeyFunc[T, ID]` | Non-nil application-key extractor. | Required. `nil` causes `New` to return `ErrInvalidHeapCount (or other specific validation error)`. |
| `AcquirePolicy AcquirePolicy` | `Shared` or `Exclusive`; immutable after construction. | Zero value is defaulted to `Shared` by `New`. |
| `AcquireStrategy AcquireStrategy[ID]` | Optional acquire-shard selection policy: called during `Acquire` to choose which Heap Shard to query. Never invoked during `Add` or `BatchAdd`. | `nil` is defaulted to the built-in Round Robin strategy by `New`. |

**Return values:** Passed by value to `New`.

**Errors:** Invalid fields cause `New` to return `ErrInvalidHeapCount (or other specific validation error)`.

**Thread safety:** Treat the value and functions as immutable after passing it to `New`.

**Complexity:** O(1) validation; `New` allocates O(`HeapCount`) shard state.

**Usage example:**

```go
cfg := scheduler.Config[*Worker, string]{
    HeapCount: 2, Comparator: compare, KeyFunc: key, AcquirePolicy: scheduler.Exclusive,
}
```

**Design rationale:** These five inputs are the full generic scheduling contract. Comparator, Acquire Strategy, and AcquirePolicy remain independent. Acquire Strategy covers only acquire-shard selection; resource distribution across shards is an internal scheduler concern. CRS uses no reflection or application field conventions.

### `type Scheduler[T any, ID comparable] struct`

**Purpose:** Opaque concurrent scheduler.

**Parameters:** Created only by `New`.

**Return values:** Used through its methods.

**Errors:** Method-specific.

**Thread safety:** A non-nil scheduler is safe for concurrent method calls.

**Complexity:** Method-specific.

**Usage example:**

```go
s, err := scheduler.New(cfg)
if err != nil { return err }
defer s.Shutdown()
```

**Design rationale:** Opaqueness prevents external mutation of Heap Shards, HeapNodes, the Lookup Map, the Inactive Store, or locks.

### `type Stats struct`

**Purpose:** Immutable snapshot of the scheduler's runtime state. It is intended only for diagnostics, monitoring, and debugging. It never exposes all resources, active/inactive resource lists, internal HeapNodes, the Lookup Map, or Inactive Store contents.

**Parameters:** None.

**Return values:**
- `HeapCount` (total number of shards)
- `TotalResources` (active + inactive)
- `ActiveResources` (currently in Heap Shards)
- `InactiveResources` (currently in the Inactive Store)
- `EmptyHeaps` (shards with 0 resources)
- `NonEmptyHeaps` (shards with >= 1 resources)
- `AcquireStrategy` (string representation of the active policy)
- `AcquirePolicy` (string representation of Shared/Exclusive)
- `HeapSizes` (slice of ints representing resource count per heap)

**Errors:** None.

**Thread safety:** Safe to read after return; it shares no mutable state.

**Complexity:** O(H) where H is the number of heap shards, since it may inspect each heap to produce heap-level information.

**Usage example:**

```go
stats := s.Stats()
log.Printf("active=%d inactive=%d total=%d", stats.ActiveResources, stats.InactiveResources, stats.TotalResources)
```

**Design rationale:** Designed to expose only scheduler runtime information without exposing internal structures or business statuses. Keeps the payload lightweight.

## Exported errors

Use `errors.Is` with these stable sentinels: `ErrInvalidHeapCount (or other specific validation error)`, `ErrInvalidAcquireStrategy`, `ErrNilResource`, `ErrDuplicateKey`, `ErrResourceNotFound`, `ErrNotExclusive`, `ErrResourceNotInactive`, `ErrNoResourceAvailable`, ``, and ``.

- `ErrInvalidHeapCount (or other specific validation error)`: invalid `HeapCount`, nil `Comparator`/`KeyFunc`, or unsupported `AcquirePolicy`.
- `ErrInvalidAcquireStrategy`: the configured Acquire Strategy returned an out-of-range Heap Shard index during `Acquire`.
- `ErrNilResource`: a nil resource was passed to `Add` or `BatchAdd`. Nil resources must never enter the scheduler.
- `ErrDuplicateKey`: an add or batch input repeats an application key already registered in either runtime location.
- `ErrResourceNotFound`: no resource with the given key exists in the Inactive Store.
- `ErrNotExclusive`: `Release` was called but the scheduler's `AcquirePolicy` is `Shared`; no resource is ever placed in the Inactive Store under `Shared` policy. No state is changed.
- `ErrResourceNotInactive`: `Release` or `Include` found the key in the Lookup Map but the resource is not in the Inactive Store (e.g. it is ACTIVE). No state is changed.
- `ErrResourceNotActive`: `Exclude` found the key in the Lookup Map but the resource is not in an active Heap Shard (e.g. it is already INACTIVE). No state is changed.
- `ErrNoResourceAvailable`: `Acquire` found all Heap Shards empty after asking AcquireStrategy for every candidate. No resources are registered in any shard.
- ``: `BatchAdd` was called after initialization ended.
- ``: an active operation was invoked after `Shutdown`.

## Exported functions and methods

### `func New[T any, ID comparable](cfg Config[T, ID]) (*Scheduler[T, ID], error)`

**Purpose:** Resolves configuration defaults, validates the final configuration, and creates an empty ready scheduler.

**Parameters:** `cfg` is the complete immutable configuration.

**Return values:** Scheduler and nil error, or nil and a configuration error.

**Errors:** `ErrInvalidHeapCount (or other specific validation error)`.

**Thread safety:** Construction is independent; the returned scheduler is concurrently usable immediately.

**Complexity:** O(`HeapCount`) setup.

**Usage example:** `s, err := scheduler.New(cfg)`

**Constructor lifecycle:** `New` always performs these five steps in order:

1. **Resolve defaults.** Apply field defaults to the caller's value before any validation:
   - `HeapCount = 0` is resolved to `1`.
   - `AcquireStrategy = nil` is resolved to the built-in Round Robin strategy.
   - `AcquirePolicy` zero value is resolved to `Shared`.
2. **Validate.** After defaults are applied, validate the resolved configuration:
   - `HeapCount` must be greater than zero.
   - `HeapCount` must not exceed an internal implementation maximum. This limit exists to protect against accidental resource exhaustion; it is not part of the public API and is subject to change between releases. Exceeding it returns `ErrInvalidHeapCount (or other specific validation error)` with a descriptive message. CRS never silently clamps or modifies the caller's value.
   - `Comparator` must be non-nil.
   - `KeyFunc` must be non-nil.
   - `AcquireStrategy` must be present after default resolution.
   - `AcquirePolicy` must be a recognized value after default resolution.
3. **Create runtime infrastructure.** Allocate O(`HeapCount`) Heap Shard state and supporting structures.
4. **Initialize internal components.** Wire Lookup Map, Inactive Store, and Acquire Strategy together.
5. **Return.** Return a fully ready scheduler and a nil error, or return nil and `ErrInvalidHeapCount (or other specific validation error)`.

If configuration validation fails at step 2, `New` returns an error immediately. No partial scheduler is created. The scheduler is never partially initialized.

**Design rationale:** Defaults before validation keeps the zero value safe to omit for optional fields while preserving fail-fast behavior for required ones. Validation after defaults means exactly one rule set is applied consistently, regardless of which fields the caller omitted.

### `func (s *Scheduler[T, ID]) BatchAdd(resources []T) error`

**Purpose:** Atomically registers a collection of ACTIVE resources, optimized for bulk insertion at initialization time. `BatchAdd` provides the same observable guarantees as `Add` applied to every element, but may reduce locking, heap-building work, or other internal overhead compared to calling `Add` repeatedly. It is not equivalent to a loop over `Add`.

**Parameters:** A slice of resource values. Each element follows the same identity contract as `Add`: the scheduler calls `KeyFunc(resource)` to derive each key. No separate ID slice is accepted; each resource is the single source of truth for its identity.

**Return values:** Nil on complete publication of all resources.

**Errors:** ``, ``, `ErrNilResource`, or `ErrDuplicateKey`; on any failure, none of the batch is published.

**Thread safety:** Safe concurrently; valid only during the initialization window before normal scheduler operation begins.

**Complexity:** O(`k log n`) for `k` resources across shard insertions after validation.

**Operation steps:** `BatchAdd` performs two strictly ordered phases. No scheduler state is modified until both phases complete without error.

*Phase 1 â€” Full batch validation (no state changes):*

1. **Nil check.** For each element in the slice, if the resource is nil (when `T` is a pointer type), return `ErrNilResource` immediately. No scheduler state is read or modified.
2. **Key derivation.** Call `KeyFunc(resource)` for every element to obtain its unique application key. If `KeyFunc` panics, the panic propagates to the caller; CRS does not recover it.
3. **Intra-batch duplicate detection.** After all keys are derived, verify that no two elements in the incoming slice share the same key. If a duplicate is found within the batch itself, return `ErrDuplicateKey` immediately.
4. **Scheduler duplicate detection.** Check every derived key against the Lookup Map. If any key is already registered in either the ACTIVE or INACTIVE location, return `ErrDuplicateKey` immediately. No existing resource is overwritten.

*Phase 2 â€” Atomic insertion (only reached if Phase 1 passes entirely):*

5. **Shard assignment.** Assign each resource to a target Heap Shard through the scheduler's internal balanced distribution. AcquireStrategy is not consulted.
6. **HeapNode creation and insertion.** Create an internal HeapNode for each resource, register it in the Lookup Map, and push it into its assigned Heap Shard.
7. **Completion.** Return nil. All resources are now ACTIVE.

**Atomicity.** `BatchAdd` is fully atomic. Either every resource is inserted successfully, or none is inserted. Partial insertion never occurs. If any step in Phase 1 fails, the scheduler state is exactly as it was before the call. Phase 2 does not begin unless Phase 1 completes without error.

**Performance note.** `BatchAdd` exists to optimize the common initialization pattern of loading many resources at once. Its implementation may use bulk heap-building strategies (such as heapify-in-place) or reduced per-element locking compared to calling `Add` in a loop. Its observable behavior â€” the same per-resource validation, the same atomicity guarantee, the same error taxonomy â€” is identical to `Add`.

**Panic policy.** `KeyFunc` and `Comparator` are caller-provided functions. If either panics during Phase 1 or Phase 2, the panic propagates to the caller; CRS does not recover it and does not hide bugs in caller-supplied code.

**Usage example:** `err := s.BatchAdd([]*Worker{{Key: "a"}, {Key: "b"}})`

**Design rationale:** Separating validation from insertion makes the atomicity guarantee straightforward to reason about and implement without complex rollback logic. Accepting only the resource slice (not a separate ID slice) keeps the same single-source-of-truth identity contract as `Add`.

### `func (s *Scheduler[T, ID]) Add(resource T) error`

**Purpose:** Registers one ACTIVE resource with the scheduler.

**Parameters:** A single resource value. The scheduler derives the unique key by calling `KeyFunc(resource)`. No separate ID parameter is accepted; the resource is the single source of truth for identity.

**Return values:** Nil on success.

**Errors:** ``, `ErrNilResource`, or `ErrDuplicateKey`.

**Thread safety:** Safe concurrently.

**Complexity:** O(1) nil check plus O(1) Lookup Map check plus O(log n) shard insertion.

**Operation steps:** `Add` always performs these steps in order:

1. **Nil check.** If the resource is nil (when `T` is a pointer type), return `ErrNilResource` immediately. No scheduler state is read or modified.
2. **Key derivation.** Call `KeyFunc(resource)` to obtain the unique application key. If `KeyFunc` panics, the panic propagates to the caller; CRS does not recover it. No scheduler state is modified before this point.
3. **Duplicate detection.** Check the Lookup Map. If the key is already registered in either the ACTIVE or INACTIVE (Inactive Store) location, return `ErrDuplicateKey` immediately. The existing resource is not overwritten and no insertion proceeds.
4. **Shard selection.** Select the target Heap Shard through the scheduler's internal balanced distribution. This is an implementation detail; it is not driven by AcquireStrategy and is not configurable.
5. **Heap insertion.** Create an internal HeapNode, register it in the Lookup Map, and push it into the selected Heap Shard.

**Atomicity.** `Add` is atomic. If any step fails, no HeapNode is inserted, the Lookup Map remains unchanged, and the scheduler state is exactly as it was before the call. A partial insertion never occurs.

**Panic policy.** `KeyFunc` and `Comparator` are caller-provided functions. If either panics, the panic propagates to the caller; CRS does not recover it and does not hide bugs in caller-supplied code.

**Usage example:** `err := s.Add(&Worker{Key: "worker-a"})`

**Design rationale:** Accepting only the resource (not a separate ID) ensures the resource is the single source of truth for identity. CRS never inspects struct fields directly and never requires an ID field on the resource type.

### `func (s *Scheduler[T, ID]) Acquire() (resource T, ok bool, err error)`

**Purpose:** Returns the highest-priority available resource according to the configured AcquireStrategy and AcquirePolicy. `Acquire` never modifies resource priority, never rebalances heaps, and never recalculates comparator ordering. It only selects and acquires.

**Parameters:** None.

**Return values:** Resource, `true`, nil on success; zero `T`, `false`, `ErrNoResourceAvailable` when all Heap Shards are empty; zero `T`, `false`, error on failure.

**Errors:** ``, `ErrInvalidAcquireStrategy`, or `ErrNoResourceAvailable`.

**Thread safety:** Safe concurrently. `Acquire` locks only the single selected Heap Shard; multiple concurrent calls can operate on different shards simultaneously. `Shared` intentionally allows multiple callers to receive the same resource simultaneously. `Exclusive` guarantees a resource moved to the Inactive Store cannot be acquired again until `Release`.

**Complexity:** O(heaps) worst-case empty-shard traversal plus O(log n) for `Exclusive` heap removal; `Shared` adds only a heap peek after shard selection.

**Operation steps:** `Acquire` performs these steps in order:

1. **AcquireStrategy selects next candidate shard.** Ask the configured AcquireStrategy for the next candidate Heap Shard index. AcquireStrategy receives only a read-only shard view; it does not know whether a shard is empty, inspect heap contents, evaluate resource priority, or observe any other scheduler state. Its only responsibility is selecting the next candidate.
2. **Validate shard index.** The scheduler validates the returned index is in `[0, HeapCount)`. An out-of-range index returns `ErrInvalidAcquireStrategy` immediately.
3. **Lock the selected shard.** Acquire the selected shard's mutex. No other shard is locked.
4. **Check for available resources.** Peek at the shard's highest-priority HeapNode.
   - If the shard is non-empty, proceed to step 5.
   - If the shard is empty, unlock it and return to step 1 to ask AcquireStrategy for the next candidate. The scheduler repeats steps 1â€“4 until a non-empty shard is found or every shard has been inspected exactly once.
5. **All shards empty.** If every shard has been checked and all are empty, return `ErrNoResourceAvailable`. No resource is returned; no state is changed.
6. **Apply AcquirePolicy** on the non-empty selected shard:
   - `Shared`: peek the highest-priority HeapNode; the resource remains ACTIVE in its Heap Shard. No ownership change occurs and nothing is moved to the Inactive Store. Unlock and return the resource.
   - `Exclusive`: remove the highest-priority HeapNode from the shard; preserve its original shard ID in the node; move the node to the Inactive Store. Unlock and return the resource. The resource remains unavailable until `Release` is called.
7. **Return the resource.**

**Priority trust.** `Acquire` always trusts the existing heap ordering established by `Comparator` during mutations (`Add`, `BatchAdd`, `Update`). It never recalculates priority inline. If application data affecting priority has changed, the application must call `Update` to restore heap order.

**Panic policy.** If AcquireStrategy or any internal scheduler code panics, the panic propagates to the caller; CRS does not recover it.

**Usage example:**

```go
worker, ok, err := s.Acquire()
if err != nil { return err }   // , ErrInvalidAcquireStrategy, or ErrNoResourceAvailable
if !ok { /* unreachable with current return contract */ }
defer s.Release(workerKey) // required only with Exclusive policy
use(worker)
```

**Design rationale:** The empty-heap retry loop ensures that a caller is not required to handle the case where a AcquireStrategy happens to select a temporarily empty shard. Skipping empty shards internally keeps the caller free of shard-awareness. The single-shard lock-per-step design preserves the concurrency contract: multiple concurrent Acquire calls can proceed simultaneously on different shards.

### `func (s *Scheduler[T, ID]) Release(key ID) error`

**Purpose:** Returns a previously exclusively acquired resource back to its original Heap Shard. `Release` never chooses a heap, never consults AcquireStrategy, never modifies resource priority, and never rebalances resources. Its only responsibility is restoring an exclusively acquired resource into the scheduler.

**Parameters:** The application key identifying the resource to release. The scheduler already owns the resource value while it is exclusively acquired; the caller does not supply the resource again.

**Return values:** Nil on successful reinsertion.

**Errors:** ``, `ErrNotExclusive`, `ErrResourceNotFound`, or `ErrResourceNotInactive`.

**Thread safety:** Safe concurrently. Exactly one concurrent release of the same key succeeds; subsequent concurrent releases return `ErrResourceNotFound`.

**Complexity:** O(1) Inactive Store lookup plus O(log n) insertion into the original Heap Shard.

**Operation steps:** `Release` performs these steps in order:

1. **Shared-mode check.** If the scheduler's `AcquirePolicy` is `Shared`, return `ErrNotExclusive` immediately. No state is read or modified. `Release` is meaningful only when `AcquirePolicy` is `Exclusive`.
2. **Inactive Store lookup.** Look up the provided key in the Inactive Store. If the key is not found, return `ErrResourceNotFound` immediately.
3. **Retrieve original shard.** Read the original owning Heap Shard ID from the HeapNode stored in the Inactive Store. This ID was recorded when the resource was moved by `Acquire`; the scheduler never asks the caller to supply or remember it.
4. **Sanity check.** Verify the located HeapNode is INACTIVE (in the Inactive Store). If it is not, return `ErrResourceNotInactive`. No state is modified.
5. **Lock original shard.** Acquire the mutex of the original owning Heap Shard. No other shard is locked.
6. **Reinsert into heap.** Push the HeapNode back into its original Heap Shard. The resource resumes participation in scheduling with its existing priority ordering.
7. **Remove from Inactive Store.** Remove the node's entry from the Inactive Store.
8. **Unlock shard.** Release the shard mutex.
9. **Return success.** Return nil.

**Atomicity.** `Release` is atomic. If reinsertion into the heap fails, the resource remains in the Inactive Store and the scheduler state is exactly as it was before the call. A partial restoration never occurs.

**AcquireStrategy.** `Release` never consults AcquireStrategy. The resource always returns to its original owning Heap Shard using the shard ID preserved in its HeapNode during `Acquire`.

**Panic policy.** `Comparator` is a caller-provided function invoked during heap insertion. If it panics, the panic propagates to the caller; CRS does not recover it and does not hide bugs in caller-supplied code.

**Usage example:** `err := s.Release(workerKey)`

**Design rationale:** Accepting only the key (not the resource value) reflects that the scheduler already owns and holds the resource while it is exclusively acquired. The caller has no resource value to supply back. Recording the original shard ID in the HeapNode avoids AcquireStrategy involvement and preserves exclusive acquire/release locality without leaking the ID. `Release` pairs with `Acquire()`.

### `func (s *Scheduler[T, ID]) Exclude(key ID) error`

**Purpose:** Removes an ACTIVE resource from its Heap Shard and places it in the Inactive Store. `Exclude` is paired with `Include()`. This represents an application's intention to temporarily pause a resource from scheduling (e.g., for maintenance).

**Parameters:** The application key identifying the resource to exclude.

**Return values:** Nil on successful exclusion.

**Errors:** ``, `ErrResourceNotFound`, or `ErrResourceNotActive`.

**Thread safety:** Safe concurrently. `Exclude` locks only the single owning Heap Shard.

**Complexity:** O(log n) heap removal plus O(1) Inactive Store insertion.

**Operation steps:** `Exclude` looks up the resource by key, validates that it is ACTIVE, locks its original Heap Shard, removes the HeapNode using its stored index, places it into the Inactive Store, and unlocks the shard. No Comparator or AcquireStrategy is called.

**Atomicity.** Moving the resource between the active heap and the Inactive Store is atomic.

**Design rationale:** `Exclude` provides a clean mechanism to temporarily pause a resource without fully removing and re-adding it, sharing the same underlying inactive mechanics as `Exclusive` acquire.

### `func (s *Scheduler[T, ID]) Include(key ID) error`

**Purpose:** Returns a previously excluded resource back to its original Heap Shard. `Include` performs the exact same internal operation as `Release()`, but is paired with `Exclude()` to clearly represent the application's intention to resume a paused resource.

**Parameters:** The application key identifying the resource to include.

**Return values:** Nil on successful reinsertion.

**Errors:** ``, `ErrResourceNotFound`, or `ErrResourceNotInactive`.

**Thread safety:** Safe concurrently.

**Complexity:** O(1) Inactive Store lookup plus O(log n) insertion into the original Heap Shard.

**Operation steps:** Like `Release`, `Include` atomically removes the resource from the Inactive Store and reinserts it into its original heap.

**Atomicity.** Moving the resource between the Inactive Store and the active heap is atomic.

### `func (s *Scheduler[T, ID]) Update(resource T) error`

**Purpose:** Replaces an existing registered resource with its latest state while preserving its identity and restoring the correct scheduler ordering. `Update` is a full replacement operation, not a patch: the scheduler stores the new resource object exactly as provided and never merges individual fields. `Update` never adds a new resource, never creates a duplicate, never changes resource ownership, never moves a resource between heaps, and never consults AcquireStrategy.

**Parameters:** A single replacement resource. The scheduler derives the unique key by calling `KeyFunc(resource)`. No separate key parameter is accepted; the resource is the single source of truth for identity. The key extracted from the replacement resource must match an already-registered resource; the key itself is immutable and cannot be changed by `Update`. If a different key is required, the caller must call `Remove` on the old key then `Add` with the new resource.

**Return values:** Nil on success.

**Errors:** ``, `ErrNilResource`, or `ErrResourceNotFound`.

**Thread safety:** Safe concurrently. `Update` locks only the single affected Heap Shard when the resource is ACTIVE; it uses only the Inactive Store's own synchronization when the resource is INACTIVE. It never locks every shard simultaneously.

**Complexity:** O(log n) for ACTIVE resources (heap.Fix); O(1) for INACTIVE resources (value replacement only).

**Operation steps:** `Update` always performs these steps in order:

1. **Nil check.** If the resource is nil (when `T` is a pointer type), return `ErrNilResource` immediately. No scheduler state is read or modified.
2. **Key derivation.** Call `KeyFunc(resource)` to obtain the unique application key. If `KeyFunc` panics, the panic propagates to the caller; CRS does not recover it. No scheduler state is modified before this point.
3. **Key validation.** Verify the extracted key is valid (e.g. non-zero for the `ID` type). If the key is invalid, return `ErrResourceNotFound` immediately.
4. **Lookup.** Look up the key in the Lookup Map. If the key is not found in either runtime location, return `ErrResourceNotFound` immediately. No scheduler state is modified.
5. **Replace resource value.** Write the new resource into the existing HeapNode. The HeapNode itself, its shard ID, and its heap index are not changed; only the stored resource value is replaced.
6. **ACTIVE path â€” restore heap ordering.** If the resource is ACTIVE (its HeapNode is in a Heap Shard):
   - Lock only the owning Heap Shard.
   - Call `heap.Fix()` on the existing HeapNode at its current heap index. This restores comparator-defined ordering without removing and reinserting the node and without creating a new HeapNode.
   - Unlock the shard.
7. **INACTIVE path â€” Inactive Store update only.** If the resource is INACTIVE (its HeapNode is in the Inactive Store):
   - Replace the stored resource value inside the Inactive Store entry. No heap operation is performed and no shard is locked.
   - When `Release` is later called, the updated resource value will be reinserted into its original Heap Shard with correct ordering.
8. **Return success.** Return nil.

**Atomicity.** `Update` is atomic. If any step fails, the original resource value remains unchanged, heap ordering is unchanged, and the Inactive Store is unchanged. The scheduler state is exactly as it was before the call.

**Resource identity.** The resource key is immutable. `Update` never changes the key of a registered resource. If the replacement resource carries a different key (i.e. `KeyFunc` returns a key that does not match any registered resource), `Update` returns `ErrResourceNotFound` and leaves the scheduler unchanged.

**AcquireStrategy.** `Update` never consults AcquireStrategy. The resource's owning Heap Shard never changes during `Update`.

**Panic policy.** `KeyFunc` and `Comparator` are caller-provided functions. If either panics, the panic propagates to the caller; CRS does not recover it and does not hide bugs in caller-supplied code.

**Usage example:** `err := s.Update(&Worker{Key: "worker-a", Priority: 10})`

**Design rationale:** Accepting the resource (not a separate key) keeps the same single-source-of-truth identity contract as `Add`. Using `heap.Fix()` on the existing HeapNode avoids remove/reinsert overhead and preserves pointer stability for the Lookup Map. Performing no heap operation when the resource is in the Inactive Store defers ordering cost to `Release`, where a heap insertion already occurs.

### `func (s *Scheduler[T, ID]) Remove(key ID) error`

**Purpose:** Permanently unregisters an existing resource from the scheduler, regardless of its current runtime location. `Remove` never disables, acquires, or releases a resource, never rebalances heaps, and never consults AcquireStrategy or Comparator. Its only responsibility is permanently deleting the resource and cleaning up its Lookup Map entry.

**Parameters:** The application key identifying the resource to remove. No resource object is accepted; the scheduler locates the resource by key alone.

**Return values:** Nil on successful removal.

**Errors:** `` or `ErrResourceNotFound`.

**Thread safety:** Safe concurrently. `Remove` locks only the single owning Heap Shard when the resource is ACTIVE; it uses only the Inactive Store's own synchronization when the resource is INACTIVE. It never locks every shard simultaneously.

**Complexity:** O(log n) for ACTIVE removal (heap removal by stored index); O(1) for INACTIVE (Inactive Store) removal.

**Operation steps:** `Remove` always performs these steps in order:

1. **Key lookup.** Look up the provided key in the Lookup Map. If the key is not found in either runtime location, return `ErrResourceNotFound` immediately. No scheduler state is modified.
2. **Determine runtime location.** Inspect the located HeapNode to determine whether the resource is ACTIVE (in a Heap Shard) or INACTIVE (in the Inactive Store).
3. **ACTIVE path.** If the resource is ACTIVE:
   - Lock only the owning Heap Shard.
   - Remove the HeapNode from the heap using its stored heap index. No Comparator is called during removal.
   - Remove the Lookup Map entry for this key.
   - Unlock the shard.
4. **INACTIVE path.** If the resource is INACTIVE (in the Inactive Store):
   - Remove the HeapNode from the Inactive Store. No shard is locked and no Comparator is called.
   - Remove the Lookup Map entry for this key.
5. **Return success.** Return nil.

**Atomicity.** `Remove` is atomic. If any internal step fails, no partial deletion occurs: the heap membership, the Inactive Store membership, and the Lookup Map entry all remain consistent and unchanged. The scheduler state is exactly as it was before the call.

**AcquirePolicy.** `Remove` behaves identically regardless of the configured `AcquirePolicy`. Resources may be removed whether they are ACTIVE in a Heap Shard or currently held INACTIVE in the Inactive Store.

**AcquireStrategy.** `Remove` never consults AcquireStrategy.

**Comparator.** `Remove` never calls `Comparator`. Heap removal uses the HeapNode's stored index directly; no ordering comparison is needed.

**Panic policy.** `Remove` does not invoke any caller-provided callbacks. No panic can originate from `Remove` itself; the only possible panic would be an internal invariant violation, which CRS does not recover.

**Usage example:** `err := s.Remove(workerKey)`

**Design rationale:** Accepting only the key (not the resource object) is consistent with the `Release` API and reflects that `Remove` is a pure identity operation. The resource object is not required because removal is driven entirely by Lookup Map membership. Removing by stored heap index makes ACTIVE removal O(log n) without scanning the heap.


### `func (s *Scheduler[T, ID]) Get(key ID) (T, error)`

**Purpose:** Return the resource currently owned by the scheduler. It is a read-only operation. It never modifies scheduler state, changes heap ordering, or calls AcquireStrategy or Comparator.

**Parameters:** Application-owned key.

**Return values:** The stored resource and a nil error, or zero `T` and an error.

**Errors:** `` or `ErrResourceNotFound`.

**Thread safety:** Safe concurrently.

**Complexity:** O(1) Lookup Map access.

**Operation steps:**
1. Look up the resource using the Lookup Map.
2. If the resource does not exist, return `ErrResourceNotFound`.
3. Return the stored resource.

### `func (s *Scheduler[T, ID]) Len() int`

**Purpose:** Returns the total number of resources currently managed by the scheduler (active and inactive resources combined). It is a read-only operation and never modifies scheduler state.

**Parameters:** None.

**Return values:** Total resource count.

**Errors:** None.

**Thread safety:** Safe concurrently.

**Complexity:** O(1).

### `func (s *Scheduler[T, ID]) Stats() Stats`

**Purpose:** Returns a lightweight, read-only snapshot of the scheduler's runtime state.

**Parameters:** None.

**Return values:** Immutable `Stats` snapshot.

**Errors:** None.

**Thread safety:** Safe concurrently; snapshot may be stale immediately after return. It never modifies scheduler state.

**Complexity:** O(H), where H is the number of heap shards.

**Usage example:** `total := s.Stats().TotalResources`

**Design rationale:** Provides observability without exporting collections or status models.

### `func (s *Scheduler[T, ID]) Shutdown()`

**Purpose:** Permanently closes the scheduler.

**Parameters:** None.

**Return values:** None; idempotent.

**Errors:** None. Afterwards all operations except `Stats` return ``.

**Thread safety:** Safe concurrently; racing operations either finish normally or return ``, without partial mutation.

**Complexity:** O(1) logical closure.

**Usage example:** `defer s.Shutdown()`

**Design rationale:** Defines a clear concurrent lifecycle without managing caller resources.

## API stability

v1 avoids breaking public API changes. New functionality should be additive whenever possible; breaking changes are reserved for a future major version. HeapNode, Heap Shards, the Lookup Map, the Inactive Store, and all runtime metadata remain implementation details and may change without API notice. Acquire Strategy is the stable extension boundary for placement policies.
