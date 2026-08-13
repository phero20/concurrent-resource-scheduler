# CRS v1 Public API

This is the authoritative public contract for package `scheduler`. 

## Core Configuration

```go
type Config[T any, ID comparable] struct {
    HeapCount       int
    Comparator      Comparator[T]
    KeyFunc         KeyFunc[T, ID]
    AcquirePolicy   config.AcquirePolicy
    AcquireStrategy acquire.AcquireStrategy
    Observers       []events.Observer[ID]
}
```

### Acquire Policies (`config.AcquirePolicy`)
- `config.Shared`: Returns a resource while leaving it ACTIVE in its Heap Shard.
- `config.Exclusive`: Removes a resource from its Heap Shard, places it INACTIVE in the Inactive Store. Requires `Release` before it can be acquired again.

### Acquire Strategies (`acquire.AcquireStrategy`)
- `acquire.NewRoundRobin()`: Cycles shards sequentially.
- `acquire.NewWeightedStrategy(weights []uint)`: Distributes based on capacity weights.
- `acquire.NewAdaptiveStrategy()`: Dynamically favors lightly loaded shards based on O(1) active counts.
- `acquire.NewConsistentHashRing(shardCount)`: Used internally for `AcquireByAffinity`.

## Lifecycle Methods

| Operation | Purpose |
| --- | --- |
| `New` | Validate configuration and create a scheduler. |
| `BatchAdd` | Atomically validate and insert a batch of resources; validates every element before modifying state. |
| `Add` | Add one resource. |
| `Remove` | Permanently remove a resource by key. Works regardless of `AcquirePolicy`. |
| `Acquire` | Ask the Acquire Strategy for a shard and return a resource according to `AcquirePolicy`. |
| `AcquireByAffinity` | Hash a `acquire.AffinityIdentifier` to deterministically target a shard (Sticky Sessions). |
| `Release` | Return an inactive resource (removed by `Exclusive` acquire) to its original Heap Shard. |
| `Get` / `Len` | Read-only state retrieval. |
| `Exclude` | Remove a resource from its Heap Shard and place it in the Inactive Store manually. |
| `Include` | Return an inactive resource (removed by `Exclude`) to its original Heap Shard. |
| `Update` | Replace a resource by key: calls `heap.Fix` to restore ordering for ACTIVE resources. |
| `Stats` | Return a lightweight, read-only snapshot of the scheduler's runtime state. |
| `Shutdown` | Safely close the scheduler, stopping internal event dispatchers. |

## Extension APIs

CRS exposes an event system to build plugins without altering the core library.

### `events.Observer`
```go
type Observer[ID comparable] interface {
    OnEvent(e Event[ID])
}
```

Available events: `EventAdd`, `EventAcquire`, `EventRelease`, `EventExclude`, `EventInclude`, `EventRemove`, `EventUpdate`.

**Included Extensions:**
- `extensions/cooldown`: `NewManager(controller, duration)` (where `controller` implements `LifecycleController`)
- `extensions/metrics`: `NewTelemetryObserver()`
- `extensions/prometheus`: `NewCollector(scheduler, telemetry)`
