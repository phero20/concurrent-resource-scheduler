# Architecture

## Component Boundaries

The scheduler is strictly layered. Higher layers depend on lower layers, with no upward dependencies.

```mermaid
flowchart TD
    Application --> Scheduler
    Scheduler --> Placement
    Scheduler --> Events
    Scheduler --> Heap
    Scheduler --> Lookup
    Scheduler --> Inactive
    Heap --> HeapNode
    Lookup --> HeapNode
    Inactive --> HeapNode
    Events --> Extensions
```

## Concurrency Model

The scheduler achieves high throughput by strictly sharding heap locks. There is no global heap mutex. The `Lookup Map` owns its own `sync.RWMutex`, entirely separated from the heaps.

```mermaid
flowchart TD
    subgraph S1 [Shard 1]
        M1[Mutex] --> D1[Heap Data]
    end
    subgraph S2 [Shard 2]
        M2[Mutex] --> D2[Heap Data]
    end
    subgraph SN [Shard N]
        MN[Mutex] --> DN[Heap Data]
    end
```

## Atomic State Transitions

A resource exists in exactly one location at a time: an active heap or the Inactive Store. Transitions between these states are perfectly atomic.

```mermaid
flowchart TD
    Add --> ActiveHeap[Active Heap]
    ActiveHeap --> AcquireExclusive[Acquire Exclusive]
    AcquireExclusive --> InactiveStore[Inactive Store]
    InactiveStore --> Release
    Release --> ActiveHeap

    ActiveHeap --> Exclude
    Exclude --> InactiveStore
    InactiveStore --> Include
    Include --> ActiveHeap
    
    ActiveHeap --> Remove
    InactiveStore --> Remove
    Remove --> Deleted
```

## Event Dispatcher Pipeline

To maintain O(log N) hot-path performance, telemetry is decoupled. Public methods emit lightweight structs into a buffered channel. A background goroutine drains this channel and invokes `Observer.OnEvent()`.

```mermaid
flowchart LR
    Acquire -->|emit| Stream[Buffered Channel]
    Release -->|emit| Stream
    Update -->|emit| Stream
    Stream -->|Background Drain| Observers
    Observers --> Prometheus
    Observers --> CooldownManager
```
