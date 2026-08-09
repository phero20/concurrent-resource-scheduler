# CRS Roadmap

The CRS project was executed in 7 strictly sequential phases. It is now completely stable and tagged `v1.0.0`.

## Phase 1 ✅ Contracts and configuration subsystem
- Defined public API, KeyFunc, Comparator, AcquirePolicy, config model, and error taxonomy.

## Phase 2 ✅ Indexed heap subsystem
- Implemented O(log n) private priority heap maintaining comparator order and heap-local indices.

## Phase 3 ✅ Lookup subsystem
- Implemented KeyFunc-derived mapping and atomic synchronization protocol.

## Phase 4 ✅ Scheduler orchestration
- Composed heap, lookup, and placement packages into concurrent core operations (`Add`, `Acquire`, `Release`, `Update`, etc.).

## Phase 5 ✅ Advanced Placement Strategies
- **Phase 5.1 & 5.2**: Consistent Hashing and `AcquireByAffinity` for Sticky Sessions.
- **Phase 5.3**: Weighted Random Selection via splitmix64.
- **Phase 5.4**: Adaptive Load Balancing favoring less-contended shards.

## Phase 6 ✅ Scheduler Hooks & Extension APIs
- **Phase 6.1**: Event Taxonomy & Observer Contract.
- **Phase 6.2**: Asynchronous Non-Blocking Dispatcher (`eventStream`).
- **Phase 6.3**: Core Lifecycle Instrumentation (`emit`).
- **Phase 6.4**: Reference Integration (External Cooldown Manager).

## Phase 7 ✅ Observability & Metrics
- **Phase 7.1**: Telemetry & Metrics Aggregation via lock-free `sync/atomic` counters.
- **Phase 7.2**: Prometheus Exporter (`extensions/prometheus`).

**Status**: v1.0.0 Finalized. 100% Statement Coverage achieved.
