# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-08-09

### Added
- Core Scheduler implementation exposing `Acquire`, `BatchAdd`, `Add`, `Release`, `Exclude`, `Include`, `Remove`, and `Update`.
- Thread-safe Sharded Heaps eliminating global locks for extreme concurrency.
- Internal `Lookup Map` enabling O(1) concurrent-safe resource membership querying.
- Event Dispatcher Hooks for asynchronous, non-blocking telemetry and callbacks.
- Pluggable `AcquireStrategy` system supporting:
  - `RoundRobin`
  - `WeightedStrategy`
  - `AdaptiveStrategy`
  - `ConsistentHashRing` (via deterministic `AcquireByAffinity`)
- Extension APIs for real-time observability and manipulation:
  - `extensions/cooldown` (Cooldown Manager).
  - `extensions/metrics` (Lock-free atomic throughput aggregator).
  - `extensions/prometheus` (Prometheus exporter collector).
- Extensive production-ready documentation and standalone examples.
- extensive test coverage with zero detected data races under Go's race detector.

### Changed
- Renamed the internal `placement` concepts to the public `acquire` package to better reflect the strict decoupling of priority from acquisition strategies.
