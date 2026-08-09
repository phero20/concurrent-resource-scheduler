# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-08-09

### Added
- Core Scheduler implementation exposing `Acquire`, `BatchAdd`, `Add`, `Release`, `Exclude`, `Include`, `Remove`, and `Update`.
- Thread-safe Sharded Heaps eliminating global locks for extreme concurrency.
- Internal `Lookup Map` enabling O(1) lock-free resource membership querying.
- Event Dispatcher Hooks for asynchronous, zero-blocking telemetry and callbacks.
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
- 100% test coverage with zero data races.
