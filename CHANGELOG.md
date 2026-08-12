# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.1] - 2026-08-12

### Documentation

- Documented the multi-module architecture introduced for the core scheduler
  and optional Prometheus extension.
- Documented the Go 1.22+ requirement for the core scheduler.
- Documented the Go 1.25+ requirement for the optional Prometheus extension.
- Clarified that the core scheduler has zero third-party dependencies.

---

## [1.2.0] - 2026-08-12

### Added

- Multi-module architecture separating the core scheduler from optional Prometheus support.
- Go 1.22+ support for the core scheduler.
- Optional Prometheus extension with its own Go 1.25+ module.
- Production-grade GoDoc documentation across all public packages.
- Expanded concurrency, correctness, and stress-test coverage.
- CI validation across Go 1.22 and Go 1.25.
- Added `SECURITY.md` and improved `CONTRIBUTING.md`.
- Improved examples and production-readiness documentation.

### Changed

- Removed all third-party dependencies from the core module.
- Moved Prometheus integration into the independent
  `extensions/prometheus` Go module.
- Prometheus support is now optional and does not affect core scheduler
  dependencies.
- Improved documentation of concurrency, lifecycle, error, and API semantics.

---

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
- Extensive test coverage with zero detected data races under Go's race detector.

### Changed
- Renamed the internal `placement` concepts to the public `acquire` package to better reflect the strict decoupling of priority from acquisition strategies.
