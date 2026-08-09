# Contributing to CRS

Thanks for contributing to Concurrent Resource Scheduler (CRS). This project aims to be a small, dependable Go scheduling library with clear ownership boundaries and predictable concurrency behavior.

## Project philosophy

- CRS schedules generic resources; it does not own application or provider policy. Comparator, Acquire Strategy, and AcquirePolicy have independent responsibilities.
- Correctness, explicit invariants, and short lock hold times come before cleverness.
- Keep the public API narrow and stable.
- Prefer standard-library, dependency-light Go.

## Coding style

- Follow idiomatic Go and run `gofmt`.
- Avoid globals, reflection, hidden side effects, and unnecessary dependencies.
- Do not run callbacks, I/O, external logging hooks, or application work while holding scheduler locks.
- Treat the comparator as a restricted dependency: it must define a strict weak ordering and be pure, deterministic, thread-safe, fast, non-blocking.

## Strict Implementation Rules

The architecture is **frozen** at v1.0.0. Every implementation decision must respect the finalized API and architecture.
1. First attempt to solve it internally.
2. Do NOT modify the public API.
3. Do NOT redesign the architecture.

## Testing requirements

- ALL test files must be placed inside their respective packages using `package <name>_test` for black-box testing.
- Target **100% statement coverage** for all changes.
- For any shared-state change, run:

```sh
go test ./...
go test -race ./...
```
