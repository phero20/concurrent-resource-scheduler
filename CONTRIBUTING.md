# Contributing to CRS

Thanks for contributing to Concurrent Resource Scheduler (CRS). This project aims to be a small, dependable Go scheduling library with clear ownership boundaries and predictable concurrency behavior.

## Project philosophy

- CRS schedules generic resources; it does not own application or provider policy. Comparator, Acquire Strategy, and AcquirePolicy have independent responsibilities.
- Correctness, explicit invariants, and short lock hold times come before cleverness.
- Keep the public API narrow and stable.
- Prefer standard-library, dependency-light Go.

## Development Setup

1. **Install Go**: Ensure you have Go 1.22 or later installed (Go 1.25 is required to work on the Prometheus extension).
2. **Clone the repository**: `git clone https://github.com/phero20/concurrent-resource-scheduler.git`
3. **Verify installation**: Run `go test ./...` in the root and `go test ./...` in `extensions/prometheus`.

## Multi-Module Releasing

This repository contains multiple Go modules. To cut a new release (e.g., `v1.2.0`), you must create and push two separate Git tags:
- `v1.2.0` (for the core module)
- `extensions/prometheus/v1.2.0` (for the Prometheus extension module)

## Coding style

- Follow idiomatic Go and run `gofmt -w .` before committing.
- Run `go vet ./...` to check for common static analysis errors.
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
- Target **94-100% package coverage** for all changes.
- For any shared-state change, run:

```sh
go test -count=1 ./...
go test -count=1 -race ./...
```

## Pull Request Expectations

- **Small and Focused**: Keep PRs single-purpose. Do not mix refactoring with feature additions.
- **Testing**: Include tests for all new behavior or bug fixes.
- **Documentation**: Update GoDoc comments for any changed exported APIs.
- **CI**: Ensure all GitHub Actions checks pass before requesting a review.

## Issue Reporting

- Use [GitHub Issues](https://github.com/phero20/concurrent-resource-scheduler/issues) for bug reports and feature requests.
- For bugs, please include the Go version, OS, a minimal reproducible example, and the expected vs. actual behavior.
- For security vulnerabilities, see [SECURITY.md](SECURITY.md).
