# Contributing to CRS

Thanks for contributing to Concurrent Resource Scheduler (CRS). This project aims to be a small, dependable Go scheduling library with clear ownership boundaries and predictable concurrency behavior.

## Project philosophy

- CRS schedules generic resources; it does not own application or provider policy. Comparator, Acquire Strategy, and AcquirePolicy have independent responsibilities.
- Correctness, explicit invariants, and short lock hold times come before cleverness.
- Keep the public API narrow and stable.
- Prefer standard-library, dependency-light Go.
- A change is incomplete until its documentation, tests, and concurrency implications are reviewed.

Read [README.md](README.md), [ARCHITECTURE.md](ARCHITECTURE.md), [ROADMAP.md](ROADMAP.md), and [AGENTS.md](AGENTS.md) before proposing architectural work.

## Coding style

- Follow idiomatic Go and run `gofmt` on changed Go files.
- Prefer small packages, cohesive functions, explicit control flow, and composition.
- Add Go doc comments for exported identifiers; comments begin with the identifier name.
- Avoid globals, reflection, hidden side effects, catch-all utility packages, and unnecessary dependencies.
- Keep resource business fields and policy outside CRS.
- Do not couple scheduler core logic to Round Robin. New placement behavior belongs behind the Acquire Strategy abstraction and must not expose HeapNodes, locks, or mutable Heap Shards.
- Do not run callbacks, I/O, external logging hooks, or application work while holding scheduler locks.
- Treat the comparator as a restricted dependency: it must define a strict weak ordering and be pure, deterministic, thread-safe, fast, non-blocking, and unable to re-enter the scheduler. Comparator panics propagate; CRS does not recover them.

## Pull request rules

- Keep each pull request focused on one concern or roadmap phase.
- Explain the problem, scope, design choice, affected invariants, and any compatibility impact.
- Do not include unrelated refactors, formatting churn, generated artifacts, credentials, or application-specific integrations.
- Update relevant documentation when changing public contracts, package ownership, flows, locks, complexity, roadmap status, or repository conventions.
- Breaking public API changes require explicit maintainer approval and a versioning/migration plan.
- Synchronization changes require a lock-order explanation in the PR description.

## Testing requirements

- ALL test files must be placed inside the `tests/` directory (e.g., `tests/scheduler/`, `tests/placement/`). Never place test files next to the implementation.
- Add or update focused tests for every behavior change.
- Cover success, invalid input, duplicate/unknown-resource, empty-state, and lifecycle paths relevant to the change.
- Protect heap, lookup, and membership invariants with tests.
- For any shared-state change, run:

```sh
go test ./...
go test -race ./...
```

- Prefer deterministic synchronization in tests; do not rely on arbitrary sleeps to prove concurrency correctness.
- Add a regression test for every fixed defect when practical.

## Benchmark requirements

Changes affecting shared/exclusive acquire, release, update, remove, HeapNode storage, comparator invocation, allocations, Lookup Map synchronization, Inactive Store behavior, or lock duration require benchmark evidence.

- Use Go benchmarks with setup outside the timed loop.
- Benchmark representative heap counts and resource-pool sizes.
- Include contention workloads where relevant.
- Compare against a baseline and explain material regressions or improvements.
- Do not claim performance improvements without reproducible benchmark results.

## Commit conventions

Use concise Conventional Commit-style subjects:

```text
feat(heap): add private single-heap update support
fix(lookup): preserve index during concurrent removal
test(scheduler): cover exclusive acquire and release
docs(architecture): clarify batch-add atomicity
perf(acquire): reduce allocation in shared peek path
```

Use an imperative subject, keep it focused, and include a body when the change needs rationale, invariant notes, or compatibility context.

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

## Review checklist

Reviewers should verify:

- [ ] The change belongs in CRS and does not add domain/provider behavior.
- [ ] Package boundaries and public API remain intentional and minimal.
- [ ] Heap membership, heap index, and lookup-map invariants remain true on success and failure.
- [ ] Lock ownership/order is explicit; normal operations do not introduce a global heap lock.
- [ ] No unbounded or caller-controlled work runs while a scheduler lock is held.
- [ ] Error behavior is typed, clear, and consistent with documented contracts.
- [ ] Tests cover behavior and race testing passes where shared state changed.
- [ ] Benchmarks accompany hot-path or contention changes.
- [ ] Documentation and examples remain internally consistent and use no sensitive data.

## Reporting issues

Use issues for reproducible defects, design proposals, and documentation gaps. Include the expected behavior, actual behavior, Go version, a minimal reproduction, and relevant race/benchmark output where appropriate. Report suspected security or concurrency vulnerabilities privately to maintainers; do not publish sensitive exploit details.
