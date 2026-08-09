# Release Checklist v1.0.0

## Pre-release
- [x] Achieve 100% statement coverage across all core packages.
- [x] Verify zero data races using `go test -race ./...`.
- [x] Finalize `README.md`, `OVERVIEW.md`, `ARCHITECTURE.md`, `API.md`, and `ROADMAP.md`.
- [x] Ensure all exported identifiers have production-grade Godoc comments.
- [x] Create comprehensive and compiling examples for all major features.
- [x] Add MIT `LICENSE`.
- [x] Initialize `.github/workflows/ci.yml` for automated CI/CD.
- [x] Draft `CHANGELOG.md`.

## Release
- [ ] Push all final commits to the `main` branch.
- [ ] Ensure GitHub Actions CI workflow passes on `main`.
- [ ] Create a new GitHub Release with the tag `v1.0.0`.
- [ ] Paste the v1.0.0 `CHANGELOG.md` notes into the GitHub Release description.
- [ ] Publish the release.

## Post-release
- [ ] Verify visibility on [pkg.go.dev](https://pkg.go.dev/github.com/phero20/concurrent-resource-scheduler).
