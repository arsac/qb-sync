# Agent Instructions

## Package Manager

Use **Go modules**: `go mod tidy`, `go build ./...`

## Module

- Module path: `github.com/arsac/qb-sync`
- Go project: torrent sync system (source -> destination) using gRPC streaming

## Architecture

- `internal/source` — Source orchestrator: streams pieces from source qBittorrent to destination, handles drain/handoff
- `internal/destination` — Destination gRPC server: receives pieces, verifies, finalizes torrents
- `internal/streaming` — BidiQueue, PieceMonitor, GRPCDestination, congestion control
- `internal/config` — `BaseConfig` embedded in `SourceConfig` and `DestinationConfig`
- `internal/metrics` — Prometheus metrics with `qbsync_` namespace
- `internal/health` — HTTP health/metrics server
- `internal/qbclient` — Resilient qBittorrent API wrapper with circuit breaker
- `proto/` — Protobuf definitions, generated code

## Skills

- Use `golang-pro` skill for Go implementation work
- Use `golang-grpc` skill for gRPC/protobuf changes (service definitions, interceptors, streaming)
- Use `grpc-protobuf` skill for protobuf layout and codegen

## Key Patterns

- Error variables: `fooErr` naming (not `errFoo`)
- `int32`/`int` conversions at proto boundaries are intentional
- Destination server lock ordering: `s.mu` -> `state.mu` -> `inodes.registeredMu` -> `inodes.inProgressMu`
- `InodeRegistry` is self-contained with internal locking
- `collectTorrents()` pattern: acquire `s.mu`, snapshot refs, release `s.mu`, then process under individual `state.mu`
- Injectable function fields on structs for testability (e.g., `checkAnnotation` on `Runner`)
- Drain annotation check is fail-closed: skip drain on error

## Testing

| Command                                  | Description                 |
| ---------------------------------------- | --------------------------- |
| `go test ./internal/... -short -count=1` | Unit tests                  |
| `go test -tags=e2e ./test/e2e/...`       | E2E tests (requires Docker) |
| `go vet -tags=e2e ./test/e2e/...`        | Vet E2E code                |

- E2E tests use `//go:build e2e` tag and testcontainers (docker-compose)
- E2E test helpers in `testenv.go` with functional options (`WithDryRun`, `WithForce`, etc.)

## Linting

- `golangci-lint run --fix` (v2.7.1, config in `.golangci.yml`)
- ~70+ linters enabled, strict settings
- `goimports` with local prefix `github.com/arsac/qb-sync`
- `golines` max-len 120

## Git Hooks

- `lefthook` configured in `lefthook.yml`
- Pre-commit: `golangci-lint run --fix`
- Pre-push: unit tests + e2e vet

## Metrics

- When adding, removing, or updating Prometheus metrics in `internal/metrics/`, also update `METRICS.md` to keep the documentation in sync

## Code Review

- After finishing a code change, always run the `code-simplifier:code-simplifier` agent on modified files

## CLI

| Command              | Description              |
| -------------------- | ------------------------ |
| `qbsync source`      | Run source server        |
| `qbsync destination` | Run destination server   |

- Flags bound via Cobra/Viper
- Env prefix: `QBSYNC_SOURCE_` / `QBSYNC_DESTINATION_`
