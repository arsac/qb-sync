# qb-sync

Sync torrents between qBittorrent instances. Stream pieces from a source server to a destination server in real-time as they download.

## Installation

```bash
go install github.com/arsac/qb-sync/cmd/qbsync@latest
```

Or with Docker:

```bash
docker pull ghcr.io/arsac/qb-sync:latest
```

## Usage

### Source Server

Streams pieces from source qBittorrent to destination server:

```bash
qbsync source \
  --data /downloads \
  --qb-url http://localhost:8080 \
  --destination-addr 192.168.1.100:50051
```

### Destination Server

Receives pieces and adds verified torrents to destination qBittorrent:

```bash
qbsync destination \
  --data /downloads \
  --qb-url http://localhost:8080 \
  --listen :50051
```

## Environment Variables

All flags can be set via environment variables with the prefix `QBSYNC_SOURCE_` or `QBSYNC_DESTINATION_`.

### Source Server

| Variable | Flag | Description | Default |
|----------|------|-------------|---------|
| `QBSYNC_SOURCE_DATA` | `--data` | Data directory path | (required) |
| `QBSYNC_SOURCE_QB_URL` | `--qb-url` | qBittorrent WebUI URL | (required) |
| `QBSYNC_SOURCE_QB_USERNAME` | `--qb-username` | qBittorrent username | |
| `QBSYNC_SOURCE_QB_PASSWORD` | `--qb-password` | qBittorrent password | |
| `QBSYNC_SOURCE_DESTINATION_ADDR` | `--destination-addr` | Destination server gRPC address | (required) |
| `QBSYNC_SOURCE_MIN_SPACE` | `--min-space` | Min free space (GB) before syncing | `50` |
| `QBSYNC_SOURCE_MIN_SEEDING_TIME` | `--min-seeding-time` | Min seeding time (seconds) | `3600` |
| `QBSYNC_SOURCE_SLEEP` | `--sleep` | Sleep interval between checks (seconds) | `30` |
| `QBSYNC_SOURCE_RATE_LIMIT` | `--rate-limit` | Max bytes/sec (0 = unlimited) | `0` |
| `QBSYNC_SOURCE_PIECE_TIMEOUT` | `--piece-timeout` | Timeout for stale in-flight pieces (seconds) | `60` |
| `QBSYNC_SOURCE_RECONNECT_MAX_DELAY` | `--reconnect-max-delay` | Max reconnect backoff delay (seconds) | `30` |
| `QBSYNC_SOURCE_NUM_SENDERS` | `--num-senders` | Concurrent sender workers | `4` |
| `QBSYNC_SOURCE_MIN_CONNECTIONS` | `--min-connections` | Minimum TCP connections to destination server | `2` |
| `QBSYNC_SOURCE_MAX_CONNECTIONS` | `--max-connections` | Maximum TCP connections to destination server | `8` |
| `QBSYNC_SOURCE_SYNCED_TAG` | `--synced-tag` | Tag for synced torrents (empty to disable) | `synced` |
| `QBSYNC_SOURCE_SOURCE_REMOVED_TAG` | `--source-removed-tag` | Tag on destination when source removed (empty to disable) | `source-removed` |
| `QBSYNC_SOURCE_EXCLUDE_CLEANUP_TAG` | `--exclude-cleanup-tag` | Tag that prevents cleanup from source (empty to disable) | |
| `QBSYNC_SOURCE_DRAIN_ANNOTATION` | `--drain-annotation` | Pod annotation key to gate shutdown drain (empty to drain unconditionally) | `qbsync/drain` |
| `QBSYNC_SOURCE_DRAIN_TIMEOUT` | `--drain-timeout` | Shutdown drain timeout (seconds) | `300` |
| `QBSYNC_SOURCE_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
| `QBSYNC_SOURCE_LOG_LEVEL` | `--log-level` | Log level: debug, info, warn, error | `info` |
| `QBSYNC_SOURCE_DRY_RUN` | `--dry-run` | Run without making changes | `false` |

### Destination Server

| Variable | Flag | Description | Default |
|----------|------|-------------|---------|
| `QBSYNC_DESTINATION_DATA` | `--data` | Data directory path | (required) |
| `QBSYNC_DESTINATION_LISTEN` | `--listen` | gRPC listen address | `:50051` |
| `QBSYNC_DESTINATION_SAVE_PATH` | `--save-path` | Save path as qBittorrent sees it | (defaults to `--data`) |
| `QBSYNC_DESTINATION_QB_URL` | `--qb-url` | qBittorrent WebUI URL | |
| `QBSYNC_DESTINATION_QB_USERNAME` | `--qb-username` | qBittorrent username | |
| `QBSYNC_DESTINATION_QB_PASSWORD` | `--qb-password` | qBittorrent password | |
| `QBSYNC_DESTINATION_POLL_INTERVAL` | `--poll-interval` | Verification poll interval (seconds) | `2` |
| `QBSYNC_DESTINATION_POLL_TIMEOUT` | `--poll-timeout` | Verification timeout (seconds) | `300` |
| `QBSYNC_DESTINATION_STREAM_WORKERS` | `--stream-workers` | Concurrent piece writers (0 = auto: 8) | `0` |
| `QBSYNC_DESTINATION_MAX_STREAM_BUFFER` | `--max-stream-buffer` | Global memory budget for buffered pieces (MB) | `512` |
| `QBSYNC_DESTINATION_SYNCED_TAG` | `--synced-tag` | Tag for synced torrents (empty to disable) | `synced` |
| `QBSYNC_DESTINATION_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
| `QBSYNC_DESTINATION_LOG_LEVEL` | `--log-level` | Log level: debug, info, warn, error | `info` |
| `QBSYNC_DESTINATION_DRY_RUN` | `--dry-run` | Run without making changes | `false` |

### Conventional Variables

These standard variables are also supported as fallbacks:

| Variable | Used For |
|----------|----------|
| `HTTP_PORT` / `HEALTH_PORT` | Health/metrics endpoint |
| `GRPC_PORT` / `PORT` | Destination server listen address |

## Health & Metrics

Both servers expose HTTP endpoints on the health address:

| Endpoint | Description |
|----------|-------------|
| `/healthz` | Basic liveness check |
| `/livez` | Liveness with dependency checks |
| `/readyz` | Readiness for traffic |
| `/metrics` | Prometheus metrics |

## Kubernetes Drain

When the source server receives SIGTERM, it can drain fully-synced torrents from the source qBittorrent before exiting. This is useful during node maintenance to migrate workloads off a node gracefully.

Drain is gated by a pod annotation. On SIGTERM, the source server checks the annotation via the Kubernetes API:

- **Annotation set to `"true"`**: drain proceeds, removing fully-synced torrents from source
- **Annotation missing, `"false"`, or any other value**: drain is skipped
- **Annotation check fails** (K8s API unreachable): drain is skipped (fail-closed)
- **No `--drain-annotation` configured** (empty string): drain runs unconditionally on SIGTERM

### RBAC

The drain annotation check reads the pod's own metadata via the Kubernetes API. The service account needs `get` on `pods` in its namespace:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: qbsync-source
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: qbsync-source
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: qbsync-source
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: qbsync-source
subjects:
  - kind: ServiceAccount
    name: qbsync-source
```

### Pod spec

```yaml
metadata:
  annotations:
    qbsync/drain: "false"  # Set to "true" before node maintenance
spec:
  serviceAccountName: qbsync-source
  terminationGracePeriodSeconds: 600
  containers:
  - name: qbsync-source
    args: ["source", "--drain-annotation=qbsync/drain", "--drain-timeout=480"]
    env:
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
```

### Drain workflow

1. Set the annotation: `kubectl annotate pod <pod> qbsync/drain=true --overwrite`
2. Evict or delete the pod (SIGTERM is sent)
3. Source server checks the annotation, drains synced torrents, then exits

Ensure `terminationGracePeriodSeconds` exceeds `--drain-timeout` so the kubelet doesn't SIGKILL before drain completes.

## Docker Compose Example

```yaml
services:
  qbsync-source:
    image: ghcr.io/arsac/qb-sync:latest
    command: source
    environment:
      QBSYNC_SOURCE_DATA: /downloads
      QBSYNC_SOURCE_QB_URL: http://qbittorrent-source:8080
      QBSYNC_SOURCE_DESTINATION_ADDR: qbsync-destination:50051
    volumes:
      - /path/to/source/downloads:/downloads:ro

  qbsync-destination:
    image: ghcr.io/arsac/qb-sync:latest
    command: destination
    environment:
      QBSYNC_DESTINATION_DATA: /downloads
      QBSYNC_DESTINATION_QB_URL: http://qbittorrent-destination:8080
    volumes:
      - /path/to/destination/downloads:/downloads
```

## License

MIT
