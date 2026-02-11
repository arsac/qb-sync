# qb-sync

Sync torrents between qBittorrent instances. Stream pieces from a hot (source) server to a cold (destination) server in real-time as they download.

## Installation

```bash
go install github.com/arsac/qb-sync/cmd/qbsync@latest
```

Or with Docker:

```bash
docker pull ghcr.io/arsac/qb-sync:latest
```

## Usage

### Hot Server (Source)

Streams pieces from source qBittorrent to cold server:

```bash
qbsync hot \
  --data /downloads \
  --qb-url http://localhost:8080 \
  --cold-addr 192.168.1.100:50051
```

### Cold Server (Destination)

Receives pieces and adds verified torrents to destination qBittorrent:

```bash
qbsync cold \
  --data /downloads \
  --qb-url http://localhost:8080 \
  --listen :50051
```

## Environment Variables

All flags can be set via environment variables with the prefix `QBSYNC_HOT_` or `QBSYNC_COLD_`.

### Hot Server

| Variable | Flag | Description | Default |
|----------|------|-------------|---------|
| `QBSYNC_HOT_DATA` | `--data` | Data directory path | (required) |
| `QBSYNC_HOT_QB_URL` | `--qb-url` | qBittorrent WebUI URL | (required) |
| `QBSYNC_HOT_QB_USERNAME` | `--qb-username` | qBittorrent username | |
| `QBSYNC_HOT_QB_PASSWORD` | `--qb-password` | qBittorrent password | |
| `QBSYNC_HOT_COLD_ADDR` | `--cold-addr` | Cold server gRPC address | (required) |
| `QBSYNC_HOT_MIN_SPACE` | `--min-space` | Min free space (GB) before syncing | `50` |
| `QBSYNC_HOT_MIN_SEEDING_TIME` | `--min-seeding-time` | Min seeding time (seconds) | `3600` |
| `QBSYNC_HOT_SLEEP` | `--sleep` | Sleep interval between checks (seconds) | `30` |
| `QBSYNC_HOT_RATE_LIMIT` | `--rate-limit` | Max bytes/sec (0 = unlimited) | `0` |
| `QBSYNC_HOT_PIECE_TIMEOUT` | `--piece-timeout` | Timeout for stale in-flight pieces (seconds) | `60` |
| `QBSYNC_HOT_RECONNECT_MAX_DELAY` | `--reconnect-max-delay` | Max reconnect backoff delay (seconds) | `30` |
| `QBSYNC_HOT_NUM_SENDERS` | `--num-senders` | Concurrent sender workers | `4` |
| `QBSYNC_HOT_MIN_CONNECTIONS` | `--min-connections` | Minimum TCP connections to cold server | `2` |
| `QBSYNC_HOT_MAX_CONNECTIONS` | `--max-connections` | Maximum TCP connections to cold server | `8` |
| `QBSYNC_HOT_SYNCED_TAG` | `--synced-tag` | Tag for synced torrents (empty to disable) | `synced` |
| `QBSYNC_HOT_SOURCE_REMOVED_TAG` | `--source-removed-tag` | Tag on cold when source removed (empty to disable) | `source-removed` |
| `QBSYNC_HOT_EXCLUDE_CLEANUP_TAG` | `--exclude-cleanup-tag` | Tag that prevents cleanup from hot (empty to disable) | |
| `QBSYNC_HOT_DRAIN_ANNOTATION` | `--drain-annotation` | Pod annotation key to gate shutdown drain (empty to drain unconditionally) | `qbsync/drain` |
| `QBSYNC_HOT_DRAIN_TIMEOUT` | `--drain-timeout` | Shutdown drain timeout (seconds) | `300` |
| `QBSYNC_HOT_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
| `QBSYNC_HOT_LOG_LEVEL` | `--log-level` | Log level: debug, info, warn, error | `info` |
| `QBSYNC_HOT_DRY_RUN` | `--dry-run` | Run without making changes | `false` |

### Cold Server

| Variable | Flag | Description | Default |
|----------|------|-------------|---------|
| `QBSYNC_COLD_DATA` | `--data` | Data directory path | (required) |
| `QBSYNC_COLD_LISTEN` | `--listen` | gRPC listen address | `:50051` |
| `QBSYNC_COLD_SAVE_PATH` | `--save-path` | Save path as qBittorrent sees it | (defaults to `--data`) |
| `QBSYNC_COLD_QB_URL` | `--qb-url` | qBittorrent WebUI URL | |
| `QBSYNC_COLD_QB_USERNAME` | `--qb-username` | qBittorrent username | |
| `QBSYNC_COLD_QB_PASSWORD` | `--qb-password` | qBittorrent password | |
| `QBSYNC_COLD_POLL_INTERVAL` | `--poll-interval` | Verification poll interval (seconds) | `2` |
| `QBSYNC_COLD_POLL_TIMEOUT` | `--poll-timeout` | Verification timeout (seconds) | `300` |
| `QBSYNC_COLD_STREAM_WORKERS` | `--stream-workers` | Concurrent piece writers (0 = auto: 8) | `0` |
| `QBSYNC_COLD_MAX_STREAM_BUFFER` | `--max-stream-buffer` | Global memory budget for buffered pieces (MB) | `512` |
| `QBSYNC_COLD_SYNCED_TAG` | `--synced-tag` | Tag for synced torrents (empty to disable) | `synced` |
| `QBSYNC_COLD_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
| `QBSYNC_COLD_LOG_LEVEL` | `--log-level` | Log level: debug, info, warn, error | `info` |
| `QBSYNC_COLD_DRY_RUN` | `--dry-run` | Run without making changes | `false` |

### Conventional Variables

These standard variables are also supported as fallbacks:

| Variable | Used For |
|----------|----------|
| `HTTP_PORT` / `HEALTH_PORT` | Health/metrics endpoint |
| `GRPC_PORT` / `PORT` | Cold server listen address |

## Health & Metrics

Both servers expose HTTP endpoints on the health address:

| Endpoint | Description |
|----------|-------------|
| `/healthz` | Basic liveness check |
| `/livez` | Liveness with dependency checks |
| `/readyz` | Readiness for traffic |
| `/metrics` | Prometheus metrics |

## Kubernetes Drain

When the hot server receives SIGTERM, it can drain fully-synced torrents from the source qBittorrent before exiting. This is useful during node maintenance to migrate workloads off a node gracefully.

Drain is gated by a pod annotation. On SIGTERM, the hot server checks the annotation via the Kubernetes API:

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
  name: qbsync-hot
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: qbsync-hot
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: qbsync-hot
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: qbsync-hot
subjects:
  - kind: ServiceAccount
    name: qbsync-hot
```

### Pod spec

```yaml
metadata:
  annotations:
    qbsync/drain: "false"  # Set to "true" before node maintenance
spec:
  serviceAccountName: qbsync-hot
  terminationGracePeriodSeconds: 600
  containers:
  - name: qbsync-hot
    args: ["hot", "--drain-annotation=qbsync/drain", "--drain-timeout=480"]
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
3. Hot server checks the annotation, drains synced torrents, then exits

Ensure `terminationGracePeriodSeconds` exceeds `--drain-timeout` so the kubelet doesn't SIGKILL before drain completes.

## Docker Compose Example

```yaml
services:
  qbsync-hot:
    image: ghcr.io/arsac/qb-sync:latest
    command: hot
    environment:
      QBSYNC_HOT_DATA: /downloads
      QBSYNC_HOT_QB_URL: http://qbittorrent-hot:8080
      QBSYNC_HOT_COLD_ADDR: qbsync-cold:50051
    volumes:
      - /path/to/hot/downloads:/downloads:ro

  qbsync-cold:
    image: ghcr.io/arsac/qb-sync:latest
    command: cold
    environment:
      QBSYNC_COLD_DATA: /downloads
      QBSYNC_COLD_QB_URL: http://qbittorrent-cold:8080
    volumes:
      - /path/to/cold/downloads:/downloads
```

## License

MIT
