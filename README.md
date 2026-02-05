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
| `QBSYNC_HOT_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
| `QBSYNC_HOT_DRY_RUN` | `--dry-run` | Run without making changes | `false` |
| `QBSYNC_HOT_FORCE` | `--force` | Force sync regardless of space | `false` |

### Cold Server

| Variable | Flag | Description | Default |
|----------|------|-------------|---------|
| `QBSYNC_COLD_DATA` | `--data` | Data directory path | (required) |
| `QBSYNC_COLD_LISTEN` | `--listen` | gRPC listen address | `:50051` |
| `QBSYNC_COLD_QB_URL` | `--qb-url` | qBittorrent WebUI URL | |
| `QBSYNC_COLD_QB_USERNAME` | `--qb-username` | qBittorrent username | |
| `QBSYNC_COLD_QB_PASSWORD` | `--qb-password` | qBittorrent password | |
| `QBSYNC_COLD_POLL_INTERVAL` | `--poll-interval` | Verification poll interval (seconds) | `2` |
| `QBSYNC_COLD_POLL_TIMEOUT` | `--poll-timeout` | Verification timeout (seconds) | `300` |
| `QBSYNC_COLD_HEALTH_ADDR` | `--health-addr` | Health/metrics endpoint | `:8080` |
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
