# RudderStack Server - Development Guide

## Overview

RudderStack is an open-source Customer Data Platform (CDP) written in Go. The server handles event ingestion (Gateway), processing (Processor), routing to destinations (Router), and warehouse loading.

## Cursor Cloud specific instructions

### Services

| Service | Purpose | How to start |
|---------|---------|-------------|
| PostgreSQL | Primary database for job queues | `sudo pg_ctlcluster 16 main start` |
| Transformer | Event transformation (Node.js) | `sudo docker run --rm -p 9090:9090 rudderstack/rudder-transformer:latest` |
| rudder-server | The main Go application | See below |

### Running the server locally (file-based config, no cloud token needed)

```bash
JOBS_DB_HOST=localhost \
JOBS_DB_PORT=5432 \
JOBS_DB_USER=rudder \
JOBS_DB_PASSWORD=password \
JOBS_DB_DB_NAME=jobsdb \
JOBS_DB_SSL_MODE=disable \
DEST_TRANSFORM_URL=http://localhost:9090 \
RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true \
RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH=/tmp/workspaceConfig.json \
RSERVER_WAREHOUSE_MODE=off \
go run main.go
```

A sample workspace config for local dev is at `/tmp/workspaceConfig.json` (created during setup). Use write key `local-dev-write-key` for sending events.

### Key ports

- `8080` — Gateway (event ingestion API)
- `9090` — Transformer service

### Important caveats

- **golangci-lint version mismatch**: The project uses `go 1.26.3` but golangci-lint v2.9.0 (as pinned in Makefile) is built with go1.25. Use `go vet ./...` as a fallback for basic static analysis until a compatible golangci-lint release is available.
- **Docker socket permissions**: After starting dockerd, run `sudo chmod 666 /var/run/docker.sock` so integration tests that use `dockertest` can access it without root.
- **Integration tests use `dockertest`**: Many integration tests spin up containers (PostgreSQL, Redis, Kafka, etc.) programmatically. These require Docker to be running.
- **Unit tests**: Run with `make test` or `SLOW=0 go test ./path/to/package/...`. Use `make test package=<pkg>` for specific packages.
- **Build**: `make build` produces the `rudder-server` binary.
- **Lint/format**: See `make lint` and `make fmt` in Makefile.
- **Workspace config**: Set `RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true` to avoid needing a RudderStack cloud workspace token.

### Sending test events

```bash
curl -X POST http://localhost:8080/v1/track \
  -u "local-dev-write-key:" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user1","event":"Test Event","properties":{"key":"value"}}'
```

Supported endpoints: `/v1/track`, `/v1/identify`, `/v1/page`, `/v1/screen`, `/v1/batch`, `/v1/alias`, `/v1/group`.
