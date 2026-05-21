# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is **rudder-server**, the core backend of the RudderStack open-source Customer Data Platform. It's a Go monolith that handles event ingestion (Gateway), processing (Processor), routing (Router/BatchRouter), and warehouse loading. See `README.md` for product context.

### Required services

| Service | How to start | Port |
|---|---|---|
| PostgreSQL 15 | `docker run -d --name rudder-postgres -p 6432:5432 -e POSTGRES_USER=rudder -e POSTGRES_PASSWORD=password -e POSTGRES_DB=jobsdb --shm-size=128mb postgres:15-alpine` | 6432 |
| RudderStack Transformer | `docker run -d --name rudder-transformer -p 9090:9090 rudderstack/rudder-transformer:latest` | 9090 |

### Running the server locally (without RudderStack Cloud)

Use `RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true` with a local `workspaceConfig.json` to run without a `WORKSPACE_TOKEN`. Minimal env vars:

```sh
JOBS_DB_HOST=localhost JOBS_DB_USER=rudder JOBS_DB_PASSWORD=password \
JOBS_DB_PORT=6432 JOBS_DB_DB_NAME=jobsdb JOBS_DB_SSL_MODE=disable \
DEST_TRANSFORM_URL=http://localhost:9090 \
RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true \
RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH=/path/to/workspaceConfig.json \
go run main.go
```

A workspace config template is at `integration_test/docker_test/testdata/workspaceConfigTemplate.json`.

### Build, lint, test commands

All in `Makefile`:
- **Build**: `make build` (or `go build`)
- **Lint**: `golangci-lint run -v` (install binary via `curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin`; do **not** use `go run` as it may use an older Go toolchain)
- **Format**: `make fmt`
- **Unit tests**: `make test` (uses `gotestsum`, install via `make install-tools`)
- **Test single package**: `gotestsum --format pkgname-and-test-fails -- -v ./path/to/package/...`

### Gotchas

- **golangci-lint toolchain mismatch**: The Makefile uses `go run $(GOLANGCI)` which builds golangci-lint with Go 1.25 (its go.mod version), but the project requires Go 1.26.3. Install the **binary** release instead of using `go run`.
- **Docker rate limits**: Integration tests using `dockertest` may fail with 429 errors from Docker Hub. This is an environment limitation, not a code issue.
- **PostgreSQL port**: The `docker-compose.yml` and local dev convention maps PostgreSQL to host port **6432** (not 5432).
- **PATH**: Go tool binaries are installed to `$(go env GOPATH)/bin` (typically `~/go/bin`). Ensure this is in `$PATH`.
- The devtool at `cmd/devtool` provides helpful commands: `event send` to send test events, `webhook run` to simulate a destination.
