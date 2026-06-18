# AGENTS.md

## Cursor Cloud specific instructions

`rudder-server` is the open-source Go data-plane (event gateway + processor + router + warehouse).
Its only hard runtime dependency is **PostgreSQL** (the "JobsDB"). Standard commands live in the
`Makefile`, `README.md`, and `config/sample.env`; this section only captures the non-obvious bits for
running it inside the Cursor Cloud VM.

### Go toolchain
- `go.mod` targets `go 1.26.x`. The VM's base `go` is older, but `GOTOOLCHAIN=auto` (the default)
  auto-downloads the correct toolchain on first `go build`/`go run` — no manual Go install needed.
- The update script runs `go mod download` to warm the module cache.

### PostgreSQL (JobsDB)
- Postgres 16 is installed via apt and runs as a system service. Start/verify it with:
  - `sudo pg_ctlcluster 16 main start` (idempotent; ignore "already running")
  - A `rudder` superuser (password `rudder`) and a `jobsdb` database are pre-created.
  - Connect: `PGPASSWORD=rudder psql -h localhost -U rudder -d jobsdb`
- rudder-server auto-creates its tables (`gw_jobs_*`, `rt_jobs_*`, etc.) on startup.

### Running the server without the control plane (no WORKSPACE_TOKEN)
- The server normally pulls workspace config from the hosted control plane using `WORKSPACE_TOKEN`.
  For local dev you can instead load config **from a file** (no token needed):
  - `RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true`
  - `RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH=/path/to/workspaceConfig.json`
  - A working template is `integration_test/docker_test/testdata/workspaceConfigTemplate.json`.
- Required JobsDB env vars: `JOBS_DB_HOST/PORT/USER/PASSWORD/DB_NAME` (and the matching
  `WAREHOUSE_JOBS_DB_*`). Use `RSERVER_WAREHOUSE_MODE=off` to skip the warehouse module locally.
- Run the built binary or `make run` (`go run main.go`). The gateway listens on `:8080`;
  check `curl localhost:8080/health`.

### GOTCHA: the gateway blocks until the transformer responds
- On startup the gateway and processor **wait for the rudder-transformer `/features` endpoint**
  (`DEST_TRANSFORM_URL`, default `http://localhost:9090`). Until `/features` answers, the gateway
  does NOT open port 8080 (`WebHandler waiting for transformer feature before starting`).
- The real `rudder-transformer` is a separate service (its own repo / Docker image
  `rudderstack/rudder-transformer`) and is NOT in this workspace. Docker is not installed here.
- For a transformer-free smoke test, point `DEST_TRANSFORM_URL` at any stub that returns the
  feature JSON on `GET /features` (a 404 also works — the server falls back to default features).
  With a passthrough stub the full pipeline (gateway → JobsDB → processor → router → destination)
  runs end to end.
- Statsd (`:8125`) and diagnostics calls are best-effort; "connection refused" logs for statsd are
  harmless in local dev.

### Lint / test / build
- Build: `make build` (or `go build .`). First build downloads a large dependency tree.
- Unit test a package: `go test ./<pkg>/...`. The full `make test` suite is large and many
  integration tests need Docker (dockertest), so prefer scoping to the package you changed.
- Lint: `make lint` runs `golangci-lint` (pinned `v2.9.0` in the Makefile), `actionlint`, and
  security scanners. NOTE: the pinned `golangci-lint v2.9.0` is built against go1.25 and currently
  refuses to analyze this module because `go.mod` targets go1.26.x
  ("Go language version used to build golangci-lint is lower than the targeted Go version").
  Until the pin is bumped, use `go vet ./<pkg>/...` for static analysis. Also note `make lint`/`make fmt`
  rewrite files via `gofumpt`/`gci`/`go fix`.
