<!-- FOR AI AGENTS - Human readability is a side effect, not a goal -->
<!-- Managed by agent: keep sections and order; edit content, not structure -->
<!-- Last updated: 2026-08-18 | Last verified: 2026-08-18 -->

# AGENTS.md

**Precedence:** the **closest `AGENTS.md`** to the files you're changing wins. Root holds global defaults only.

RudderStack Server — open-source Customer Data Platform (CDP). Go 1.26.5 monolith: events flow Gateway → Processor →
Router/BatchRouter → 90+ destinations and warehouses. Segment API-compatible. Jobs are queued in PostgreSQL (`jobsdb`).

## Commands

> Source: Makefile + `.github/workflows/verify.yml` / `tests.yaml` — CI runs these exact targets

<!-- AGENTS-GENERATED:START commands -->

| Task                    | Command                                                       | Notes                                                                  |
|-------------------------|---------------------------------------------------------------|------------------------------------------------------------------------|
| Build                   | `make build`                                                  | Also builds `wait-for-go` and `regulation-worker`                      |
| Run                     | `make run` (`go run main.go`)                                 | `make run-mt` for multi-tenant via devtool+etcd                        |
| Format                  | `make fmt`                                                    | gofumpt + gci + matrixchecker; CI fails on `git diff` after this       |
| Lint                    | `make lint`                                                   | golangci-lint v2.9.0 + actionlint + `make sec` (gitleaks, govulncheck) |
| Test (all)              | `make test`                                                   | gotestsum, `-p=1 -failfast -shuffle=on`, 15m timeout                   |
| Test (one package)      | `make test package=gateway`                                   | Path relative to repo root; `exclude=` regex also supported            |
| Test (single test)      | `go test -count 1 -run TestName ./gateway/...`                |                                                                        |
| Warehouse integration   | `make test-warehouse package=warehouse/integrations/postgres` | `SLOW=1`, 30m timeout; needs Docker                                    |
| Regenerate mocks        | `make mocks`                                                  | `go generate ./...` via mockgen v0.6.0; CI diffs the result            |
| Regenerate protobuf     | `make proto`                                                  | Needs `protoc`; CI diffs the result                                    |
| Regenerate OpenAPI docs | `make generate-openapi-spec`                                  | Needs Docker; run after editing `gateway/openapi.yaml`                 |

<!-- AGENTS-GENERATED:END commands -->

> If commands fail, verify against the Makefile or ask the user to update this file.

## Response Style

- Answer first, elaborate only if needed. No sycophantic openers.
- For yes/no or status questions, lead with the answer.
- Skip preamble. Match response length to task complexity.

## Workflow

1. **Before coding**: Read the nearest `AGENTS.md`, then the README/docs for the subsystem you're touching (see File
   Map).
2. **After each change**: Run the smallest relevant check (`make fmt` → `go build ./<pkg>/...` → single test).
3. **Before committing**: Run `make lint` and tests for every package you touched; run `go mod tidy` if deps changed (CI
   diffs `go.mod`).
4. **Before claiming done**: Run verification and **show output as evidence** — never say "tested" or "all green"
   without pasted command output in the same turn.

## File Map

<!-- AGENTS-GENERATED:START filemap -->

```
main.go                     Entry point: signal handling, config init, launches runner/
runner/                     Bootstrap: stats init, service orchestration
app/                        App interface, enterprise feature registry
app/apphandlers/            Per-mode startup wiring (EMBEDDED / GATEWAY / PROCESSOR)
gateway/                    HTTP ingestion API (webhooks, auth, throttling, openapi.yaml)
processor/                  Core pipeline: transformations, enrichment, bot detection
router/                     Per-destination event delivery
router/batchrouter/         Batch + async destinations (see asyncdestinationmanager/README.md)
jobsdb/                     PostgreSQL-backed job queue; dataset rotation, migrations
warehouse/                  Warehouse loading; backends in warehouse/integrations/
backend-config/             Workspace config fetch/cache from control plane; pub-sub
services/                   Shared services: OAuth (see services/oauth/README.md), debuggers, rsources
enterprise/                 Enterprise-only: suppression, reporting, config env override
internal/                   Enrichers (geo, bot), drain-config, transformer client
cmd/                        CLI tools: devtool (see cmd/devtool/README.md), rudder-cli, backupfilemigrator
config/                     Config key constants and defaults
proto/                      Protobuf definitions (regenerate with make proto)
mocks/                      Generated mocks — never edit by hand, run make mocks
integration_test/           Docker-based end-to-end tests
testhelper/                 Reusable test fixtures: docker resources, backendconfigtest, transformertest
regulation-worker/          GDPR/regulation compliance worker (separate binary)
suppression-backup-service/ Suppression data backup/export (separate binary + Dockerfile)
sql/                        SQL migration files (golang-migrate) — never delete
```

<!-- AGENTS-GENERATED:END filemap -->

## Golden Samples (follow these patterns)

<!-- AGENTS-GENERATED:START golden-samples -->

| For                                      | Reference                                              |
|------------------------------------------|--------------------------------------------------------|
| Adding an async/batch destination        | `router/batchrouter/asyncdestinationmanager/README.md` |
| Warehouse staging pipeline changes       | `warehouse/.cursor/docs/staging-file-flow.md`          |
| OAuth destination flows                  | `services/oauth/README.md`                             |
| Gateway API contract                     | `gateway/openapi.yaml`                                 |
| Dev tooling (etcd modes, sending events) | `cmd/devtool/README.md`                                |

<!-- AGENTS-GENERATED:END golden-samples -->

## Utilities (check before creating new)

<!-- AGENTS-GENERATED:START utilities -->

| Need                                                 | Use                                                    | Location                                                                      |
|------------------------------------------------------|--------------------------------------------------------|-------------------------------------------------------------------------------|
| JSON marshal/unmarshal                               | `jsonrs`                                               | `github.com/rudderlabs/rudder-go-kit/jsonrs` (linter forbids `encoding/json`) |
| Config values (env/file/hot-reload)                  | `config`                                               | `github.com/rudderlabs/rudder-go-kit/config`                                  |
| Structured logging                                   | `logger` non-sugared (`Infon`, `Errorn`, typed fields) | `github.com/rudderlabs/rudder-go-kit/logger`                                  |
| Error fields in logs                                 | `obskit.Error(err)`                                    | `github.com/rudderlabs/rudder-observability-kit/go/labels`                    |
| Metrics                                              | `stats.NewTaggedStat`                                  | `github.com/rudderlabs/rudder-go-kit/stats`                                   |
| Goroutine lifecycle                                  | `errgroup`                                             | `golang.org/x/sync/errgroup`                                                  |
| Docker test resources (postgres, etcd, transformer…) | testhelper packages                                    | `testhelper/`, `github.com/rudderlabs/rudder-go-kit/testhelper/docker`        |

<!-- AGENTS-GENERATED:END utilities -->

## Heuristics (quick decisions)

<!-- AGENTS-GENERATED:START heuristics -->

| When                                   | Do                                                                                                                         |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Async assertion in tests               | `require.Eventually` — never `time.Sleep`; return bool only inside the callback                                            |
| Touching an interface with a mock      | Regenerate with `make mocks` and commit the diff                                                                           |
| Editing `proto/**/*.proto`             | Run `make proto` and commit generated files                                                                                |
| Editing `gateway/openapi.yaml`         | Validate + `make generate-openapi-spec`, commit output                                                                     |
| Adding a warehouse integration package | Add it to the `warehouse-integration` matrix in `.github/workflows/tests.yaml` (matrixchecker in `make fmt` enforces this) |
| Adding dependency                      | Ask first — then `go mod tidy` (CI diffs go.mod); no pseudo-version `rudderlabs` deps                                      |
| Unsure about pattern                   | Check Golden Samples above                                                                                                 |

<!-- AGENTS-GENERATED:END heuristics -->

## Repository Settings

<!-- AGENTS-GENERATED:START repo-settings -->

- Default branch: `master`; release branches: `release/*`. Releases via release-please.
- PR titles must be conventional commits (enforced by semantic-pr workflow). Allowed types:
  `fix feat chore refactor exp doc test`. Optional scopes:
  `core multi-tenant tooling gateway jobsdb warehouse processor router batchrouter destination startup shutdown ci`.
  Subject must not start with an uppercase letter.

<!-- AGENTS-GENERATED:END repo-settings -->

<!-- AGENTS-GENERATED:START ci-rules -->

### CI Quality Gates (`.github/workflows/verify.yml`, `tests.yaml`)

- `go mod tidy` must produce no diff.
- `make mocks`, `make proto`, `make fmt`, `make generate-openapi-spec` must each produce no diff.
- golangci-lint (`.golangci.yml`): depguard, forbidigo, gosec, bodyclose, nilerr, and more.
- Unit tests run per-package with coverage; integration tests run against Docker (oss + enterprise matrices).

<!-- AGENTS-GENERATED:END ci-rules -->

## Key Decisions

<!-- AGENTS-GENERATED:START key-decisions -->

- Job queue is PostgreSQL (`jobsdb`) with dataset rotation — not a message broker.
- One binary, three modes via `APP_TYPE` env: `EMBEDDED` (default), `GATEWAY`, `PROCESSOR` (`app/apphandlers/setup.go`).
- Enterprise features live in `enterprise/` behind `ENTERPRISE_TOKEN`; OSS build must keep working without it.
- All JSON goes through `jsonrs` (pluggable implementation), enforced by linter.

<!-- AGENTS-GENERATED:END key-decisions -->

## Boundaries

### Always Do

- Run `make fmt` and relevant tests before committing
- Add tests for new code paths
- Use conventional commit format: `type(scope): subject` (lowercase subject)
- Use **atomic commits** (one logical change per commit)
- **Show test output as evidence before claiming work is complete**
- Wrap errors with context: `fmt.Errorf("starting server: %w", err)` — active voice, no "failed to"
- Pass `context.Context` as the first parameter; propagate cancellation

### Ask First

- Adding new dependencies
- Modifying CI/CD configuration (`.github/workflows/`)
- Changing public API contracts (`gateway/openapi.yaml`, `proto/`)
- Running full integration test suites (they need Docker and take 30m+)
- Repo-wide refactoring or rewrites

### Never Do

- Commit secrets or credentials (gitleaks runs in `make lint`)
- Hand-edit generated files: `mocks/`, protobuf `*.pb.go`, `gateway/openapi/`
- Push directly to `master` or `release/*` — open a PR
- Delete SQL migration files under `sql/`
- Use `encoding/json` (→ `jsonrs`), `github.com/gofrs/uuid` (→ `google/uuid`), `golang.org/x/exp/slices` (→ stdlib
  `slices`), `aws-sdk-go` v1 (→ v2), `cenkalti/backoff` < v5 (→ v5) — all linter-enforced
- Use sugared logger methods (`Logger.Info`, `Logger.Errorf`, …) — use `Infon`/`Errorn` with typed field constructors
  and `obskit.Error(err)`

## Contributing (for AI agents)

- **Comprehension**: Understand *why* a change is needed before submitting code; read the linked issue.
- **Context**: Explain trade-offs in the PR description and link the issue it addresses (see `CONTRIBUTING.md`).
- **Continuity**: Respond to review feedback; drive-by PRs without follow-up will be closed.

## Terminology

| Term          | Means                                                                                         |
|---------------|-----------------------------------------------------------------------------------------------|
| Gateway       | HTTP ingestion service; writes raw events to jobsdb                                           |
| Processor     | Transforms/enriches events; routes them to router or batchrouter queues                       |
| Router        | Streams events to cloud destinations one-by-one with retries                                  |
| Batch router  | Batches events for warehouses and async destinations                                          |
| jobsdb        | PostgreSQL-backed append-only job queue with dataset rotation                                 |
| Transformer   | Separate JS service applying destination/user transformations (`internal/transformer-client`) |
| Control plane | RudderStack hosted config service (`CONFIG_BACKEND_URL`); `backend-config/` syncs from it     |
| Workspace     | Tenant unit; identified by `WORKSPACE_TOKEN`                                                  |
| rsources      | Job-status/failed-records service for sources (`services/rsources`)                           |
| RETL          | Reverse ETL — warehouse-to-destination pipelines                                              |

## When instructions conflict

The nearest `AGENTS.md` wins. Explicit user prompts override files.
