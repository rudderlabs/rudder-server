# Concerns & Technical Debt

> Technical debt, warnings, issues, and improvements needed.
> This file is append-only - agents add discoveries, never delete.

(Add concerns as you discover them)

## Observations / tech-debt around the clone area

- **mediator.go panics on flusher creation error** (`enterprise/reporting/mediator.go` ~:101,
  with an in-code `// TODO: Should we panic here?`). A MAR clone that follows this pattern will
  also panic the process if its flusher fails to construct. Consider whether MAR should degrade
  gracefully instead.
- **Duplicate gate logic** lives in two factories (trackedusers/factory.go and
  flusher/factory.go). The same `<Feature>.enabled` key must be kept in sync by hand; there is
  no shared constant. Easy to drift — see mistakes.md.
- **Two parallel app handlers** (processorAppHandler.go, embeddedAppHandler.go) duplicate the
  Setup→MigrateDatabase→inject sequence. Any new feature must be added to BOTH; nothing enforces it.
- **[RESOLVED in MAR implementation]** `supportedTables` now lists both
  `"tracked_users_reports"` and `"activation_records_reports"`; both error strings updated to
  name both tables (flusher/factory.go). The pattern of hard-coding supported tables remains —
  adding a third pipeline requires extending the slice and error strings again.
- **`tracked_users` migrations have only an `.up.sql`** (no down migration), matching repo
  convention. Mirror that for MAR (forward-only).
- **HLL settings are configurable via `TrackedUsers.precision`/`registerWidth`** despite being a
  wire contract. They CAN be overridden at runtime, which is a footgun — overriding them breaks
  backend compatibility. MAR inherits the same risk if it exposes equivalent keys.

## loom acceptance timeout vs dockertest integration suites in ./processor/...

The `./processor/...` glob includes 6+ dockertest integration suites (each spinning Postgres
containers), taking ~344s total. If loom's built-in acceptance command timeout is shorter than
that, `loom stage complete` will report `✗ TIMEOUT` on `go test ./processor/... -count=1` even
though the criterion passes when run with `-timeout 1800s`. Future plans should scope the
processor acceptance criterion to `-run '^TestProcessor$'` (Ginkgo unit suite, ~5s) for the
fast MAR/MTU regression signal, and rely on CI for the full integration suite.
