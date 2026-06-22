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
- **supportedTables hard-codes a single table** and `CreateRunner` returns "Only
  tracked_users_reports is supported now" both before and after the branch
  (flusher/factory.go:26,60). MAR must extend `supportedTables` and add a parallel branch; the
  error strings will be stale unless updated.
- **`tracked_users` migrations have only an `.up.sql`** (no down migration), matching repo
  convention. Mirror that for MAR (forward-only).
- **HLL settings are configurable via `TrackedUsers.precision`/`registerWidth`** despite being a
  wire contract. They CAN be overridden at runtime, which is a footgun — overriding them breaks
  backend compatibility. MAR inherits the same risk if it exposes equivalent keys.
