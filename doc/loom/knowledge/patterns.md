# Architectural Patterns

> Discovered patterns in the codebase that help agents understand how things work.
> This file is append-only - agents add discoveries, never delete.

(Add patterns as you discover them)

## Clone-with-three-deltas (MTU → MAR)

MAR is a near-verbatim clone of the tracked-users pipeline. Keep everything identical
EXCEPT three deltas; changing anything else risks breaking the HLL wire contract or
sweeping in the wrong sources.

1. **Gate** — a new fail-closed config flag. Clone the `TrackedUsers.enabled` pattern:
   `conf.GetBoolVar(false, "<NewFeature>.enabled")` defaulting to **false** so the
   pipeline is OFF unless explicitly enabled. This gate appears in TWO places that must
   agree: the feature factory (trackedusers/factory.go:14 → returns a noop collector when
   off) and the flusher factory (flusher/factory.go:43 → returns `&NOPCronRunner{}` when
   off). MAR must gate both ends the same way.
2. **Key** — the aggregator grouping key (tracked_users_inapp.go:71,
   `fmt.Sprintf("%s-%s-%s", WorkspaceID, SourceID, InstanceID)`). MAR groups by whatever
   tuple MAR semantics require; HLLs sharing a key are `Union`-ed before shipping.
3. **Query** — the client route constant (client.go: `RouteTrackedUsers="/trackedUser"`)
   AND the SQL table queried in `Aggregate` + written in `ReportUsers`/`MigrateDatabase`.

## HyperLogLog sketch + fixed murmur seed

Unique-count estimation uses `github.com/segmentio/go-hll`. Each identifier is hashed with
`murmur3.Sum64WithSeed([]byte(id), murmurSeed)` (seed=123) then `hll.AddRaw(hash)`. Sketches
are serialized to the DB and to the backend as **hex strings** (`hll.ToBytes()` →
`hex.EncodeToString`). Aggregation merges sketches with `hll.Hll.Union`. HLL settings
(Log2m=16, Regwidth=5, AutoExplicitThreshold, SparseEnabled=true) are a cross-service
contract — see mistakes.md. Reuse them verbatim for MAR.

## Feature factory + noop gate

Enterprise features follow: an `app.Features()` interface (app/features.go) → a `Factory`
with `Setup(conf) (Reporter, error)` (e.g. trackedusers/factory.go) that returns a **real**
reporter when enabled and a **noop** implementation when the gate is off. The app handlers
call `Features().X.Setup(config)` then `MigrateDatabase(...)`, then inject the reporter into
`proc.New(...)`. Both processorAppHandler.go and embeddedAppHandler.go must be wired
identically — miss one and the feature silently doesn't run in that deployment mode.

## transformationMessage carrier field

The processor generates reports early (GenerateReportsFromJobs) but persists them inside the
later store transaction (ReportUsers). The reports ride from generate→store on the
`transformationMessage.trackedUsersReports` field (processor.go:2446). A clone needs its own
carrier field; reusing the same field would conflate MTU and MAR data.

## Flusher = Aggregator + Client + CronRunner, registered per table

The reporting mediator registers one flusher `CronRunner` per supported table. A flusher is
composed of: an `Aggregator` (reads + HLL-unions local rows), a `client` bound to a backend
`Route`, and a `CronRunner` that periodically flushes. New pipeline = new table in
`supportedTables` + a new branch in `flusher.CreateRunner` + a new aggregator + a new route +
registration in mediator.go. `Reporting.flusher.batchSizeToReporting` controls batch size.

## Migrations auto-embed by directory

Add a migration set by creating `sql/migrations/<name>/000001_*.up.sql`. The recursive
`//go:embed **/*.sql` in sql/migrations/embed.go picks it up automatically (no code change to
the embed). Run it with `migrator.Migrator{MigrationsTable: "<name>_migrations"}.Migrate("<name>")`.
Each feature uses its OWN MigrationsTable so version tracking is independent.

## Integration tests via dockertest + postgres.Setup

Flusher/reporter integration tests spin a real Postgres with
`dockertest.NewPool("")` + `postgres.Setup(pool, t)` (rudder-go-kit testhelper), migrate,
seed rows, run the flusher, and assert payloads against a mock reporting endpoint. No build
tag gates these; they require a running Docker daemon.
