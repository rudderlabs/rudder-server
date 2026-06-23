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

## End-to-end recipe: clone an HLL metering reporter (MAR as the worked example)

This recipe walks through every file needed to add a second HLL-based metering reporter
alongside tracked-users. Follow in this order to avoid build failures at each step.

### Step 0 — understand the three-delta constraint

Only three things differ from MTU; everything else is verbatim:
1. **Gate key** — `ActivationRecords.enabled` (default `false`)
2. **Aggregation key** — `workspace-source-destination-instance`
3. **Backend route + SQL table** — `/activationRecords` + `activation_records_reports`

HLL params, murmur seed, serialization, flusher lifecycle, migration embedding — ALL unchanged.

### Step 1 — enterprise/<feature>/ (reporter package)

Files: `records_reporter.go`, `factory.go`, `noop.go`
- Define `ActivationRecordsReporter` interface: `GenerateReportsFromJobs`, `ReportActivationRecords`, `MigrateDatabase`.
- `UniqueActivationRecordsReporter`: identical HLL settings (Log2m=16, Regwidth=5, AutoExplicitThreshold, SparseEnabled=true), same murmur seed=123.
- `GenerateReportsFromJobs`: groups by `recordKey{workspaceID,sourceID,destinationID}`; `origin` is carried in the accumulator (NOT in the key). jsonparser array path uses bracket notation `"[0]"`.
- `ReportActivationRecords`: `pq.CopyIn(activationRecordsTable, "workspace_id","instance_id","source_id","destination_id","origin","reported_at","fingerprint_hll")`.
- `MigrateDatabase`: `migrator.Migrator{MigrationsTable:"activation_records_reports_migrations"}.Migrate("activation_records")`.
- `Factory.Setup`: returns noop if `!conf.GetBoolVar(false, "ActivationRecords.enabled")`.
- Tests: unit test (Docker-free, mirrors `trackedusers` GenerateReportsFromJobs subtest) + wire-compat test asserting `hll.FromBytes(serialized).Settings().Log2m==16` and `Regwidth==5`.

### Step 2 — sql/migrations/<feature>/000001_init_schema.up.sql

No embed change — the recursive `//go:embed **/*.sql` in `sql/migrations/embed.go` picks it up
automatically. Forward-only (no `.down`). Include `destination_id` + `origin` columns if the
grain includes them.

### Step 3 — enterprise/reporting/flusher/aggregator/

Files: `activation_records_inapp.go`, `activation_records_types.go`
- Use UNIQUE names in the package: `activationRecordsTableName` (not `tableName`), `marshalActivationRecordsReports` (not `marshalReports`). Check sibling files first.
- Aggregation key includes `destination_id`: `fmt.Sprintf("%s-%s-%s-%s", workspaceID, sourceID, destinationID, instanceID)`.
- `ActivationRecordsReport` struct: `FingerprintHLL *hll.Hll \`json:"-"\`` + `FingerprintHLLHex string`; `MarshalJSON` hex-encodes before marshalling.

### Step 4 — enterprise/reporting/client/client.go

Add route const: `RouteActivationRecords Route = "/activationRecords"`.

### Step 5 — enterprise/reporting/flusher/factory.go

- Extend `supportedTables` slice to include `"activation_records_reports"`.
- Add a new branch gated by `conf.GetBoolVar(false, "ActivationRecords.enabled")`.
- Update error string to name both supported tables.

### Step 6 — enterprise/reporting/mediator.go

- Add table const `ActivationRecordsReportsTable = "activation_records_reports"`.
- Call `flusher.CreateRunner(rm.ctx, ActivationRecordsReportsTable, ...)` and handle the error.

### Step 7 — processor/processor.go

- Add processor-local interface (only `GenerateReportsFromJobs` + `ReportActivationRecords`, NO `MigrateDatabase`).
- Add struct field + Setup assignment.
- **Generate hook** (near MTU's hook at ~:2370): call `GenerateReportsFromJobs(preTrans.jobList)` with NO source filter.
- **Carrier field** on `transformationMessage` (like `trackedUsersReports`). NOTE: the field must also be added to `userTransformData` and `storeMessage` (three structs total — trace to the actual report-call receiver).
- **Store-txn hook** (near MTU's `ReportUsers` at ~:2917): call `ReportActivationRecords(ctx, in.activationRecordsReports, tx.Tx())`.

### Step 8 — processor/manager.go

- Add field, `New()` param, struct assign, `Setup()` pass-through (mirrors `trackedUsersReporter`).
- After changing the signature, run `rg 'manager\.New\(' --type go` repo-wide and update ALL callers, including `processor/manager_test.go` and `app/cluster/integration_test.go`. Pass `activationrecords.NewNoopActivationRecordsReporter()` at test-only sites.

### Step 9 — app/features.go + app/app.go

- Add `ActivationRecords ActivationRecordsFeature` field to `Features` struct.
- Define `type ActivationRecordsFeature interface { Setup(*config.Config) (activationrecords.ActivationRecordsReporter, error) }`.
- In `app.go` `initFeatures`: `ActivationRecords: &activationrecords.Factory{Log: ...}`.

### Step 10 — app/apphandlers/processorAppHandler.go AND embeddedAppHandler.go (BOTH)

Mirror the `TrackedUsers` block exactly: `Setup` → check error → `MigrateDatabase` → inject into `proc.New(...)`. Missing one handler silently disables MAR in that deployment mode.

### Step 11 — integration test (enterprise/reporting/flusher/activation_records_test.go)

Use UNIQUE helper names vs `tracked_users_test.go` (same `flusher_test` package). Check existing helper names first: `rg 'func [a-z]' enterprise/reporting/flusher/tracked_users_test.go`.

## `origin` is carried, never keyed — why

`origin` is sourced from `batch[0].context.activation.origin`. Per MAR's spec it is constant
for a given source, so including it in the aggregation key would partition the HLL bucket for
no gain and risk accidental grain splits if different jobs see different origin strings.
Carry it as a column; do NOT add it to the grouping key.
