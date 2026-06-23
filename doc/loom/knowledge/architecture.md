# Architecture

> High-level component relationships, data flow, and module dependencies.
> This file is append-only - agents add discoveries, never delete.

(Add architecture diagrams and component relationships as you discover them)

## MTU / Tracked-Users Reporting Pipeline (the clone source for MAR)

This codebase has a "tracked users" (a.k.a. MTU — Monthly Tracked Users) pipeline that
counts unique users per (workspace, source) using HyperLogLog (HLL) sketches, persists
them locally, then periodically aggregates and ships them to the rudderstack-reporting
service. The **MAR (Monthly Active Records)** feature is being built as a near-exact
**clone** of this pipeline with **three deltas: gate, key, query** (see patterns.md).

### End-to-end data flow

```text
Gateway (rETL record jobs carry source_id + destination_id in job.Parameters)
   │
   ▼
Processor.processJobsForDest  ── proc.trackedUsersReporter.GenerateReportsFromJobs(jobs, getNonEventStreamSources())
   │   (builds per-(workspace,source) HLL sketches from batch[0] userId/anonymousId/type)
   ▼
transformationMessage.trackedUsersReports  (carrier field, processor.go:2446)
   │
   ▼
Processor store txn ── proc.trackedUsersReporter.ReportUsers(ctx, reports, tx)
   │   (COPY into tracked_users_reports: workspace_id, instance_id, source_id, reported_at,
   │    userid_hll, anonymousid_hll, identified_anonymousid_hll — HLLs hex-encoded)
   ▼
Postgres table  tracked_users_reports  (reporting DB)
   │
   ▼
Reporting mediator (enterprise/reporting/mediator.go) registers a flusher CronRunner per table
   │
   ▼
Flusher CronRunner ── aggregator.Aggregate(start,end): reads rows, decodes HLLs,
   │   UNIONs them keyed by "workspace-source-instance", re-encodes to hex JSON
   ▼
client.New(RouteTrackedUsers).Send(...) ── POST to rudderstack-reporting /trackedUser
```

### Two reporter interfaces (do not confuse them)

- `trackedusers.UsersReporter` (enterprise/trackedusers/users_reporter.go:50-54) — full
  interface: `ReportUsers`, `GenerateReportsFromJobs`, **`MigrateDatabase`**.
- `processor.trackedUsersReporter` (processor/processor.go:98-101) — narrower, processor-local
  interface: only `ReportUsers` + `GenerateReportsFromJobs` (NO MigrateDatabase). The concrete
  `*UniqueUsersReporter` satisfies both; migration is driven by the app-handler, not the processor.

### HLL wire-compatibility contract (CRITICAL — see mistakes.md)

The HLL is built with `github.com/segmentio/go-hll`, hashing identifiers with
`murmur3.Sum64WithSeed(id, 123)`. Settings: **Log2m=16, Regwidth=5,
ExplicitThreshold=hll.AutoExplicitThreshold, SparseEnabled=true**
(users_reporter.go:67-72). These params + the murmur seed 123 must byte-for-byte match
rudderstack-reporting's Postgres `hll` extension, or the sketches the backend ingests are
corrupt/incompatible. The seed has the in-code comment "changing this will be non backwards
compatible" (users_reporter.go:33). MAR MUST reuse identical HLL params and seed.

### MAR's three deltas at a glance

1. **Gate** — a new fail-closed config flag (clone of `TrackedUsers.enabled`, default false).
2. **Key** — the aggregation grouping key (clone of `workspace-source-instance`,
   tracked_users_inapp.go:71).
3. **Query** — the client route / backend endpoint + the SQL table queried
   (clone of `RouteTrackedUsers="/trackedUser"` and table `tracked_users_reports`).

## App bootstrap layer (where MAR gets wired)

rudder-server boots through `app/` which selects a run mode and an app handler:

- **app/apphandlers/** — the two handlers that own feature setup, DB migration, and
  dependency injection into the processor: `processorAppHandler.go` (processor-only mode) and
  `embeddedAppHandler.go` (gateway+processor in one process). BOTH call
  `Features().TrackedUsers.Setup(config)` → `MigrateDatabase(...)` → inject the reporter into
  `proc.New(...)`. A cloned MAR feature must be wired into BOTH handlers or it silently does
  not run in one of the modes. This is the single most important integration surface for MAR.
- **app/cluster/** — cluster-mode coordination (multi-node / dynamic mode lifecycle:
  start/stop of gateway, processor, router as the node's mode changes). Not modified by MAR,
  but it is the layer that drives the processor's lifecycle (the processor holds the
  trackedUsersReporter that produces MAR/MTU reports), so it is useful context for how the
  reporter's owning component is started and stopped.

The reporting flusher (which ships the persisted reports to rudderstack-reporting) is started
separately via the reporting mediator's `DatabaseSyncer()`, not through app/cluster.

## MAR (Monthly Active Records) Pipeline — as implemented

MAR is a near-exact sibling of the MTU pipeline, added as a second independent reporter
in the tracked-users reporting infrastructure. Key differences:

### MAR-specific files

- `enterprise/activationrecords/records_reporter.go` — `UniqueActivationRecordsReporter`;
  table `activation_records_reports`; migrations dir `activation_records`.
- `enterprise/activationrecords/factory.go` — `Factory.Setup` returning noop when
  `ActivationRecords.enabled=false`.
- `enterprise/activationrecords/noop.go` — noop implementation returned when gate is off.
- `enterprise/reporting/flusher/aggregator/activation_records_inapp.go` —
  `ActivationRecordsInAppAggregator`; key = `workspace-source-destination-instance`;
  one HLL field (`fingerprint_hll`). Const `activationRecordsTableName` (not `tableName`).
- `enterprise/reporting/flusher/aggregator/activation_records_types.go` —
  `ActivationRecordsReport` struct with `MarshalJSON`.
- `enterprise/reporting/flusher/activation_records_test.go` — dockertest integration test.
- `enterprise/activationrecords/records_reporter_test.go` — unit test (Docker-free).
- `enterprise/activationrecords/wire_compat_test.go` — wire-compat test asserting HLL
  round-trip params (`Settings().Log2m==16`, `Regwidth==5`).
- `sql/migrations/activation_records/000001_init_schema.up.sql` — schema:
  `activation_records_reports` table + `reported_at` index.

### MAR vs MTU diff table

| Aspect | MTU (tracked-users) | MAR (activation-records) |
|--------|---------------------|--------------------------|
| Feature flag | `TrackedUsers.enabled` | `ActivationRecords.enabled` |
| Table | `tracked_users_reports` | `activation_records_reports` |
| Migration dir | `tracked_users` | `activation_records` |
| Migrations table | `tracked_users_reports_migrations` | `activation_records_reports_migrations` |
| Aggregation key | `workspace-source-instance` | `workspace-source-destination-instance` |
| HLL fields | `userid_hll`, `anonymousid_hll`, `identified_anonymousid_hll` | `fingerprint_hll` (one field) |
| Input identifier | `userId` / `anonymousId` / event `type` | `batch[0].context.activation.fingerprint` |
| Origin field | N/A | `batch[0].context.activation.origin` (carried column, NOT in key) |
| Backend route | `/trackedUser` | `/activationRecords` |
| Processor hook | `ReportUsers` | `ReportActivationRecords` |
| Carrier field | `transformationMessage.trackedUsersReports` | `transformationMessage.activationRecordsReports` |

### MAR aggregation key rationale

`destination_id` is in the key because an activation record is per destination — records for
(workspace, source, dest-A) must not be merged with (workspace, source, dest-B). `origin` is
NOT in the key (constant per source in MAR's spec); it is carried as a plain column.

### Wiring verified in integration-verify stage

Both `processorAppHandler.go` (:118-122) and `embeddedAppHandler.go` (:107-111) call
`Features().ActivationRecords.Setup` + `MigrateDatabase(...,"activation_records",...)`.
`processor.go` has both call sites: `GenerateReportsFromJobs` (:2370, no filter arg) and
`ReportActivationRecords` inside the store txn (:2917, `WithUpdateSafeTx`, same as
`ReportUsers` :2912, `tx.Tx()`). `mediator.go` (:22,:107) registers the runner;
`flusher/factory.go` (:21,:60-76) lists and branches the table; `client.go` (:38) has
`/activationRecords`.
