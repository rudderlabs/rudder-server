# Entry Points

> Key files agents should read first to understand the codebase.
> This file is append-only - agents add discoveries, never delete.

(Add entry points as you discover them)

## MTU / Tracked-Users Pipeline — Key Files (read in this order)

All paths relative to repo root /Volumes/sourcecode/rudderstack/rudder-server.
Line numbers verified 2026-06-22; treat as approximate anchors if code shifts.

### 1. The reporter (clone target) — enterprise/trackedusers/users_reporter.go  (package `trackedusers`)

- `const murmurSeed = 123` :34 — comment :33 "changing this will be non backwards compatible".
- `const trackUsersTable = "tracked_users_reports"` :36.
- `type UsersReport struct` :41-47 — WorkspaceID, SourceID, 3× *hll.Hll (UserID, AnonymousID, IdentifiedAnonymousID).
- `type UsersReporter interface` :50-54 — ReportUsers / GenerateReportsFromJobs / MigrateDatabase.
- `type UniqueUsersReporter struct` :56-62; constructor `NewUniqueUsersReporter` :64-79.
- **HLL settings literal** :67-72 — Log2m=`GetIntVar(16,1,"TrackedUsers.precision")`, Regwidth=`GetIntVar(5,1,"TrackedUsers.registerWidth")`, ExplicitThreshold=`hll.AutoExplicitThreshold`, SparseEnabled=`true`.
- `MigrateDatabase` :81-102 — opens reporting DB, MigrationsTable=`tracked_users_reports_migrations`, `m.Migrate("tracked_users")`.
- `GenerateReportsFromJobs` :104-190 — reads job.Parameters source_id + EventPayload `batch[0]` userId/anonymousId/type; builds nested map → []*UsersReport; alias-event dedup logic :152-169.
- `ReportUsers` :192-241 — pq COPY into tracked_users_reports; HLLs via `hllToString`.
- `hllToString` :244-253 — `hll.ToBytes()` → `hex.EncodeToString`.
- `recordIdentifier` :255-268 — `murmur3.Sum64WithSeed(id, murmurSeed)` → `hll.AddRaw`.
- `recordHllSizeStats` :270-292 — `tracked_users_hll_bytes` histogram.
- Imports: `github.com/segmentio/go-hll`, `github.com/spaolacci/murmur3`.

### 2. Processor hook sites — processor/processor.go

- `type trackedUsersReporter interface` :98-101 — ReportUsers + GenerateReportsFromJobs only (NO MigrateDatabase).
- Struct field `trackedUsersReporter trackedUsersReporter` :205; assigned in Setup() :475.
- **Generate hook** :2355-2357 — `proc.trackedUsersReporter.GenerateReportsFromJobs(preTrans.jobList, proc.getNonEventStreamSources())`.
- Carrier field `transformationMessage.trackedUsersReports []*trackedusers.UsersReport` :2446 (NOTE: not :2374).
- **Report-in-store-txn hook** :2881-2884 — `proc.trackedUsersReporter.ReportUsers(ctx, in.trackedUsersReports, tx.Tx())`.
- `nonEventStreamSources` populated :864-865 (non-empty, non-webhook category); accessor `getNonEventStreamSources()` :922-926. GENERIC filter — see mistakes.md (do NOT invert for MAR).

### 3. Manager injection — processor/manager.go

- Field `trackedUsersReporter trackedusers.UsersReporter` :49; New() param :123; struct assign :153; passed to Handle.Setup() :78.

### 4. Gateway — rETL record jobs carry source_id AND destination_id

- gateway/handle.go: `sourceID = arctx.SourceID` :304, `destinationID = arctx.DestinationID` :305; params map built :484-490 (`source_id` always), `destination_id` added when non-empty :491-493; record payload `SingularEventBatch{Batch,RequestIP,WriteKey,ReceivedAt}` :516-532 (events in `batch`).
- gateway/handle_http_auth.go: `arctx.DestinationID = destinationID` :173 inside `sourceDestIDAuth` middleware; rETL route wired via `webRetlHandler()` (gateway/handle_http_retl.go) = `callType("retl", sourceDestIDAuth(webHandler()))`.
- Conclusion: for an rETL record job, BOTH `source_id` and `destination_id` are present in `job.Parameters` (destination_id guaranteed because sourceDestIDAuth validates the X-Rudder-Destination-Id header).

### 5. Flush path — enterprise/reporting/

- mediator.go: `const TrackedUsersReportsTable = "tracked_users_reports"` :21; runner registration `flusher.CreateRunner(...)` :99-105 appended to `rm.cronRunners`; started in `DatabaseSyncer()` :116-119.
- flusher/factory.go: `var supportedTables = []string{"tracked_users_reports"}` :21; `CreateRunner` :24; tracked_users branch :41-58 — gate `GetBoolVar(false,"TrackedUsers.enabled")` :43 (NOPCronRunner if off), `client.New(client.RouteTrackedUsers,...)` :47, `aggregator.NewTrackedUsersInAppAggregator(...)` :49, `NewFlusher` :50, `NewCronRunner` :55.
- flusher/aggregator/tracked_users_inapp.go: struct :18, ctor `NewTrackedUsersInAppAggregator` :28, `Aggregate(ctx,start,end)` :40-97; **aggregation KEY** :71 `k := fmt.Sprintf("%s-%s-%s", r.WorkspaceID, r.SourceID, r.InstanceID)`; HLL `Union` :74-76.
- flusher/aggregator/types.go: `type TrackedUsersReport struct` :12-23 (HLL fields `json:"-"`, hex fields exported); `MarshalJSON` :25-36 hex-encodes the 3 HLLs.
- client/client.go: route consts :34-38 — `RouteMetrics`, `RouteRecordErrors`, `RouteTrackedUsers="/trackedUser"`.

### 6. Feature factory + app wiring

- enterprise/trackedusers/factory.go: `Factory.Setup(conf)` :13-18 — `if !GetBoolVar(false,"TrackedUsers.enabled") return NewNoopDataCollector()`, else `NewUniqueUsersReporter`.
- app/app.go: `TrackedUsers: &trackedusers.Factory{Log: ...}` :91-93.
- app/features.go: `Features` struct field `TrackedUsers TrackedUsersFeature` :43; `type TrackedUsersFeature interface { Setup(c *config.Config) (trackedusers.UsersReporter, error) }` :46-48.
- app/apphandlers/processorAppHandler.go: Setup + MigrateDatabase :109-116; reporter injected into `proc.New(...)` :354.
- app/apphandlers/embeddedAppHandler.go: Setup + MigrateDatabase :98-105; injected :358.

### 7. Migrations embedding

- sql/migrations/embed.go: `//go:embed **/*.tmpl` + `//go:embed **/*.sql`; `var FS embed.FS`. Recursive — a new subdir's *.sql is auto-embedded, no code change.
- services/sql-migrator/migrator.go: `Migrate(migrationsDir)` :51+ uses `iofs.New(migrations.FS, migrationsDir)`; dir string (e.g. "tracked_users") maps to `sql/migrations/<dir>/`.
- Existing subdirs include: tracked_users, reports, reports_always, error_detail_reports, jobsdb, warehouse, … (sql/migrations/tracked_users/000001_init_schema.up.sql defines tracked_users_reports).

### 8. Integration test pattern

- enterprise/reporting/flusher/tracked_users_test.go: NO build tag; uses `dockertest.NewPool("")` + `postgres.Setup(pool, t)` (rudder-go-kit testhelper) to spin a Postgres container; sets `TrackedUsers.enabled=true` and DB.* config; mock reporting endpoint via webhook recorder. Requires Docker daemon.
