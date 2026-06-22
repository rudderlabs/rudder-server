# Coding Conventions

> Discovered coding conventions in the codebase.
> This file is append-only - agents add discoveries, never delete.

(Add conventions as you discover them)

## Naming & config conventions (tracked-users pipeline)

- **Config keys** are PascalCase, dotted, namespaced by feature: `TrackedUsers.enabled`,
  `TrackedUsers.precision`, `TrackedUsers.registerWidth`, `Reporting.flusher.maxOpenConnections`,
  `Reporting.flusher.batchSizeToReporting`, `SQLMigrator.forceSetLowerVersion`. Read via
  `conf.GetBoolVar(default, "Key")` / `conf.GetIntVar(default, min, "Key")` /
  `config.GetStringVar(default, "Key")`. Feature gates default to the OFF/safe value.
- **DB table** = `tracked_users_reports`; its **migrations table** = `tracked_users_reports_migrations`;
  its **migrations dir** = `sql/migrations/tracked_users/`. A new pipeline mirrors all three with a
  consistent feature stem.
- **Backend routes** are lowerCamel under client.go consts, value is the URL path:
  `RouteTrackedUsers Route = "/trackedUser"`.
- **HLL DB columns**: `userid_hll`, `anonymousid_hll`, `identified_anonymousid_hll` (hex text);
  metadata columns `workspace_id`, `instance_id`, `source_id`, `reported_at`.
- **JSON tags** on report structs: camelCase (`workspaceId`, `sourceId`, `userIdHLL`, …); the live
  `*hll.Hll` fields are `json:"-"` and a custom `MarshalJSON` hex-encodes them into exported hex fields.

## Code style / structure

- Enterprise features live under `enterprise/<feature>/`; reporting flush machinery under
  `enterprise/reporting/{client,flusher,flusher/aggregator}`.
- Constructors are `New<Type>(...)`; interfaces named `<Thing>er` (UsersReporter, Runner, Aggregator).
- Logging: `logger.Logger` with structured `obskit` fields (`obskit.Error(err)`), `Errorn/Infon` (n = structured).
- Stats: `stats.Stats` with `NewTaggedStat(name, type, tags)`; DB stats via
  `collectors.NewDatabaseSQLStats(name, db)` registered through `stats.RegisterCollector`.
- Errors wrapped with `fmt.Errorf("...: %w", err)`.
- Time injected as `now func() time.Time` (defaults to `timeutil.Now()`) for testability.
- Native tooling in this repo: `make build`, `make test`, `make test-it`, `make lint`; mocks via `make mocks` (gomock).

## Wiring checklist for a cloned pipeline (must touch ALL)

1. enterprise/<feature>/ reporter + factory (+ noop collector).
2. processor.go: interface, struct field, Setup() assignment, generate hook, carrier field, store-txn hook.
3. processor/manager.go: field, New() param, struct assign, Setup() pass-through.
4. app/features.go: Features field + Feature interface.
5. app/app.go: Factory init in initFeatures.
6. app/apphandlers/processorAppHandler.go AND embeddedAppHandler.go: Setup + MigrateDatabase + proc.New injection (BOTH).
7. enterprise/reporting/mediator.go: table const + CreateRunner registration.
8. enterprise/reporting/flusher/factory.go: supportedTables + table branch.
9. enterprise/reporting/flusher/aggregator/: new aggregator (with MAR key).
10. enterprise/reporting/client/client.go: new Route const.
11. sql/migrations/<feature>/000001_init_schema.up.sql.
12. integration test mirroring tracked_users_test.go.
