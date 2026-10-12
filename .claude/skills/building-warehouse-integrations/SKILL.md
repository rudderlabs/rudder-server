---
name: building-warehouse-integrations
description: Use when adding a new warehouse destination under warehouse/integrations/, writing a spec, design or plan for one, or changing an existing warehouse's error mappings, load path (COPY/MERGE/INSERT), Cleanup, data type maps, connection or object-storage setup, or its integration tests.
---

# Building warehouse integrations

## Overview

Every warehouse in `warehouse/integrations/` honours the same contract with the router, the stats pipeline and the shared libraries. Nothing enforces it at compile time, so a new integration can pass its sqlmock tests while misclassifying every failure, reporting zero rows and leaking staging tables. Check each item below against an existing integration before calling the work done, and before a spec or plan describes how the integration will work.

**Same as the siblings unless there's a reason.** Anything the integration does differently from the other warehouses gets its reason written down: in a code comment, and in the spec's deviations table if there is a spec. Don't add a mechanism no sibling has (a "precaution", a re-validation of locations rudder-server wrote itself, a duplicate safety check, a guard for a state the caller already rules out) unless a requirement or an observed failure needs it.

A sibling dialect is not a spec. Copying MSSQL into a T-SQL warehouse, or Postgres into a Postgres-wire one, carries over types, functions and defaults the new provider may not support. Confirm provider behaviour against its docs or a live instance.

## The contract

| # | Area | Requirement | Reference |
|---|------|-------------|-----------|
| 1 | Connection | Open the DB through an sqlconnect-go driver: marshal its config struct, `sqlconnect.NewDB("<driver>", json)`, `.SqlDB()`, wrap with `sqlmw.New`. Auth bootstraps (including ones that call a separate provider REST API before the SQL connect), token flows and default ports belong in the driver; if the driver is missing, add it to sqlconnect-go first and bump `go.mod`. Values fixed by the provider (SQL port, storage host) are constants, never read from destination config. | `snowflake.go` `connect`, `deltalake.go` `connect` |
| 2 | Object storage | Build file managers with rudder-go-kit `filemanager.New`. The destType → storage-provider mapping lives only in `warehouseutils.ObjectStorageType`. If go-kit lacks the provider, add it there first, even when the batch router and the warehouse slave also need it: they already call `filemanager.New`, so no resolver or factory gets threaded through them. | `warehouse/utils/utils.go` `ObjectStorageType` |
| 3 | `ErrorMappings()` | Each regex matches the **provider's own** error text or code. `MatchUploadJobErrorType` returns the **first** match (`warehouse/router/errors.go`), so order matters and broad patterns swallow everything after them. Cover auth/credential failures under `PermissionError`. Map only strings something can still emit. For failures this package or a shared library raises itself (HTTP calls, config checks), classify by status, never by a package prefix: 401/403 is `PermissionError`, and transient 429/5xx/network stays uncategorised. Test each mapping with a realistic provider error in the package test and `warehouse/router/errors_test.go`. Classification is reporting-only: an upload retries whatever its type until both `minRetryAttempts` and `retryTimeWindow` pass (`UploadJob.Aborted`), so no mapping makes an error non-retryable. Error text reaches the customer's sync history: keep provider error codes and correlation IDs, never response bodies or tokens. | `redshift.go`, `snowflake.go`, `deltalake.go` `errorsMappings` |
| 4 | `LoadTableStats` | Return `RowsInserted` / `RowsUpdated` from the `RowsAffected` of the COPY / INSERT / MERGE. They feed `post_load_table_rows` and `dedup_rows` (`warehouse/router/state_export_data.go`). Only the plain-file datalake returns `{}`. Return the stats as-is: loads are not transactional, so an error returned after the write commits makes the retry re-insert in append mode. | `snowflake.go`, `mssql.go` `loadTable` |
| 5 | Staging cleanup | `Cleanup` calls `dropDanglingStagingTables`, which finds tables by `warehouseutils.StagingTablePrefix(provider)`. Inline staging drops use `context.WithoutCancel(ctx)` so a cancelled or timed-out upload still drops them. | `mssql.go`, `redshift.go` `Cleanup` |
| 6 | Type maps | `dataTypesMap` (rudder → warehouse) must accept the load file's physical type (check the parquet/CSV writer mapping): a float column typed `decimal` rejects a Parquet DOUBLE. `dataTypesMapToRudder` lists only types the warehouse can store and that `INFORMATION_SCHEMA` returns. `fullEventColumnTypeByDestType` in the embedded transformer (it holds rudder logical types such as `model.JSONDataType`, not warehouse types) (`processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils/utils.go`) agrees with `dataTypesMap` on json vs string. If no producer emits a type, its handling in `dataTypesMap` and the parquet writer/loader is dead code: either make the transformer emit it or remove it. | any integration's `dataTypesMap` |
| 7 | Schema evolution | `AddColumns` is retry-safe: it adds only missing columns (`mssql.go` `IF NOT EXISTS`, `redshift.go` `CheckAndIgnoreColumnAlreadyExistError`), and a test calls it twice on the same table. Values that don't fit an existing column go to `rudder_discards` via the transformer, not to DDL; for `AlterColumn`, copy the closest sibling, which is a no-op in the T-SQL ones. Size string columns for the load path: siblings that insert rows client-side truncate to the column width (`azure-synapse.go` `getVarcharLengthMap`), but a server-side COPY can't, so one over-long value fails the whole table. | `mssql.go`, `redshift.go` `AddColumns` |
| 8 | SQL idioms | Quote with `warehouseutils.QuoteQualifiedIdentifier` / `SQLStringLiteral`. `QueryContext` never returns `sql.ErrNoRows` (only `QueryRow().Scan` does). A SQL fragment used by two statements that must agree (e.g. a MERGE join and the matching COUNT) is one helper. | `warehouse/utils/identifier.go` |
| 9 | Registration | `warehouse/utils` constant + `WarehouseDestinations`; `misc.BatchDestinations`; `warehouseutils.GetLoadFileType` and the writer map in `warehouse/encoding/parquetwriter.go` (if loading Parquet); both switches in `warehouse/integrations/manager/manager.go`; `warehouse/integrations/config`; the embedded-transformer map from #6; reserved keywords in `warehouse/utils/reservedkeywords.go` and the embedded transformer's `internal/reservedkeywords/reservedkeywords.go` (alias a sibling's set when the dialect matches); the `warehouse-integration` matrix in `.github/workflows/tests.yaml` (`make fmt` runs matrixchecker). | `AzureSynapse` / `azure-synapse` entries |
| 10 | Integration test | An end-to-end `integration_test.go` against the real warehouse, modelled on `redshift_test.go` / `snowflake_test.go`. Credentials come from `<DEST>_INTEGRATION_TEST_CREDENTIALS`; skip when unset. sqlmock unit tests do not replace it. | `warehouse/integrations/redshift/redshift_test.go` |

## The shared load shape

Specs and code both describe this flow; deviate only with a stated reason.

- **Destination config.** For non-OAuth destinations the control plane inlines account credentials into the destination config, so rudder-server reads them like any other key and needs no account handling. `DeliveryAccount` is read only on the OAuth path (`backend-config/oauth.go`). Reuse the siblings' key names and readers (`host` via `model.HostSetting`, `database` via `model.DatabaseSetting`) rather than inventing new ones. Use endpoint values exactly as the user enters them: don't append suffixes, and don't expose a setting for a value the provider fixes. Every key the spec adds names the code that reads it and what it changes.
- **Object storage.** `warehouseutils.ObjectStorageType` picks the provider, `misc.GetObjectStorageConfig` builds its config, and `filemanager.New` builds the manager. There is no per-destination resolver. `ObjectStorageType` returns S3 whenever `useRudderStorage` is set, so a warehouse that can only use its own storage must short-circuit before that check. Deleting load objects after a sync is the router's job (`warehouse/router/upload.go` `cleanupObjectStorageFiles`); the integration deletes nothing itself, so deletion behaves the same as for every sibling.
- **Per table.** Create a staging table shaped like the target. Load each load file into it with an explicit, sorted column list (`warehouseutils.SortColumnKeysFromColumnMap`). Then:
  - if `ShouldMerge(table)` is true, MERGE (or delete + insert) into the target on the primary key, deduplicating staging with `row_number() OVER (PARTITION BY <partition key> ORDER BY received_at DESC) = 1`. Keys come from `primaryKeyMap` / `partitionKeyMap` (`redshift.go`); `Uploader.ShouldOnDedupUseNewRecord()` decides whether a match takes the new row.
  - otherwise, `INSERT ... SELECT` from staging.
- **`ShouldMerge` gates.** `Warehouse.<dest>.allowMerge`, `Warehouse.<dest>.appendOnlyTables`, the destination's `preferAppend` setting (`GetPreferAppendSetting`), and `Uploader.CanAppend()`. Copy `snowflake.go` `ShouldMerge`.
- **Users.** Stage the union of existing users and the upload's identifies, compute the latest non-null value per `id` with `FIRST_VALUE(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ...)` (`redshift.go` `loadUserTables`), then merge into `users`. A correlated per-column subquery is much slower; avoid it.

## Speccing or planning a warehouse

A spec or plan for a warehouse integration is held to this skill too:

- List every repo the contract puts work in (integrations-config for the destination definition, sqlconnect-go for the driver, rudder-go-kit for the file manager) as an implementation target in the front matter. Never write a constraint that keeps work out of the repo that owns the layer.
- Write out what the integration will execute: the staging-table and COPY statements, the MERGE with its `ON` key and dedup, the users latest-traits statement, and the full type map, including the load-file writer's entries for every type (`json` too).
- Once the code has merged, the merged code at its pinned versions is the source for the spec, not the earlier draft or PR descriptions. A rule the code currently breaks stays in the spec, listed as a known gap.

## Scope

Shared code outside the integration package gets registration-style diffs only. Before changing a shared path, confirm a warehouse destination actually reaches it:

- `warehouse/api/grpc.go` `ValidateObjectStorageDestination` validates a **workspace's** data-retention storage; its type is always a storage provider, never a warehouse. Destination validation is `warehouse/validations`.
- The load-file downloader (`warehouse/internal/service/loadfiles/downloader`) serves warehouses that download load files, not ones that `COPY` from object storage.

A refactor of a shared path is a separate PR with its own tests.

## Tests that prove what they claim

- A negative test asserts the error **type** via `MatchUploadJobErrorType`, not just `require.Error`, so it can't pass by failing at an earlier step.
- Make the mismatch go in the direction the provider rejects; many warehouses coerce silently in the other direction.
- Expected row counts account for every batch the test loads.
- Expected tables and columns come from the embedded transformer (`processor/internal/transformer/destination_transformer/embedded/warehouse/events.go`): only `tracks` and the other non-per-event tables get `rudder_event`; per-event track tables don't.
- An identifier-injection test keeps the hostile value in SQL identifiers only; object-store paths stay safe so the load reaches the SQL.

## Common mistakes

| Mistake | Consequence |
|---------|-------------|
| Error regex matches a prefix this package adds (`merge:`, `copy_into:`) | Every failure of that step gets one wrong type |
| `return &types.LoadTableStats{}, nil` | Rows-loaded and dedup metrics are always zero |
| Staging drop uses the upload's ctx, no dangling sweep | `rudder_staging_*` tables pile up in the customer's warehouse, hidden by `FetchSchema` |
| Local connector / file manager instead of sqlconnect-go / go-kit | Duplicated auth logic that other consumers can't reuse; reviewers will ask to move it |
| Port or endpoint read from destination config | Misconfiguration surface for a value the provider fixes |
