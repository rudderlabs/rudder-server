---
name: building-warehouse-integrations
description: Use when adding a new warehouse destination under warehouse/integrations/, or changing an existing warehouse's error mappings, load path (COPY/MERGE/INSERT), Cleanup, data type maps, connection or object-storage setup, or its integration tests.
---

# Building warehouse integrations

## Overview

Every warehouse in `warehouse/integrations/` honours the same contract with the router, the stats pipeline and the shared libraries. Nothing enforces it at compile time, so a new integration can pass its sqlmock tests while misclassifying every failure, reporting zero rows and leaking staging tables. Check each item below against an existing integration before calling the work done.

A sibling dialect is not a spec. Copying MSSQL into a T-SQL warehouse, or Postgres into a Postgres-wire one, carries over types, functions and defaults the new provider may not support. Confirm provider behaviour against its docs or a live instance.

## The contract

| # | Area | Requirement | Reference |
|---|------|-------------|-----------|
| 1 | Connection | Open the DB through an sqlconnect-go driver: marshal its config struct, `sqlconnect.NewDB("<driver>", json)`, `.SqlDB()`, wrap with `sqlmw.New`. Auth bootstraps (including ones that call a separate provider REST API before the SQL connect), token flows and default ports belong in the driver; if the driver is missing, add it to sqlconnect-go first and bump `go.mod`. Values fixed by the provider are constants, never read from destination config. | `snowflake.go` `connect`, `deltalake.go` `connect` |
| 2 | Object storage | Build file managers with rudder-go-kit `filemanager.New`. The destType → storage-provider mapping lives only in `warehouseutils.ObjectStorageType`. If go-kit lacks the provider, add it there first, even when the batch router and the warehouse slave also need it: they already call `filemanager.New`, so no resolver or factory gets threaded through them. | `warehouse/utils/utils.go` `ObjectStorageType` |
| 3 | `ErrorMappings()` | Each regex matches the **provider's own** error text or code. `MatchUploadJobErrorType` returns the **first** match (`warehouse/router/errors.go`), so order matters and broad patterns swallow everything after them. Cover auth/credential failures under `PermissionError`. Map only strings something can still emit. For failures this package or a shared library raises itself (HTTP calls, config checks), classify by status, never by a package prefix: 401/403 is `PermissionError`, and transient 429/5xx/network stays uncategorised. Test each mapping with a realistic provider error in the package test and `warehouse/router/errors_test.go`. | `redshift.go`, `snowflake.go`, `deltalake.go` `errorsMappings` |
| 4 | `LoadTableStats` | Return `RowsInserted` / `RowsUpdated` from the `RowsAffected` of the COPY / INSERT / MERGE. They feed `post_load_table_rows` and `dedup_rows` (`warehouse/router/state_export_data.go`). Only the plain-file datalake returns `{}`. Return the stats as-is: loads are not transactional, so an error returned after the write commits makes the retry re-insert in append mode. | `snowflake.go`, `mssql.go` `loadTable` |
| 5 | Staging cleanup | `Cleanup` calls `dropDanglingStagingTables`, which finds tables by `warehouseutils.StagingTablePrefix(provider)`. Inline staging drops use `context.WithoutCancel(ctx)` so a cancelled or timed-out upload still drops them. | `mssql.go`, `redshift.go` `Cleanup` |
| 6 | Type maps | `dataTypesMap` (rudder → warehouse) must accept the load file's physical type (check the parquet/CSV writer mapping). `dataTypesMapToRudder` lists only types the warehouse can store and that `INFORMATION_SCHEMA` returns. `fullEventColumnTypeByDestType` in the embedded transformer (`processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils/utils.go`) agrees with `dataTypesMap` on json vs string. If no producer emits a type, its handling in `dataTypesMap` and the parquet writer/loader is dead code: either make the transformer emit it or remove it. | any integration's `dataTypesMap` |
| 7 | SQL idioms | Quote with `warehouseutils.QuoteQualifiedIdentifier` / `SQLStringLiteral`. `QueryContext` never returns `sql.ErrNoRows` (only `QueryRow().Scan` does). A SQL fragment used by two statements that must agree (e.g. a MERGE join and the matching COUNT) is one helper. | `warehouse/utils/identifier.go` |
| 8 | Registration | `warehouse/utils` constant + `WarehouseDestinations`; both switches in `warehouse/integrations/manager/manager.go`; `warehouse/integrations/config`; the embedded-transformer map from #6; the `warehouse-integration` matrix in `.github/workflows/tests.yaml` (`make fmt` runs matrixchecker). | `AzureSynapse` / `azure-synapse` entries |
| 9 | Integration test | An end-to-end `integration_test.go` against the real warehouse, modelled on `redshift_test.go` / `snowflake_test.go`. Credentials come from `<DEST>_INTEGRATION_TEST_CREDENTIALS`; skip when unset. sqlmock unit tests do not replace it. | `warehouse/integrations/redshift/redshift_test.go` |

## Scope

Shared code outside the integration package gets registration-style diffs only. Before changing a shared path, confirm a warehouse destination actually reaches it:

- `warehouse/api/grpc.go` `ValidateObjectStorageDestination` validates a **workspace's** data-retention storage; its type is always a storage provider, never a warehouse. Destination validation is `warehouse/validations`.
- The load-file downloader (`warehouse/internal/service/loadfiles/downloader`) serves warehouses that download load files, not ones that `COPY` from object storage.

A refactor of a shared path is a separate PR with its own tests.

## Tests that prove what they claim

- A negative test asserts the error **type** via `MatchUploadJobErrorType`, not just `require.Error`, so it can't pass by failing at an earlier step.
- Make the mismatch go in the direction the provider rejects; many warehouses coerce silently in the other direction.
- Expected row counts account for every batch the test loads.
- An identifier-injection test keeps the hostile value in SQL identifiers only; object-store paths stay safe so the load reaches the SQL.

## Common mistakes

| Mistake | Consequence |
|---------|-------------|
| Error regex matches a prefix this package adds (`merge:`, `copy_into:`) | Every failure of that step gets one wrong type |
| `return &types.LoadTableStats{}, nil` | Rows-loaded and dedup metrics are always zero |
| Staging drop uses the upload's ctx, no dangling sweep | `rudder_staging_*` tables pile up in the customer's warehouse, hidden by `FetchSchema` |
| Local connector / file manager instead of sqlconnect-go / go-kit | Duplicated auth logic that other consumers can't reuse; reviewers will ask to move it |
| Port or endpoint read from destination config | Misconfiguration surface for a value the provider fixes |
