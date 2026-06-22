# Stack & Dependencies

> Project technology stack, frameworks, and key dependencies.
> This file is append-only - agents add discoveries, never delete.

(Add stack information as you discover it)

## Key libraries for the MTU/MAR pipeline

- `github.com/segmentio/go-hll` — HyperLogLog implementation; `hll.NewHll(settings)`,
  `AddRaw(uint64)`, `Union(hll)`, `ToBytes()`/`FromBytes()`, `hll.Settings`,
  `hll.AutoExplicitThreshold`. Wire-compatible with rudderstack-reporting's Postgres `hll`
  extension at Log2m=16/Regwidth=5.
- `github.com/spaolacci/murmur3` — `murmur3.Sum64WithSeed([]byte, seed)`; seed fixed at 123.
- `github.com/tidwall/gjson` — pulls fields out of job EventPayload/Parameters JSON.
- `github.com/golang-migrate/migrate` via `iofs` source driver — runs embedded SQL migrations
  (services/sql-migrator/migrator.go); migrations embedded with `//go:embed **/*.sql`.
- `github.com/lib/pq` — Postgres driver + `pq.CopyIn` bulk COPY in ReportUsers.
- `github.com/ory/dockertest/v3` + rudder-go-kit `testhelper/docker/resource/postgres`
  (`postgres.Setup(pool, t)`) — integration tests need a Docker daemon.
- `github.com/rudderlabs/rudder-go-kit` — config (`GetBoolVar/GetIntVar/GetStringVar`),
  logger, stats (`NewTaggedStat`, `collectors.NewDatabaseSQLStats`).
- `github.com/rudderlabs/rudder-observability-kit/go/labels` (obskit) — structured log fields.
- `github.com/samber/lo` — functional helpers (e.g. `lo.MapToSlice`, `lo.Find`).
- Internal: `jobsdb` (JobT), `utils/tx` (txn.Tx), `utils/timeutil`, `utils/misc`
  (`GetConnectionString(conf, "tracked_users"|"reporting")`).

Go module: `github.com/rudderlabs/rudder-server`. Build/test via Makefile
(`make build`, `make test`, `make test-it`, `make lint`, `make mocks`).
