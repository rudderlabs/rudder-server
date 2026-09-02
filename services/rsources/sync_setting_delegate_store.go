package rsources

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The durable half of the delegate: one table, one upsert, one cleanup sweep.

const (
	syncSettingsTable           = "rsources_sync_settings"
	syncSettingsMigrationsTable = "rsources_sync_settings_migrations"

	syncSettingsMaxIdleConnsKey     = "Rsources.syncSettings.maxIdleConns"
	syncSettingsMaxOpenConnsKey     = "Rsources.syncSettings.maxOpenConns"
	syncSettingsMaxAgeKey           = "Rsources.syncSettings.maxAge"
	syncSettingsCleanupFrequencyKey = "Rsources.syncSettings.cleanupFrequency"
)

// syncSettingsDSN resolves the component's connection string: the shared database when
// the deployment has one (the pin has to be visible to every pod of the tenant), the
// local one otherwise.
func syncSettingsDSN(conf *config.Config) (string, error) {
	if !conf.IsSet("SharedDB.dsn") {
		return misc.GetConnectionString(conf, "rsources-sync-settings"), nil
	}
	dsn, err := misc.SetAppNameInDBConnURL(conf.GetStringVar("", "SharedDB.dsn"), "rsources-sync-settings")
	if err != nil {
		return "", fmt.Errorf("setting application name in db conn url: %w", err)
	}
	return dsn, nil
}

// setupSyncSettingsDBConn opens the component's long-lived pool.
func setupSyncSettingsDBConn(conf *config.Config, dsn string, statFactory stats.Stats) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	db.SetMaxIdleConns(conf.GetIntVar(1, 1, syncSettingsMaxIdleConnsKey))
	db.SetMaxOpenConns(conf.GetIntVar(2, 1, syncSettingsMaxOpenConnsKey))
	if err := statFactory.RegisterCollector(collectors.NewDatabaseSQLStats(syncSettingsTable, db)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("registering collector: %w", err)
	}
	return db, nil
}

// migrateSyncSettings creates the component's table through the shared sql-migrator,
// which already serialises concurrent migrators of the same set - a deploy rolls many
// pods at once against one database.
//
// It runs on a throwaway pool of its own, closed before the long-lived one is opened.
// golang-migrate's postgres driver checks out a dedicated *sql.Conn and only returns it
// when the migration is Closed; sharing the serving pool would therefore park one of
// its (two) connections for the process's whole life, leaving the cleanup DELETE and
// every pin upsert to contend for the survivor.
func migrateSyncSettings(conf *config.Config, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("db open for migration: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	m := &migrator.Migrator{
		Handle:                     db,
		MigrationsTable:            syncSettingsMigrationsTable,
		ShouldForceSetLowerVersion: conf.GetBoolVar(true, "SQLMigrator.forceSetLowerVersion"),
	}
	if err := m.Migrate(syncSettingsTable); err != nil {
		return fmt.Errorf("migrating %s: %w", syncSettingsTable, err)
	}
	return nil
}

// loadAll seeds the cache with every pinned decision still inside the retention window.
//
// One unpaginated SELECT on purpose: the cleanup routine keeps only maxAge (24h by
// default) worth of job run ids, so the table holds one row per rETL sync started in
// the last day - kilobytes, not megabytes. Pods restart every couple of hours, so this
// runs often; if the retention is ever raised into the "millions of rows" range this
// is the assumption that breaks.
func (d *syncSettingDelegate) loadAll(ctx context.Context) error {
	rows, err := d.db.QueryContext(ctx,
		`SELECT job_run_id, store_error_responses, EXTRACT(EPOCH FROM (NOW() - created_at)) FROM `+syncSettingsTable)
	if err != nil {
		return fmt.Errorf("load pinned decisions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	maxAge := d.maxAge.Load()
	for rows.Next() {
		var (
			jobRunID   string
			store      bool
			ageSeconds float64
		)
		if err := rows.Scan(&jobRunID, &store, &ageSeconds); err != nil {
			return fmt.Errorf("scan pinned decision: %w", err)
		}
		d.cacheDecision(jobRunID, store, ageSeconds, maxAge)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load pinned decisions: %w", err)
	}
	return nil
}

// storeErrorResponses answers step 3 of the check order: the run's pinned decision.
//
// A cache hit answers immediately. A miss goes to the database, which is also what
// pins the run - absence is never cached, so a pod that started before another pod
// pinned the run adopts the stored answer instead of deriving one from its own view of
// the config.
func (d *syncSettingDelegate) storeErrorResponses(ctx context.Context, key statKey) (bool, error) {
	if entry := d.decisions.Get(key.jobRunId); entry != nil {
		return entry.storeErrorResponses, nil
	}
	// Only a genuine miss waits for the first config push; from that push on this is a
	// read from a closed channel.
	if err := d.awaitConfig(ctx); err != nil {
		return false, err
	}
	store, ageSeconds, err := d.pinDecision(ctx, key)
	if err != nil {
		return false, err
	}
	d.cacheDecision(key.jobRunId, store, ageSeconds, d.maxAge.Load())
	return store, nil
}

// syncSettingsUpsert pins a run's decision and returns whatever is stored for it -
// which, for every racer after the first, is the FIRST writer's value.
//
// The `DO UPDATE SET job_run_id = EXCLUDED.job_run_id` is a deliberate no-op write: a
// plain DO NOTHING would suppress the RETURNING clause for the losing racers and hand
// them an empty result, whereas touching the row makes postgres return the stored
// values. So one statement gives first-writer-wins plus a read for everybody, with no
// transaction and no advisory lock.
const syncSettingsUpsert = `INSERT INTO ` + syncSettingsTable + ` (job_run_id, store_error_responses)
VALUES ($1, $2)
ON CONFLICT (job_run_id) DO UPDATE SET job_run_id = EXCLUDED.job_run_id
RETURNING store_error_responses, EXTRACT(EPOCH FROM (NOW() - created_at))`

// pinDecision resolves the run against the current connection index and pins it,
// unless it is already pinned - in which case the stored decision wins and the value
// computed here is discarded.
//
// The row's AGE comes back rather than its timestamp: the cache TTL is then derived
// from the database's clock, the same one the cleanup cutoff uses, so a cached entry
// cannot outlive its row (or expire early) because two machines disagree about now.
func (d *syncSettingDelegate) pinDecision(ctx context.Context, key statKey) (bool, float64, error) {
	var (
		store      bool
		ageSeconds float64
	)
	if err := d.db.QueryRowContext(ctx, syncSettingsUpsert,
		key.jobRunId, d.connectionEnabled(key.SourceID, key.DestinationID),
	).Scan(&store, &ageSeconds); err != nil {
		return false, 0, fmt.Errorf("pin decision: %w", err)
	}
	return store, ageSeconds, nil
}

// cacheDecision stores a decision that is known to exist in the database, expiring it
// when the row itself becomes eligible for the cleanup DELETE. A row already past
// maxAge is not cached at all.
//
// ageSeconds is the row's age according to the DATABASE, so the entry's lifetime and
// the row's lifetime are measured against the same clock.
//
// The cache is not size-capped: it holds one small entry per rETL sync of the last
// maxAge, bounded by the same retention as the table.
func (d *syncSettingDelegate) cacheDecision(jobRunID string, store bool, ageSeconds float64, maxAge time.Duration) {
	ttl := maxAge - time.Duration(ageSeconds*float64(time.Second))
	if ttl <= 0 {
		return
	}
	d.decisions.Put(jobRunID, &syncSettingEntry{storeErrorResponses: store}, ttl)
}

// CleanupRoutine drops decisions older than maxAge from the table, until the context is
// cancelled or Stop is called.
//
// Every pod runs this concurrently against the same table, so nothing here may assume a
// single runner: the DELETE is naturally idempotent. The in-memory cache needs no sweep
// - each entry carries its own TTL - so the only other housekeeping is resetting the
// suppression-log throttle, which lets a still-blacklisted connection warn again next
// tick instead of going quiet forever.
func (d *syncSettingDelegate) CleanupRoutine(ctx context.Context) error {
	if !d.routineStarted() {
		return nil
	}
	defer d.wg.Done()
	for {
		if _, err := d.db.ExecContext(ctx,
			// The cutoff is computed by the database, not this process: the cache TTL
			// is derived from the same clock, so the two cannot drift apart.
			`DELETE FROM `+syncSettingsTable+` WHERE created_at < NOW() - make_interval(secs => $1)`,
			d.maxAge.Load().Seconds(),
		); err != nil {
			if ctx.Err() != nil || d.stopping() {
				return nil
			}
			// A failed sweep is not fatal: the table is bounded by the other pods'
			// sweeps and by the next tick. Killing the errgroup over it would take the
			// whole pod down for a housekeeping error.
			d.log.Errorn("rsources sync settings: cleanup", obskit.Error(err))
		}
		d.resetWarnThrottle()

		select {
		case <-ctx.Done():
			return nil
		case <-d.stop:
			return nil
		case <-time.After(d.cleanupFrequency.Load()):
		}
	}
}

func (d *syncSettingDelegate) resetWarnThrottle() {
	d.warnedMu.Lock()
	defer d.warnedMu.Unlock()
	clear(d.warned)
}
