package rsources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The durable half of the delegate: one table, one advisory-locked read-or-pin, one
// cleanup sweep.
//
// The table is self-managed (CREATE TABLE IF NOT EXISTS at startup) rather than a
// jobsdb migration, the same shape internal/drain-config uses: the component owns its
// connection and its schema, and adding it does not touch the jobsdb migration chain.

const (
	syncSettingsTable = "rsources_sync_settings"

	syncSettingsMaxIdleConnsKey     = "Rsources.syncSettings.maxIdleConns"
	syncSettingsMaxOpenConnsKey     = "Rsources.syncSettings.maxOpenConns"
	syncSettingsMaxAgeKey           = "Rsources.syncSettings.maxAge"
	syncSettingsCleanupFrequencyKey = "Rsources.syncSettings.cleanupFrequency"
)

// syncSettingsLockClass is this component's private class id for the TWO-argument
// pg_advisory_xact_lock(classid, objid).
//
// The two-argument form has its own key space, disjoint from the single-argument
// (bigint) form used elsewhere in the process - services/rsources/handler.go locks on
// 100020001 and jobsdb derives its ids by hashing table names - so no coordination
// with either is needed. The value is adjacent to the rsources handler's id purely so
// that a `SELECT * FROM pg_locks` during an incident reads as one family.
const syncSettingsLockClass int32 = 100020002

// syncSettingsSetupLockObj is the object id reserved for "creating the table". Job run
// ids are hashed into the same class, so the hash is forced away from this value.
const syncSettingsSetupLockObj int32 = 0

// postgres error codes we tolerate when several pods create the table at once. A
// concurrent CREATE TABLE IF NOT EXISTS is not race-free in postgres: the existence
// check and the catalog insert are separate, so a loser can surface either
// duplicate_table or a unique_violation on a catalog index.
const (
	pgErrDuplicateTable  = "42P07"
	pgErrUniqueViolation = "23505"
)

// setupSyncSettingsDBConn opens the component's own connection, following
// internal/drain-config: the shared database when the deployment has one (the pin has
// to be visible to every pod of the tenant), the local one otherwise.
func setupSyncSettingsDBConn(conf *config.Config, statFactory stats.Stats) (*sql.DB, error) {
	psqlInfo := misc.GetConnectionString(conf, "rsources-sync-settings")
	if conf.IsSet("SharedDB.dsn") {
		psqlInfo = conf.GetStringVar("", "SharedDB.dsn")
	}
	db, err := sql.Open("postgres", psqlInfo)
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

// setupTable creates the table if it does not exist.
//
// A deploy rolls many pods at once and they all run this against the same database, so
// the statement is serialised behind an advisory lock on {syncSettingsLockClass,
// syncSettingsSetupLockObj}. The duplicate-object codes are tolerated as well: the
// advisory lock only coordinates processes that took it, and a database restored or
// migrated by other means may race us.
func (d *syncSettingDelegate) setupTable(ctx context.Context) error {
	err := d.withAdvisoryLock(ctx, syncSettingsSetupLockObj, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+syncSettingsTable+` (
			job_run_id TEXT PRIMARY KEY,
			store_error_responses BOOLEAN NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
		return err
	})
	if err != nil && !isDuplicateObjectError(err) {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func isDuplicateObjectError(err error) bool {
	var pgErr *pq.Error
	if !errors.As(err, &pgErr) {
		return false
	}
	code := string(pgErr.Code)
	return code == pgErrDuplicateTable || code == pgErrUniqueViolation
}

// loadAll seeds the cache with every pinned decision.
//
// One unpaginated SELECT on purpose: the cleanup routine keeps only maxAge (24h by
// default) worth of job run ids, so the table holds one row per rETL sync started in
// the last day - kilobytes, not megabytes. Pods restart every couple of hours, so this
// runs often; if the retention is ever raised into the "millions of rows" range this
// is the assumption that breaks.
func (d *syncSettingDelegate) loadAll(ctx context.Context) error {
	rows, err := d.db.QueryContext(ctx,
		`SELECT job_run_id, store_error_responses, created_at FROM `+syncSettingsTable)
	if err != nil {
		return fmt.Errorf("load pinned decisions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	loaded := make(map[string]syncSettingEntry)
	for rows.Next() {
		var (
			jobRunID string
			entry    syncSettingEntry
		)
		if err := rows.Scan(&jobRunID, &entry.storeErrorResponses, &entry.createdAt); err != nil {
			return fmt.Errorf("scan pinned decision: %w", err)
		}
		loaded[jobRunID] = entry
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load pinned decisions: %w", err)
	}
	d.decisionsMu.Lock()
	d.decisions = loaded
	d.decisionsMu.Unlock()
	return nil
}

// storeErrorResponses answers step 3 of the check order: the run's pinned decision.
//
// A cache hit answers immediately. A miss always goes to the database before computing
// anything - absence is never cached - so a pod that started before another pod pinned
// the run adopts the stored answer instead of re-deriving one from its own view of the
// config.
func (d *syncSettingDelegate) storeErrorResponses(ctx context.Context, key statKey) (bool, error) {
	if entry, ok := d.cachedDecision(key.jobRunId); ok {
		return entry.storeErrorResponses, nil
	}
	// Only a genuine miss waits for the first config push. Waiting here rather than
	// inside the transaction keeps the advisory lock's hold time bounded by the
	// database alone, and the wait is a closed-channel read from the first push on.
	if err := d.awaitConfig(ctx); err != nil {
		return false, err
	}
	entry, err := d.readOrPin(ctx, key)
	if err != nil {
		return false, err
	}
	d.cacheDecision(key.jobRunId, entry)
	return entry.storeErrorResponses, nil
}

// readOrPin returns the run's stored decision, computing and inserting one if the run
// has never been seen.
//
// The advisory lock is taken before the SELECT and released by the commit, so two
// racers - two goroutines of one pod, or two pods - are serialised: the loser's SELECT
// runs after the winner's INSERT is committed and returns the winner's value. Both
// therefore answer identically even if the config flipped between their computations.
func (d *syncSettingDelegate) readOrPin(ctx context.Context, key statKey) (syncSettingEntry, error) {
	var entry syncSettingEntry
	err := d.withAdvisoryLock(ctx, jobRunLockObj(key.jobRunId), func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx,
			`SELECT store_error_responses, created_at FROM `+syncSettingsTable+` WHERE job_run_id = $1`,
			key.jobRunId,
		).Scan(&entry.storeErrorResponses, &entry.createdAt)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, sql.ErrNoRows):
		default:
			return fmt.Errorf("read pinned decision: %w", err)
		}
		// created_at comes back from the database so that the cache ages entries on
		// the same clock the cleanup DELETE uses.
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO `+syncSettingsTable+` (job_run_id, store_error_responses)
			 VALUES ($1, $2) RETURNING store_error_responses, created_at`,
			key.jobRunId, d.connectionEnabled(key.SourceID, key.DestinationID),
		).Scan(&entry.storeErrorResponses, &entry.createdAt); err != nil {
			return fmt.Errorf("pin decision: %w", err)
		}
		return nil
	})
	if err != nil {
		return syncSettingEntry{}, err
	}
	return entry, nil
}

func (d *syncSettingDelegate) cachedDecision(jobRunID string) (syncSettingEntry, bool) {
	d.decisionsMu.RLock()
	defer d.decisionsMu.RUnlock()
	entry, ok := d.decisions[jobRunID]
	return entry, ok
}

// cacheDecision stores a decision that is known to exist in the database.
//
// The cache is not size-capped: it holds one small entry per rETL sync of the last
// maxAge, bounded by the same retention as the table, and the cleanup sweep evicts by
// age. Capping it would mean either evicting live runs or refusing to cache them, and
// both cost a database round trip per record for the runs that lost the race.
func (d *syncSettingDelegate) cacheDecision(jobRunID string, entry syncSettingEntry) {
	d.decisionsMu.Lock()
	defer d.decisionsMu.Unlock()
	d.decisions[jobRunID] = entry
}

// CleanupRoutine drops decisions older than maxAge, from the table and from this
// pod's cache, until the context is cancelled.
//
// Every pod runs this concurrently against the same table, so nothing here may assume
// a single runner: the DELETE is naturally idempotent, and the cache is swept by age
// rather than by "the rows I just deleted" - another pod winning the DELETE must not
// leave this pod's cache holding rows forever.
func (d *syncSettingDelegate) CleanupRoutine(ctx context.Context) error {
	if !d.routineStarted() {
		return nil
	}
	defer d.wg.Done()
	for {
		if d.stopping() {
			return nil
		}
		maxAge := d.maxAge.Load()
		if _, err := d.db.ExecContext(ctx,
			`DELETE FROM `+syncSettingsTable+` WHERE created_at < $1`,
			time.Now().Add(-maxAge),
		); err != nil {
			if ctx.Err() != nil || d.stopping() {
				return nil
			}
			// A failed sweep is not fatal: the table is bounded by the other pods'
			// sweeps and by the next tick. Killing the errgroup over it would take the
			// whole pod down for a housekeeping error.
			d.log.Errorn("rsources sync settings: cleanup", obskit.Error(err))
		}
		d.evictExpired(time.Now().Add(-maxAge))

		select {
		case <-ctx.Done():
			return nil
		case <-d.stop:
			return nil
		case <-time.After(d.cleanupFrequency.Load()):
		}
	}
}

// evictExpired drops cached decisions pinned before `before`, and resets the
// suppression-log throttle so a still-blacklisted connection warns again next sweep
// instead of going quiet forever.
func (d *syncSettingDelegate) evictExpired(before time.Time) {
	d.decisionsMu.Lock()
	for jobRunID, entry := range d.decisions {
		if entry.createdAt.Before(before) {
			delete(d.decisions, jobRunID)
		}
	}
	d.decisionsMu.Unlock()

	d.warnedMu.Lock()
	clear(d.warned)
	d.warnedMu.Unlock()
}

// withAdvisoryLock runs f inside a transaction holding the two-argument transactional
// advisory lock {syncSettingsLockClass, objID}.
func (d *syncSettingDelegate) withAdvisoryLock(ctx context.Context, objID int32, f func(tx *sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`, syncSettingsLockClass, objID,
	); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	}
	if err := f(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// jobRunLockObj hashes a job run id into the object id half of the advisory lock key.
// The mask keeps the result non-negative (postgres takes int4) and the zero is stepped
// over because it is reserved for table setup. A collision between two run ids only
// costs the two runs a shared lock, never a wrong answer: the SELECT inside the lock
// is keyed by the run id itself.
func jobRunLockObj(jobRunID string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobRunID))
	obj := int32(h.Sum32() & 0x7fffffff) // #nosec G115 -- masked to 31 bits
	if obj == syncSettingsSetupLockObj {
		return syncSettingsSetupLockObj + 1
	}
	return obj
}
