package rsources

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// The durable half of the sync setting delegate, against a real postgres.
//
// Everything here exists because of one operational fact: server pods restart roughly
// every two hours and several of them share one database. So the cases are not
// "does the SQL work" - they are "do two pods, or one pod either side of a restart,
// answer the same question the same way".
//
// The subtests share one container and one table on purpose (a container per case
// would dominate the runtime) and each one uses its own job run ids. The migration
// case runs first, on the genuinely empty database, and rebuilds the table for the
// rest.

const dbTestSourceID = "db-src"

// delegateDeps are the collaborators of one delegate instance, built before the
// instance so that construction itself can be driven concurrently.
type delegateDeps struct {
	conf  *config.Config
	stats *memstats.Store
	log   *capturingLogger
	bc    backendconfig.BackendConfig
}

// subscribeFunc is how a test stands in for the backend config's subscription.
type subscribeFunc func(context.Context, backendconfig.Topic) pubsub.DataChannel

func newDelegateDeps(t *testing.T, dsn string, tweaks ...func(*config.Config)) *delegateDeps {
	t.Helper()
	// Most cases drive the index directly through indexConnections, so the
	// subscription only has to exist.
	idle := make(chan pubsub.DataEvent)
	return newDelegateDepsWith(t, dsn,
		func(context.Context, backendconfig.Topic) pubsub.DataChannel { return idle },
		tweaks...)
}

func newDelegateDepsWith(
	t *testing.T, dsn string, subscribe subscribeFunc, tweaks ...func(*config.Config),
) *delegateDeps {
	t.Helper()
	conf := config.New()
	conf.Set("SharedDB.dsn", dsn)
	conf.Set(captureErrorDetailKey, true)
	// The upsert race is only real if several racers can be in the database at once;
	// the production default of 2 would serialise them in the connection pool instead.
	conf.Set(syncSettingsMaxOpenConnsKey, 12)
	for _, tweak := range tweaks {
		tweak(conf)
	}
	statsStore, err := memstats.New()
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	bc := mock_backendconfig.NewMockBackendConfig(ctrl)
	bc.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(subscribe).AnyTimes()

	return &delegateDeps{conf: conf, stats: statsStore, log: newCapturingLogger(), bc: bc}
}

// build runs the real constructor: migration, load-all and the subscription.
func (deps *delegateDeps) build(ctx context.Context) (*syncSettingDelegate, error) {
	return NewSyncSettingDelegate(ctx, deps.conf, deps.log, deps.stats, deps.bc)
}

type dbDelegate struct {
	*syncSettingDelegate
	deps *delegateDeps
}

// The accessors are named away from the embedded struct's own `log` field so that a
// reader never has to work out which one a selector resolves to.
func (d *dbDelegate) cfg() *config.Config         { return d.deps.conf }
func (d *dbDelegate) logs() *capturingLogger      { return d.deps.log }
func (d *dbDelegate) statsStore() *memstats.Store { return d.deps.stats }

func (d *dbDelegate) key(jobRunID string) statKey {
	return statKey{
		jobRunId: jobRunID,
		JobTargetKey: JobTargetKey{
			TaskRunID: testTaskRunID, SourceID: dbTestSourceID, DestinationID: testDestID,
		},
	}
}

// index sets this instance's view of the control plane and marks the first push as
// landed, exactly as ConfigSubscriberRoutine would.
func (d *dbDelegate) index(enabled bool) {
	d.indexConnections(connectionsPayload(map[string][]backendconfig.Connection{
		testWorkspace: {connectionWith(dbTestSourceID, testDestID, enabled)},
	}))
}

// ask runs the whole check order for one aborted record of the given run.
func (d *dbDelegate) ask(ctx context.Context, jobRunID string) (string, error) {
	return d.GetErrorResponse(ctx, d.key(jobRunID), abortedStatus(`{"response":"boom"}`))
}

// cached is the pin cache lookup: a hit returns the entry, a miss or an entry whose
// row has aged out returns nil.
func (d *dbDelegate) cached(jobRunID string) *syncSettingEntry {
	return d.decisions.Get(jobRunID)
}

// warnedKeys counts the throttled suppression keys, under the lock the cleanup
// routine also takes - this is read while that routine is running.
func (d *dbDelegate) warnedKeys() int {
	d.warnedMu.Lock()
	defer d.warnedMu.Unlock()
	return len(d.warned)
}

func newDBDelegate(t *testing.T, dsn string, tweaks ...func(*config.Config)) *dbDelegate {
	t.Helper()
	deps := newDelegateDeps(t, dsn, tweaks...)
	d, err := deps.build(context.Background())
	require.NoError(t, err)
	t.Cleanup(d.Stop) // Stop is idempotent, so an explicit Stop in a test is fine too
	return &dbDelegate{syncSettingDelegate: d, deps: deps}
}

func storedDecision(t *testing.T, db *sql.DB, jobRunID string) (value, found bool) {
	t.Helper()
	err := db.QueryRow(
		`SELECT store_error_responses FROM `+syncSettingsTable+` WHERE job_run_id = $1`, jobRunID,
	).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false
	}
	require.NoError(t, err)
	return value, true
}

func rowsFor(t *testing.T, db *sql.DB, jobRunID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM `+syncSettingsTable+` WHERE job_run_id = $1`, jobRunID).Scan(&n))
	return n
}

func tableCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = current_schema() AND table_name = $1`, table).Scan(&n))
	return n
}

// insertPinned writes a row the way another pod would have, at an explicit age.
//
// created_at is supplied from this process' clock rather than the database's NOW(), so
// that the row's age, the cleanup cutoff (computed in Go) and the cache TTL (also
// computed in Go) are all on one clock and the case does not depend on the container's.
func insertPinned(t *testing.T, db *sql.DB, jobRunID string, store bool, createdAt time.Time) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO `+syncSettingsTable+` (job_run_id, store_error_responses, created_at)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (job_run_id) DO UPDATE
		 SET store_error_responses = EXCLUDED.store_error_responses, created_at = EXCLUDED.created_at`,
		jobRunID, store, createdAt)
	require.NoError(t, err)
}

func TestSyncSettingDelegateDB(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	dsn := pg.DBDsn
	ctx := context.Background()

	// First, on the empty database: a deploy rolls many pods at once and they all
	// migrate against it simultaneously. services/sql-migrator owns that serialisation
	// (golang-migrate takes a postgres advisory lock keyed on the migrations table), but
	// nothing in this repository exercises several migrators of one set at once, and the
	// component would be unbootable if that ever regressed - so this stays, minimal:
	// build the real constructor concurrently and require every instance to come up.
	// The migrator must not keep a connection out of the serving pool. golang-migrate's
	// postgres driver checks out a dedicated *sql.Conn and only releases it when the
	// migration is closed, so running it on the serving pool would park one of its two
	// connections for the process's whole life - leaving the cleanup DELETE and every
	// pin upsert to fight over the survivor, and failing router status batches on the
	// strict path whenever a sweep ran long.
	t.Run("construction leaves the serving pool fully available", func(t *testing.T) {
		deps := newDelegateDeps(t, dsn, func(c *config.Config) {
			c.Set(syncSettingsMaxOpenConnsKey, 2) // the production default
		})
		d, err := deps.build(ctx)
		require.NoError(t, err)
		t.Cleanup(d.Stop)

		require.Zero(t, d.db.Stats().InUse,
			"the migrator must not still be holding a connection after construction")

		// Both connections must be usable at once: two queries that overlap in time.
		release := make(chan struct{})
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				conn, err := d.db.Conn(qctx)
				if err != nil {
					errs <- err
					return
				}
				defer func() { _ = conn.Close() }()
				if _, err := conn.ExecContext(qctx, `SELECT 1`); err != nil {
					errs <- err
					return
				}
				<-release // hold the connection until both have one
				errs <- nil
			}()
		}
		require.Eventually(t, func() bool { return d.db.Stats().InUse == 2 },
			30*time.Second, 10*time.Millisecond,
			"both pooled connections must be checked out at once, so neither is parked by the migrator")
		close(release)
		for range 2 {
			require.NoError(t, <-errs)
		}
	})

	t.Run("instances created concurrently against an empty database all start", func(t *testing.T) {
		for _, table := range []string{syncSettingsTable, syncSettingsMigrationsTable} {
			_, err := pg.DB.Exec(`DROP TABLE IF EXISTS ` + table)
			require.NoError(t, err)
			require.Zero(t, tableCount(t, pg.DB, table))
		}

		const instances = 6
		deps := make([]*delegateDeps, instances)
		for i := range deps {
			deps[i] = newDelegateDeps(t, dsn)
		}
		built := make([]*syncSettingDelegate, instances)
		errs := make([]error, instances)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range instances {
			wg.Go(func() {
				<-start
				built[i], errs[i] = deps[i].build(ctx)
			})
		}
		close(start)
		wg.Wait()

		for i := range instances {
			require.NoErrorf(t, errs[i], "instance %d failed to start on a fresh database", i)
			require.NotNilf(t, built[i], "instance %d", i)
			built[i].Stop()
		}
		require.Equal(t, 1, tableCount(t, pg.DB, syncSettingsTable))

		// The migrated schema is the one the component actually uses.
		insertPinned(t, pg.DB, "db-migrated", true, time.Now())
		value, found := storedDecision(t, pg.DB, "db-migrated")
		require.True(t, found)
		require.True(t, value)
	})

	t.Run("a pinned decision survives a restart and is not re-derived", func(t *testing.T) {
		const jobRunID = "db-restart"

		// Instance 1: the control plane says capture is on for the connection.
		first := newDBDelegate(t, dsn)
		first.index(true)
		text, err := first.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text)
		value, found := storedDecision(t, pg.DB, jobRunID)
		require.True(t, found, "the first miss must have pinned the run")
		require.True(t, value)
		first.Stop()

		// Instance 2 is the pod that replaced it, and its view of the control plane
		// disagrees: the connection has since been switched off. The pin has to win,
		// otherwise one sync's records are split between two answers.
		second := newDBDelegate(t, dsn)
		second.index(false)
		require.False(t, second.connectionEnabled(dbTestSourceID, testDestID),
			"the second instance's own index must genuinely disagree")

		entry := second.cached(jobRunID)
		require.NotNil(t, entry, "the pin must be loaded at startup, not on first use")
		require.True(t, entry.storeErrorResponses)

		text, err = second.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text, "the restarted pod must answer identically")

		// A third instance that has not seen a single config push answers from the
		// startup load too: with a dead context it could not have waited for one.
		third := newDBDelegate(t, dsn)
		dead, cancel := context.WithCancel(ctx)
		cancel()
		text, err = third.ask(dead, jobRunID)
		require.NoError(t, err, "a cache hit must not depend on the config subscription")
		require.Equal(t, "boom", text)

		require.Equal(t, 1, rowsFor(t, pg.DB, jobRunID))
	})

	t.Run("a pinned false survives a restart too", func(t *testing.T) {
		const jobRunID = "db-restart-off"
		first := newDBDelegate(t, dsn)
		first.index(false)
		text, err := first.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Empty(t, text)
		value, found := storedDecision(t, pg.DB, jobRunID)
		require.True(t, found, "a decision of false is pinned just like a true one")
		require.False(t, value)
		first.Stop()

		second := newDBDelegate(t, dsn)
		second.index(true) // the operator switched it on mid-run
		text, err = second.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Empty(t, text, "a run that started without capture must not start capturing mid-flight")
	})

	t.Run("concurrent misses on one instance pin exactly once", func(t *testing.T) {
		// Nothing serialises these any more: every goroutine runs the same single
		// INSERT ... ON CONFLICT DO UPDATE ... RETURNING, so postgres has to be the one
		// that ends up with one row and one answer for all of them.
		const goroutines = 8
		d := newDBDelegate(t, dsn)
		d.index(true)

		for round := range 5 {
			jobRunID := "db-race-goroutines-" + strconv.Itoa(round)
			texts := make([]string, goroutines)
			errs := make([]error, goroutines)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Go(func() {
					<-start
					texts[i], errs[i] = d.ask(ctx, jobRunID)
				})
			}
			close(start)
			wg.Wait()

			for i := range goroutines {
				require.NoErrorf(t, errs[i], "goroutine %d", i)
				require.Equalf(t, "boom", texts[i], "goroutine %d saw a different answer", i)
			}
			require.Equal(t, 1, rowsFor(t, pg.DB, jobRunID), "round %d must have pinned exactly one row", round)
			entry := d.cached(jobRunID)
			require.NotNilf(t, entry, "round %d: the decision must be cached once resolved", round)
			require.Truef(t, entry.storeErrorResponses, "round %d", round)
		}
	})

	t.Run("two instances racing one new run agree on one stored answer", func(t *testing.T) {
		// The production-normal case: two pods, one database, a brand new job run, and
		// control-plane views that disagree. The upsert is what has to make this come out
		// with a single row and a single answer - the no-op `DO UPDATE SET job_run_id =
		// EXCLUDED.job_run_id` touches the conflicting row precisely so that RETURNING
		// yields the STORED values to the loser instead of nothing. A plain DO NOTHING
		// would hand the loser an empty result, and no upsert at all would hand it a
		// unique violation.
		on := newDBDelegate(t, dsn)
		on.index(true)
		off := newDBDelegate(t, dsn)
		off.index(false)
		require.True(t, on.connectionEnabled(dbTestSourceID, testDestID))
		require.False(t, off.connectionEnabled(dbTestSourceID, testDestID))

		for round := range 10 {
			jobRunID := "db-race-instances-" + strconv.Itoa(round)
			var (
				texts [2]string
				errs  [2]error
				wg    sync.WaitGroup
			)
			start := make(chan struct{})
			for i, d := range []*dbDelegate{on, off} {
				wg.Go(func() {
					<-start
					texts[i], errs[i] = d.ask(ctx, jobRunID)
				})
			}
			close(start)
			wg.Wait()

			require.NoErrorf(t, errs[0], "round %d: the enabled instance", round)
			require.NoErrorf(t, errs[1], "round %d: the disabled instance", round)
			require.Equalf(t, texts[0], texts[1],
				"round %d: both pods must report the same answer for one sync", round)
			require.Equal(t, 1, rowsFor(t, pg.DB, jobRunID), "round %d", round)

			value, found := storedDecision(t, pg.DB, jobRunID)
			require.True(t, found)
			want := ""
			if value {
				want = "boom"
			}
			require.Equalf(t, want, texts[0], "round %d: the answer must be the stored one", round)

			// Both caches must now hold that one stored decision - including the loser's,
			// which cached what it read back rather than what it had computed.
			for name, d := range map[string]*dbDelegate{"on": on, "off": off} {
				entry := d.cached(jobRunID)
				require.NotNilf(t, entry, "round %d: the %s instance did not cache the decision", round, name)
				require.Equalf(t, value, entry.storeErrorResponses,
					"round %d: the %s instance cached something other than the stored row", round, name)
			}
		}
	})

	t.Run("the sweep deletes by age, and cache entries expire with their rows", func(t *testing.T) {
		const (
			oldRun      = "db-cleanup-old"
			expiringRun = "db-cleanup-expiring"
			freshRun    = "db-cleanup-fresh"
		)
		const maxAge = 24 * time.Hour
		now := time.Now()
		// Two rows pinned by some other pod: one long past the retention, one with a
		// couple of seconds of life left in it.
		insertPinned(t, pg.DB, oldRun, true, now.Add(-2*maxAge))
		insertPinned(t, pg.DB, expiringRun, true, now.Add(-maxAge+2*time.Second))

		d := newDBDelegate(t, dsn, func(c *config.Config) {
			c.Set(syncSettingsMaxAgeKey, "24h") // == maxAge; the config reads durations as strings
			c.Set(syncSettingsCleanupFrequencyKey, "10ms")
		})
		d.index(true)
		// loadAll ran before the out-of-band inserts, so adopt them the way a startup
		// would.
		require.NoError(t, d.loadAll(ctx))
		require.Nil(t, d.cached(oldRun),
			"a row the sweep is about to delete must not be adopted into memory at all")
		require.NotNil(t, d.cached(expiringRun), "a row still inside the window is adopted")

		text, err := d.ask(ctx, freshRun)
		require.NoError(t, err)
		require.Equal(t, "boom", text)

		sweepCtx, cancelSweep := context.WithCancel(ctx)
		swept := make(chan error, 1)
		go func() { swept <- d.CleanupRoutine(sweepCtx) }()

		// The condition runs on its own goroutine, so it must not use require.
		countRows := func(jobRunID string) int {
			var n int
			if err := pg.DB.QueryRow(
				`SELECT count(*) FROM `+syncSettingsTable+` WHERE job_run_id = $1`, jobRunID).Scan(&n); err != nil {
				return -1
			}
			return n
		}
		require.Eventually(t, func() bool { return countRows(oldRun) == 0 },
			30*time.Second, 10*time.Millisecond, "a row past maxAge must be deleted by the sweep")
		require.Eventually(t, func() bool {
			return countRows(expiringRun) == 0 && d.cached(expiringRun) == nil
		}, 30*time.Second, 10*time.Millisecond,
			"a row must leave the table as it ages past maxAge, and its cache entry must expire with it")

		require.Equal(t, 1, rowsFor(t, pg.DB, freshRun), "a decision inside maxAge must survive the sweep")
		entry := d.cached(freshRun)
		require.NotNil(t, entry, "a decision inside maxAge must stay cached")
		require.True(t, entry.storeErrorResponses)

		// Another pod (here: this test) deletes the rows out of band. The sweep must keep
		// going without complaining. Its other job - resetting the suppression-log
		// throttle - is what makes a completed iteration observable, so a warn key is
		// planted and its disappearance is waited for rather than slept through.
		_, err = pg.DB.Exec(`DELETE FROM ` + syncSettingsTable)
		require.NoError(t, err)
		require.True(t, d.shouldWarn("db-cleanup-probe", blockedScopeConnection))
		require.Eventually(t, func() bool { return d.warnedKeys() == 0 },
			30*time.Second, 10*time.Millisecond,
			"the sweep must keep iterating over a table it finds empty")

		cancelSweep()
		require.NoError(t, <-swept)
		require.Empty(t, d.logs().errorsWith("rsources sync settings: cleanup"),
			"a sweep that finds nothing to delete is not an error")
	})

	t.Run("a run pinned by another pod after startup is adopted, never re-derived", func(t *testing.T) {
		const jobRunID = "db-adopt"

		// A is already running and its control plane view says capture is off.
		a := newDBDelegate(t, dsn)
		a.index(false)
		require.Nil(t, a.cached(jobRunID), "the run must be unknown to A at this point")

		// B starts later - or simply gets the first record - and pins the run to true.
		b := newDBDelegate(t, dsn)
		b.index(true)
		text, err := b.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text)

		// A's first miss must read B's row rather than compute an answer of its own.
		// Caching absence anywhere in that path would make A answer "" forever.
		text, err = a.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text, "A must adopt the stored decision, not re-derive it from its own index")
		entry := a.cached(jobRunID)
		require.NotNil(t, entry)
		require.True(t, entry.storeErrorResponses)
		require.Equal(t, 1, rowsFor(t, pg.DB, jobRunID))
	})

	t.Run("the config knobs are bound to their documented keys and hot reloadable", func(t *testing.T) {
		const jobRunID = "db-knobs"
		d := newDBDelegate(t, dsn, func(c *config.Config) { c.Set(captureErrorDetailKey, false) })
		d.index(true)

		// Step 2: the feature is off process-wide. Nothing is captured and - the part
		// that matters - nothing is pinned, so switching the flag on later must not
		// find every live run already pinned to false.
		text, err := d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Empty(t, text)
		require.Equal(t, 0, rowsFor(t, pg.DB, jobRunID), "a disabled process must not pin anything")

		d.cfg().Set(captureErrorDetailKey, true)
		text, err = d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text, captureErrorDetailKey+" must be reloadable")
		require.Equal(t, 1, rowsFor(t, pg.DB, jobRunID))

		// The remaining knobs act on an already pinned run, so they are observable
		// without another round trip.
		d.cfg().Set(maxErrorLengthKey, 2)
		text, err = d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "bo", text, maxErrorLengthKey+" must be reloadable")

		d.cfg().Set(maxErrorLengthKey, defaultMaxErrorLength)
		d.cfg().Set(blockedConnectionsKey, []string{dbTestSourceID + ":" + testDestID})
		text, err = d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Empty(t, text, blockedConnectionsKey+" must be reloadable")

		d.cfg().Set(blockedConnectionsKey, []string{})
		d.cfg().Set(blockedWorkspacesKey, []string{testWorkspace})
		text, err = d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Empty(t, text, blockedWorkspacesKey+" must be reloadable")

		d.cfg().Set(blockedWorkspacesKey, []string{})
		text, err = d.ask(ctx, jobRunID)
		require.NoError(t, err)
		require.Equal(t, "boom", text, "lifting the blacklist must take effect without a restart")

		require.Equal(t, 3, d.counter(rsourcesErrorCaptured), "one captured count per capturing call")
		require.Equal(t, 1, d.counter(rsourcesErrorClipped))
	})

	t.Run("the constructor's context owns the subscription for the instance's life", func(t *testing.T) {
		// NewSyncSettingDelegate subscribes with the context it is handed, and
		// ConfigSubscriberRoutine only ends when that subscription's channel closes.
		// So the constructor's context is a process lifetime, not a setup timeout: a
		// short-lived one would tear the subscription down immediately, leave the
		// connection index empty forever, and turn every first miss into a blocked
		// awaitConfig. This pins the contract in both directions.
		bus := pubsub.New()
		subCtx, cancelSub := context.WithCancel(ctx)
		deps := newDelegateDepsWith(t, dsn,
			func(c context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				return bus.Subscribe(c, string(topic))
			})
		d, err := deps.build(subCtx)
		require.NoError(t, err)

		routine := make(chan error, 1)
		go func() { routine <- d.ConfigSubscriberRoutine(context.Background()) }()

		bus.Publish(string(backendconfig.TopicProcessConfig),
			connectionsPayload(map[string][]backendconfig.Connection{
				testWorkspace: {connectionWith(dbTestSourceID, testDestID, true)},
			}))
		require.Eventually(t, func() bool {
			return d.connectionEnabled(dbTestSourceID, testDestID)
		}, 30*time.Second, 10*time.Millisecond, "a real push must reach the index through the routine")

		text, err := d.GetErrorResponse(ctx,
			statKey{jobRunId: "db-lifecycle", JobTargetKey: JobTargetKey{SourceID: dbTestSourceID, DestinationID: testDestID}},
			abortedStatus(`{"response":"boom"}`))
		require.NoError(t, err)
		require.Equal(t, "boom", text)

		cancelSub()
		select {
		case err := <-routine:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("the subscriber routine did not exit when the constructor's context was cancelled")
		}
		d.Stop() // must return rather than block on the routine's waitgroup
	})

	t.Run("a closed instance reports database failures instead of swallowing them", func(t *testing.T) {
		d := newDBDelegate(t, dsn)
		d.index(true)
		d.Stop()

		text, err := d.ask(ctx, "db-after-stop")
		require.Error(t, err, "a shut down pod must not report an empty capture as success")
		require.ErrorContains(t, err, "pin decision",
			"the failure must be attributed to the pin lookup, not swallowed into an empty text")
		require.Empty(t, text)
		require.Nil(t, d.cached("db-after-stop"), "a failed lookup must not cache anything")
		require.Zero(t, rowsFor(t, pg.DB, "db-after-stop"), "a failed lookup must not have pinned anything")
	})
}

// counter reads one of the delegate's capture counters for the db test's connection.
func (d *dbDelegate) counter(name string) int {
	tags := map[string]string{
		"sourceId": dbTestSourceID, "destinationId": testDestID, "workspaceId": testWorkspace,
	}
	m := d.statsStore().Get(name, tags)
	if m == nil {
		return 0
	}
	return int(m.LastValue())
}
