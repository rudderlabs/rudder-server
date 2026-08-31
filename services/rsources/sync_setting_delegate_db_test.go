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
// would dominate the runtime); each one uses its own job run ids, and the two
// table-creation cases drop the table first and rebuild it themselves.

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
	// The advisory lock is only exercised if several racers can hold a connection at
	// once; the production default of 2 would serialise them in the pool instead.
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

// build runs the real constructor: table setup, load-all and the subscription.
func (deps *delegateDeps) build(ctx context.Context) (*syncSettingDelegate, error) {
	return NewSyncSettingDelegate(ctx, deps.conf, deps.log, deps.stats, deps.bc)
}

type dbDelegate struct {
	*syncSettingDelegate
	deps *delegateDeps
}

// The accessors are named away from the embedded struct's own `conf` and `log` fields
// so that a reader never has to work out which one a selector resolves to.
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

func syncSettingsTableCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = current_schema() AND table_name = $1`, syncSettingsTable).Scan(&n))
	return n
}

func dropSyncSettingsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + syncSettingsTable)
	require.NoError(t, err)
	require.Zero(t, syncSettingsTableCount(t, db))
}

func TestSyncSettingDelegateDB(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	dsn := pg.DBDsn
	ctx := context.Background()

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

		entry, cached := second.cachedDecision(jobRunID)
		require.True(t, cached, "the pin must be loaded at startup, not on first use")
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
		}
	})

	t.Run("two instances racing one new run agree on one stored answer", func(t *testing.T) {
		// The production-normal case: two pods, one database, a brand new job run, and
		// control-plane views that disagree. Only the advisory lock makes this come out
		// with a single row and a single answer - a primary key alone would give the
		// loser a unique violation.
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

			// Both caches must now hold that one stored decision.
			for name, d := range map[string]*dbDelegate{"on": on, "off": off} {
				entry, cached := d.cachedDecision(jobRunID)
				require.Truef(t, cached, "round %d: the %s instance did not cache the decision", round, name)
				require.Equalf(t, value, entry.storeErrorResponses,
					"round %d: the %s instance cached something other than the stored row", round, name)
			}
		}
	})

	t.Run("two instances created concurrently against an empty database both start", func(t *testing.T) {
		// A deploy rolls many pods at once and they all run CREATE TABLE IF NOT EXISTS
		// against the same database. That statement is not race free in postgres.
		dropSyncSettingsTable(t, pg.DB)

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
		require.Equal(t, 1, syncSettingsTableCount(t, pg.DB))
	})

	t.Run("the sweep deletes and evicts by age and tolerates losing the race", func(t *testing.T) {
		const (
			oldRun   = "db-cleanup-old"
			freshRun = "db-cleanup-fresh"
		)
		// A row pinned two days ago by some other pod, inserted the way that pod would
		// have: the sweep's cutoff is the database's clock, not this process'.
		_, err := pg.DB.Exec(
			`INSERT INTO `+syncSettingsTable+` (job_run_id, store_error_responses, created_at)
			 VALUES ($1, true, NOW() - INTERVAL '48 hours')
			 ON CONFLICT (job_run_id) DO UPDATE SET created_at = EXCLUDED.created_at`, oldRun)
		require.NoError(t, err)

		d := newDBDelegate(t, dsn, func(c *config.Config) {
			c.Set(syncSettingsMaxAgeKey, "24h")
			c.Set(syncSettingsCleanupFrequencyKey, "10ms")
		})
		d.index(true)
		// loadAll ran before the out-of-band insert, so adopt it the way a miss would.
		require.NoError(t, d.loadAll(ctx))
		_, cached := d.cachedDecision(oldRun)
		require.True(t, cached, "the expired row must be in the cache for the eviction to be observable")

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
		require.Eventually(t, func() bool {
			if countRows(oldRun) != 0 {
				return false
			}
			_, stillCached := d.cachedDecision(oldRun)
			return !stillCached
		}, 30*time.Second, 10*time.Millisecond, "the expired decision must leave both the table and the cache")

		require.Equal(t, 1, rowsFor(t, pg.DB, freshRun), "a decision inside maxAge must survive the sweep")
		entry, cached := d.cachedDecision(freshRun)
		require.True(t, cached, "a decision inside maxAge must survive the cache eviction")
		require.True(t, entry.storeErrorResponses)

		// Another pod (here: this test) deletes the rows out of band. The sweep runs
		// every 10ms and must keep going without complaining.
		_, err = pg.DB.Exec(`DELETE FROM ` + syncSettingsTable)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond) // several more sweeps over an empty table

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
		_, cached := a.cachedDecision(jobRunID)
		require.False(t, cached, "the run must be unknown to A at this point")

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
		entry, cached := a.cachedDecision(jobRunID)
		require.True(t, cached)
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
		require.ErrorContains(t, err, "begin transaction",
			"the failure must be attributed to the pin lookup, not swallowed into an empty text")
		require.Empty(t, text)
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
