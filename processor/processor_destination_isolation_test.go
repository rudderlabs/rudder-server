package processor_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// destIsolationScenario parametrises a single run of the destination-isolated processing
// pipeline.
type destIsolationScenario struct {
	name         string
	destinations int // number of WEBHOOK destinations the single source fans out to
	events       int // number of distinct identify events sent to the gateway
	// forked is how many of the destinations (from index 0) are siphoned to the
	// intermediate (proc) jobsdb. When it equals destinations, the coarse
	// DestinationIsolation.enabledDestinations.all switch is used; otherwise the leading
	// `forked` destinations are enabled individually and the rest stay inline.
	forked int
	// filtered is how many of the destinations (the trailing ones) get a user
	// transformation that drops every event. Those destinations still incur a proc
	// consumer status update per event, but nothing reaches the router — this is the
	// fan-out cost we want to exercise: a source wired to many destinations that a user
	// transformation filters down to a handful.
	filtered int
	// procCompaction enables the proc jobsdb compaction loop (status cleanup + dataset
	// reclaim). Enabled ⇒ realistic drain but drained proc rows are reclaimed, so the exact
	// proc fan-out assertions are skipped (benchmarks). Disabled ⇒ drained rows persist and
	// are verified (tests). gw/rt never compact, so completion is always detectable.
	procCompaction bool
	// maxProcDSSize caps the number of jobs per proc dataset, controlling how finely the
	// fan-out is split across datasets. Defaults to 100000 when zero.
	maxProcDSSize int
	// timeout bounds how long we wait for the pipeline to drain; defaults to 300s.
	timeout time.Duration
}

// TestProcessorDestinationIsolation exercises the fan-out siphon (write side of the
// destination-isolated processing pipeline). A single source fans out to two WEBHOOK
// destinations; depending on the fork configuration, events for a destination are either
// siphoned to the intermediate (proc) jobsdb and drained by the proc pool, or transformed
// inline in the gw pool. In every case all events must reach the router jobsdb exactly once
// per destination — the fork only changes the path, never the outcome.
func TestProcessorDestinationIsolation(t *testing.T) {
	for _, sc := range []destIsolationScenario{
		{name: "none_forked", destinations: 2, events: 300, forked: 0},
		{name: "some_forked", destinations: 2, events: 300, forked: 1},
		{name: "all_forked", destinations: 2, events: 300, forked: 2},
		{name: "fanout_50", destinations: 50, events: 300, forked: 50, filtered: 48},
	} {
		t.Run(sc.name, func(t *testing.T) {
			procDestinationIsolationScenario(t, sc)
		})
	}
}

// BenchmarkProcessorDestinationIsolation stresses the major fan-out status-update path. A
// single source is wired to many forked destinations while a user transformation filters
// out events for all but the first two — mirroring the common setup where a source is
// connected to 20-50 destinations but a transformation drops events for most of them. With
// destination isolation every event is siphoned to the proc jobsdb and drained per
// destination, so each (event, destination) pair costs a proc consumer status update even
// though only the two unfiltered destinations ever reach the router. The reported time is
// the drain time for events x destinations status updates (e.g. 200k events x 50
// destinations = 10M updates).
func BenchmarkProcessorDestinationIsolation(b *testing.B) {
	for _, sc := range []destIsolationScenario{
		{name: "10_nofork", destinations: 10, forked: 0, filtered: 9, events: 100_000, procCompaction: true, timeout: 10 * time.Minute},
		{name: "10_fork_100kmaxprocds", destinations: 10, forked: 10, filtered: 9, maxProcDSSize: 100_000, events: 100_000, procCompaction: true, timeout: 10 * time.Minute},
		{name: "10_fork_20kmaxprocds", destinations: 10, forked: 10, filtered: 9, maxProcDSSize: 20_000, events: 100_000, procCompaction: true, timeout: 10 * time.Minute},
		{name: "10_fork_10kmaxprocds", destinations: 10, forked: 10, filtered: 9, maxProcDSSize: 10_000, events: 100_000, procCompaction: true, timeout: 10 * time.Minute},

		{name: "20_nofork", destinations: 20, forked: 0, filtered: 19, events: 100_000, procCompaction: true, timeout: 20 * time.Minute},
		{name: "20_fork_100kmaxprocds", destinations: 20, forked: 20, filtered: 19, maxProcDSSize: 100_000, events: 100_000, procCompaction: true, timeout: 20 * time.Minute},
		{name: "20_fork_20kmaxprocds", destinations: 20, forked: 20, filtered: 19, maxProcDSSize: 20_000, events: 100_000, procCompaction: true, timeout: 20 * time.Minute},
		{name: "20_fork_10kmaxprocds", destinations: 20, forked: 20, filtered: 19, maxProcDSSize: 10_000, events: 100_000, procCompaction: true, timeout: 20 * time.Minute},

		{name: "50_nofork", destinations: 50, forked: 0, filtered: 49, events: 100_000, procCompaction: true, timeout: 30 * time.Minute},
		{name: "50_fork_20kmaxprocds", destinations: 50, forked: 50, filtered: 49, maxProcDSSize: 20_000, events: 100_000, procCompaction: true, timeout: 30 * time.Minute},
		{name: "50_fork_10kmaxprocds", destinations: 50, forked: 50, filtered: 49, maxProcDSSize: 10_000, events: 100_000, procCompaction: true, timeout: 30 * time.Minute},
		{name: "50_fork_5kmaxprocds", destinations: 50, forked: 50, filtered: 49, maxProcDSSize: 5_000, events: 100_000, procCompaction: true, timeout: 30 * time.Minute},
	} {
		b.Run(sc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				procDestinationIsolationScenario(b, sc)
			}
		})
	}
}

func procDestinationIsolationScenario(tb testing.TB, sc destIsolationScenario) {
	tb.Helper()
	require.GreaterOrEqual(tb, sc.destinations, sc.filtered, "cannot filter more destinations than exist")
	require.LessOrEqual(tb, sc.forked, sc.destinations, "cannot fork more destinations than exist")
	bench, isBench := tb.(*testing.B)
	if isBench {
		bench.StopTimer()
	}
	events := sc.events
	timeout := sc.timeout
	if timeout == 0 {
		timeout = 300 * time.Second
	}

	config.Reset()
	defer config.Reset()
	defer logger.Reset()
	logger.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(tb, err)

	// The trailing `filtered` destinations get a user transformation that drops every
	// event. It is fetched by the transformer container from an in-process config backend
	// (served through NewTransformerBackendConfigHandler).
	const filterTransformationVersionID = "dest-isolation-filter-v1"
	transformerOpts := []transformertest.Option{}
	if sc.filtered > 0 {
		transformerConfigBE := newTransformerConfigBackend(tb, map[string]string{
			filterTransformationVersionID: `export function transformEvent(event, metadata) { return; }`,
		})
		transformerOpts = append(transformerOpts,
			transformertest.WithConnectionToHostEnabled(),
			transformertest.WithConfigBackendURL(transformerConfigBE),
		)
	}

	var (
		postgresContainer    *postgres.Resource
		transformerContainer *transformertest.Resource
	)
	containersGroup, _ := errgroup.WithContext(ctx)
	containersGroup.Go(func() (err error) {
		postgresContainer, err = postgres.Setup(pool, tb,
			postgres.WithOptions("max_connections=1000"),
			postgres.WithShmSize(256*bytesize.MB),
			postgres.WithTag("17-alpine"),
		)
		return err
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, tb, transformerOpts...)
		return err
	})
	require.NoError(tb, containersGroup.Wait())

	const (
		workspaceID = "dest-isolation-workspace"
		sourceID    = "dest-isolation-source"
		writeKey    = "dest-isolation-writekey"
	)
	// destIDs[0:destinations-filtered] pass through to the router; the trailing ones are
	// filtered by the user transformation.
	destIDs := make([]string, sc.destinations)
	destinations := make([]map[string]any, sc.destinations)
	for i := range destIDs {
		destIDs[i] = trand.String(27)
		versionID := ""
		if i >= sc.destinations-sc.filtered {
			versionID = filterTransformationVersionID
		}
		destinations[i] = map[string]any{"id": destIDs[i], "transformationVersionID": versionID}
	}

	configJSONPath := workspaceConfig.CreateTempFile(tb, "testdata/procDestinationIsolationTestTemplate.json.tpl", map[string]any{
		"webhookUrl":   "http://localhost:1234", // not important, router aborts before delivery
		"workspaceId":  workspaceID,
		"sourceId":     sourceID,
		"writeKey":     writeKey,
		"destinations": destinations,
	})
	mockCBE := (procIsolationMethods{}).newMockConfigBackend(tb, configJSONPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)

	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "proc_destination_isolation_test")
	config.Set("HOSTED_SERVICE_SECRET", "proc_destination_isolation_secret")
	config.Set("recovery.storagePath", path.Join(tb.TempDir(), "/recovery_data.json"))

	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerContainer.TransformerURL)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("AdaptivePayloadLimiter.enabled", false)
	config.Set("JobsDB.backup.enabled", false)
	// Compaction is disabled everywhere by default (this is the compaction-loop interval;
	// migrateDSLoopSleepDuration is its legacy alias). gw/rt must never compact so their
	// job counts stay stable as the completion signal.
	config.Set("JobsDB.compactionLoopSleepDuration", "60m")
	// proc: split into smaller datasets so the consumer drains dataset-by-dataset and skips
	// finished ones, instead of repeatedly scanning one giant status table (events x forked
	// rows). The direct-count helpers sum across datasets, so this stays countable.
	maxDSSize := sc.maxProcDSSize
	if maxDSSize == 0 {
		maxDSSize = 100000
	}
	config.Set("JobsDB.proc.maxDSSize", maxDSSize)
	config.Set("JobsDB.proc.addNewDSLoopSleepDuration", "1s")
	if sc.procCompaction {
		// enable compaction for proc only (prefix key wins over the global disable above):
		// reclaim drained datasets as the fan-out drains, as it would in production.
		config.Set("JobsDB.proc.compactionLoopSleepDuration", "10s")
	}
	config.Set("Router.toAbortDestinationIDs", destIDs) // abort at the router; we only assert on jobsdb state
	config.Set("archival.Enabled", false)
	config.Set("enableStats", false)

	config.Set("Processor.pipelinesPerPartition", 1)
	config.Set("Processor.pingerSleep", "1s")
	config.Set("Processor.readLoopSleep", "1s")
	config.Set("Processor.maxLoopProcessEvents", 4000)
	config.Set("Processor.subJobSize", 1000)
	config.Set("JobsDB.enableWriterQueue", false)

	// destination isolation: fork per configuration
	config.Set("Processor.DestinationIsolation.enabled", true)
	if sc.forked == sc.destinations && sc.forked > 0 {
		config.Set("Processor.DestinationIsolation.enabledDestinations.all", true)
	} else {
		// enable only the leading `forked` destinations; the rest stay inline
		for i := 0; i < sc.forked; i++ {
			config.Set("Processor.DestinationIsolation.enabledDestinations."+destIDs[i], true)
		}
	}

	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(tb, err)
	gatewayPort := strconv.Itoa(httpPortInt)
	config.Set("Gateway.webPort", gatewayPort)
	config.Set("RUDDER_TMPDIR", os.TempDir())

	svcDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				tb.Errorf("rudder-server panicked: %v", r)
				close(svcDone)
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		if c := r.Run(ctx, cancel, []string{"proc-destination-isolation-test"}); c != 0 {
			tb.Errorf("rudder-server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()

	health.WaitUntilReady(ctx, tb,
		fmt.Sprintf("http://localhost:%s/health", gatewayPort),
		200*time.Second, 100*time.Millisecond, tb.Name(),
	)

	// Setup is done; time only the send + drain of the fan-out.
	start := time.Now()
	if isBench {
		bench.StartTimer()
	}

	sendEvents(tb, gatewayPort, writeKey, events)

	db := postgresContainer.DB
	// all gateway jobs are processed (forked and/or transformed inline)
	require.Eventually(tb, func() bool {
		return countDistinctJobs(tb, db, "gw", "job_state = 'succeeded'") == events
	}, timeout, 500*time.Millisecond, "all gateway jobs should be marked succeeded")

	// every event reaches the router once per unfiltered destination (the filtered
	// destinations are dropped by the user transformation and never reach the router).
	// gw and rt never compact (see config above), so this is our stable completion signal
	// in every scenario, including when proc compaction is on and its rows are reclaimed.
	unfilteredDestinations := sc.destinations - sc.filtered
	require.Eventually(tb, func() bool {
		return countDistinctJobs(tb, db, "rt", "") == unfilteredDestinations*events
	}, timeout, 500*time.Millisecond, "router should receive one job per (event, unfiltered destination)")

	// Exact proc-side fan-out verification. Only meaningful when proc compaction is off:
	// with compaction on (benchmarks) drained proc datasets are reclaimed, so neither the
	// job count nor the succeeded-status count survives to be counted.
	if !sc.procCompaction {
		// proc jobsdb: one job per event that has at least one forked destination; empty
		// when nothing is forked. Counted straight off the proc_jobs table.
		expectedProcJobs := 0
		if sc.forked > 0 {
			expectedProcJobs = events
		}
		require.Equal(tb, expectedProcJobs, countJobs(tb, db, "proc"), "unexpected number of forked proc jobs")

		if sc.forked > 0 {
			// Every forked (event, destination) pair drains to a succeeded proc consumer
			// status — set when the job is consumed (at rebuild), before the user
			// transformation drops the event for the filtered destinations. So this count is
			// both the drain signal and the fan-out assertion: exactly `forked` succeeded
			// statuses per event.
			//
			// A succeeded status is written at most once per (job, consumer), so we count
			// them directly on the proc_job_status table(s) — no "latest status per (job,
			// consumer)" view (v_last_c_) and no jobs join, which is what makes
			// unionjobsdbmetadata scan millions of rows here (events x forked).
			require.Eventually(tb, func() bool {
				return countJobStatuses(tb, db, "proc", "job_state = 'succeeded'") == sc.forked*events
			}, timeout, 2*time.Second, "each forked event should drain to a succeeded status for exactly %d consumer(s)", sc.forked)
		}
	}

	elapsed := time.Since(start)
	if isBench {
		bench.StopTimer()
		statusUpdates := float64(sc.events * sc.forked)
		bench.ReportMetric(statusUpdates/elapsed.Seconds(), "statusUpdates/s")
		bench.ReportMetric(float64(sc.events)/elapsed.Seconds(), "events/s")
	}
	tb.Logf("[%s] drained %d events x %d destinations (%d forked, %d filtered) in %s",
		sc.name, sc.events, sc.destinations, sc.forked, sc.filtered, elapsed)

	cancel()
	<-svcDone
}

// newTransformerConfigBackend starts an in-process config backend that serves the given
// user transformations (keyed by version id) to the transformer container, and returns a
// URL reachable from inside the container. It is torn down with the test.
func newTransformerConfigBackend(tb testing.TB, transformations map[string]string) string {
	tb.Helper()
	srv := httptest.NewUnstartedServer(transformertest.NewTransformerBackendConfigHandler(transformations))
	// The transformer container reaches this server through host.docker.internal. On
	// Linux that resolves to the docker bridge IP, so the server must listen on all
	// interfaces — httptest's default 127.0.0.1 binding is unreachable from inside the
	// container. macOS Docker Desktop forwards host.docker.internal to the host loopback,
	// so the default binding is fine there.
	if runtime.GOOS != "darwin" {
		ln, err := net.Listen("tcp", "0.0.0.0:0") //nolint:gosec // deliberate: must be reachable from the docker bridge
		require.NoError(tb, err)
		_ = srv.Listener.Close()
		srv.Listener = ln
	}
	srv.Start()
	tb.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(tb, err)
	return fmt.Sprintf("http://host.docker.internal:%s", port)
}

// sendEvents posts `count` identify events (distinct userIDs) to the gateway in gzipped
// batches of 10.
func sendEvents(t testing.TB, gatewayPort, writeKey string, count int) {
	t.Helper()
	client := &http.Client{}
	url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
	const batchSize = 100
	for start := 0; start < count; start += batchSize {
		func() {
			var batch bytes.Buffer
			batch.WriteString(`{"batch":[`)
			for i := start; i < start+batchSize && i < count; i++ {
				if i > start {
					batch.WriteByte(',')
				}
				fmt.Fprintf(&batch, `{"userId":%q,"anonymousId":%q,"type":"identify","context":{"traits":{"trait1":"v"}},"timestamp":"2020-02-02T00:23:09.544Z"}`,
					strconv.Itoa(i), strconv.Itoa(i))
			}
			batch.WriteString(`]}`)

			var gz bytes.Buffer
			w := gzip.NewWriter(&gz)
			_, err := w.Write(batch.Bytes())
			require.NoError(t, err)
			require.NoError(t, w.Close())

			req, err := http.NewRequest(http.MethodPost, url, io.Reader(&gz))
			require.NoError(t, err)
			req.Header.Set("Content-Encoding", "gzip")
			req.SetBasicAuth(writeKey, "password")
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { kithttputil.CloseResponse(resp) }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}()
	}
}

// countDistinctJobs counts distinct job ids in a jobsdb (across its datasets), optionally
// filtered by a job-state predicate over unionjobsdbmetadata.
func countDistinctJobs(t testing.TB, db *sql.DB, prefix, where string) int {
	t.Helper()
	query := fmt.Sprintf("SELECT count(DISTINCT job_id) FROM unionjobsdbmetadata('%s', 10)", prefix)
	if where != "" {
		query += " WHERE " + where
	}
	var count int
	require.NoError(t, db.QueryRow(query).Scan(&count))
	return count
}

// jobsdbTables returns the live (non pre-drop) dataset tables of the given kind ("jobs" or
// "job_status") for a jobsdb prefix, ordered by dataset index. Used by the direct-count
// helpers below to sum across datasets without going through unionjobsdbmetadata.
func jobsdbTables(t testing.TB, db *sql.DB, prefix, kind string) []string {
	t.Helper()
	rows, err := db.Query(
		`SELECT c.relname FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
		 WHERE c.relkind = 'r'
		   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		   AND c.relname ~ ('^' || $1 || '_' || $2 || '_[0-9]+$')
		   AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
		 ORDER BY c.relname ASC`, prefix, kind)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	return tables
}

// countJobs counts the jobs in a jobsdb straight off its _jobs tables — no status join.
func countJobs(t testing.TB, db *sql.DB, prefix string) int {
	t.Helper()
	total := 0
	for _, tbl := range jobsdbTables(t, db, prefix, "jobs") {
		var count int
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q`, tbl)).Scan(&count))
		total += count
	}
	return total
}

// countJobStatuses counts _job_status rows matching the given predicate directly, summed
// across datasets. It deliberately bypasses the v_last_c_ "latest status per (job,
// consumer)" view and the jobs join that unionjobsdbmetadata performs: for a terminal
// predicate (e.g. job_state = 'succeeded'), a status is written at most once per (job,
// consumer), so the raw count equals the number of matching (job, consumer) pairs — without
// the millions-of-rows DISTINCT ON + fan-out join.
func countJobStatuses(t testing.TB, db *sql.DB, prefix, where string) int {
	t.Helper()
	total := 0
	for _, tbl := range jobsdbTables(t, db, prefix, "job_status") {
		query := fmt.Sprintf(`SELECT count(*) FROM %q`, tbl)
		if where != "" {
			query += " WHERE " + where
		}
		var count int
		require.NoError(t, db.QueryRow(query).Scan(&count))
		total += count
	}
	return total
}
