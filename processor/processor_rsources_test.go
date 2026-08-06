package processor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	testwebhook "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// rsourcesScenario parametrises a single run of the rsources accounting test. A single
// retl-tracked source fans out to `destinations` WEBHOOK destinations; indices in `forked`
// are additionally configured for destination isolation (siphoned to the intermediate proc
// jobsdb), `filtered` destinations drop every event via a user transformation, and `aborted`
// destinations are drained (aborted) by the router without ever attempting delivery. A
// destination index may appear in `forked` and `filtered` at once — that combination
// exercises the interesting cell: a proc-consumer status update with nothing ever reaching
// the router.
type rsourcesScenario struct {
	name             string
	events           int
	destinations     int
	forked           []int
	filtered         []int
	aborted          []int
	isolationEnabled bool // Processor.DestinationIsolation.enabled
	forkRetl         bool // Processor.DestinationIsolation.forkRsourcesTrackedJobs
}

// TestProcessorRsourcesStats exercises rsources accounting (In/Out/Failed, Completed,
// failed-records) across the gw->proc fork hop for retl-tracked (SourceJobRunID-carrying)
// events. It verifies both the forked and non-forked paths through the same fan-out
// mechanics as TestProcessorDestinationIsolation, but asserts on the rsources job-status API
// instead of raw router/proc counts.
func TestProcessorRsourcesStats(t *testing.T) {
	for _, sc := range []rsourcesScenario{
		{
			name:         "inline",
			events:       3,
			destinations: 3,
			filtered:     []int{2},
			aborted:      []int{1},
		},
		{
			name:             "retl_excluded",
			events:           3,
			destinations:     3,
			filtered:         []int{2},
			aborted:          []int{1},
			isolationEnabled: true,
			forked:           []int{0, 1, 2},
			forkRetl:         false,
		},
		{
			name:             "all_forked",
			events:           3,
			destinations:     3,
			filtered:         []int{2},
			aborted:          []int{1},
			isolationEnabled: true,
			forked:           []int{0, 1, 2},
			forkRetl:         true,
		},
		{
			// destination 0: inline, delivered
			// destination 1: forked, aborted at the router
			// destination 2: inline, filtered by user transformation
			// destination 3: forked AND filtered -- a proc-consumer status update with
			// nothing ever reaching the router.
			name:             "mixed",
			events:           4,
			destinations:     4,
			forked:           []int{1, 3},
			filtered:         []int{2, 3},
			aborted:          []int{1},
			isolationEnabled: true,
			forkRetl:         true,
		},
	} {
		t.Run(sc.name, func(t *testing.T) {
			procRsourcesScenario(t, sc)
		})
	}
}

func procRsourcesScenario(t *testing.T, sc rsourcesScenario) {
	t.Helper()
	require.GreaterOrEqual(t, sc.destinations, len(sc.filtered), "cannot filter more destinations than exist")
	require.LessOrEqual(t, len(sc.forked), sc.destinations, "cannot fork more destinations than exist")

	config.Reset()
	defer config.Reset()
	defer logger.Reset()
	logger.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// filtered destinations get a user transformation that drops every event. It is fetched
	// by the transformer container from an in-process config backend.
	const filterTransformationVersionID = "rsources-test-filter-v1"
	transformerOpts := []transformertest.Option{}
	if len(sc.filtered) > 0 {
		transformerConfigBE := newTransformerConfigBackend(t, map[string]string{
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
		postgresContainer, err = postgres.Setup(pool, t,
			postgres.WithOptions("max_connections=1000"),
			postgres.WithShmSize(256*bytesize.MB),
			postgres.WithTag("17-alpine"),
		)
		return err
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t, transformerOpts...)
		return err
	})
	require.NoError(t, containersGroup.Wait())

	const (
		workspaceID = "rsources-test-workspace"
		sourceID    = "rsources-test-source"
		writeKey    = "rsources-test-writekey"
	)
	destIDs := make([]string, sc.destinations)
	destinations := make([]map[string]any, sc.destinations)
	recorders := make([]*testwebhook.Recorder, sc.destinations)
	filteredSet := toSet(sc.filtered)
	forkedSet := toSet(sc.forked)
	abortedSet := toSet(sc.aborted)
	for i := range destIDs {
		destIDs[i] = trand.String(27)
		recorders[i] = testwebhook.NewRecorder()
		t.Cleanup(recorders[i].Close)
		versionID := ""
		if filteredSet[i] {
			versionID = filterTransformationVersionID
		}
		destinations[i] = map[string]any{
			"id":                      destIDs[i],
			"webhookUrl":              recorders[i].Server.URL,
			"transformationVersionID": versionID,
		}
	}

	configJSONPath := workspaceConfig.CreateTempFile(t, "testdata/procRsourcesTestTemplate.json.tpl", map[string]any{
		"workspaceId":  workspaceID,
		"sourceId":     sourceID,
		"writeKey":     writeKey,
		"destinations": destinations,
	})
	mockCBE := (procIsolationMethods{}).newMockConfigBackend(t, configJSONPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)

	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "rsources_test")
	config.Set("HOSTED_SERVICE_SECRET", "rsources_test_secret")
	config.Set("recovery.storagePath", path.Join(t.TempDir(), "/recovery_data.json"))

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
	config.Set("JobsDB.compactionLoopSleepDuration", "60m")
	config.Set("JobsDB.proc.addNewDSLoopSleepDuration", "1s")
	if len(sc.aborted) > 0 {
		abortedDestIDs := make([]string, 0, len(sc.aborted))
		for i := range destIDs {
			if abortedSet[i] {
				abortedDestIDs = append(abortedDestIDs, destIDs[i])
			}
		}
		config.Set("Router.toAbortDestinationIDs", abortedDestIDs)
	}
	config.Set("archival.Enabled", false)
	config.Set("enableStats", false)

	config.Set("Processor.pipelinesPerPartition", 1)
	config.Set("Processor.pingerSleep", "100ms")
	config.Set("Processor.readLoopSleep", "100ms")
	config.Set("Processor.maxLoopProcessEvents", 4000)
	config.Set("Processor.subJobSize", 1000)
	config.Set("JobsDB.enableWriterQueue", false)

	// internal endpoints (retl ingestion + job-status) are enabled independent of the
	// enterprise token via this override.
	config.Set("Gateway.internalEndpointsEnabled", true)

	if sc.isolationEnabled {
		config.Set("Processor.DestinationIsolation.enabled", true)
		for i := range destIDs {
			if forkedSet[i] {
				config.Set("Processor.DestinationIsolation.enabledDestinations."+destIDs[i], true)
			}
		}
		config.Set("Processor.DestinationIsolation.forkRsourcesTrackedJobs", sc.forkRetl)
	}

	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(t, err)
	gatewayPort := strconv.Itoa(httpPortInt)
	config.Set("Gateway.webPort", gatewayPort)
	config.Set("RUDDER_TMPDIR", os.TempDir())

	svcDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("rudder-server panicked: %v", r)
				close(svcDone)
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		if c := r.Run(ctx, cancel, []string{"proc-rsources-test"}); c != 0 {
			t.Errorf("rudder-server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()

	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%s/health", gatewayPort),
		200*time.Second, 100*time.Millisecond, t.Name(),
	)

	jobRunID := trand.String(27)
	taskRunID := trand.String(27)
	sendRsourcesRetlBatch(t, gatewayPort, sourceID, jobRunID, taskRunID, sc.events)

	// Completed implies every gw job processed, every forked consumer drained, and every
	// destination row balanced (see mapper.go calculateCompleted) -- so it's a sound single
	// wait condition; the jobsdb assertions below double-check the two views agree instead
	// of racing a partial-drain window.
	var status rsources.JobStatus
	require.Eventually(t, func() bool {
		var found bool
		status, found = rsourcesJobStatus(t, gatewayPort, jobRunID, taskRunID)
		return found && sourceCompleted(status, sourceID)
	}, 120*time.Second, 200*time.Millisecond, "source should be reported completed")

	db := postgresContainer.DB
	require.Eventually(t, func() bool {
		return countDistinctJobs(t, db, "gw", "job_state = 'succeeded'") == sc.events
	}, 30*time.Second, 200*time.Millisecond, "all gateway jobs should be marked succeeded")

	passthrough := 0
	for i := range destIDs {
		if !filteredSet[i] {
			passthrough++
		}
	}
	require.Eventually(t, func() bool {
		return countDistinctJobs(t, db, "rt", "") == passthrough*sc.events
	}, 30*time.Second, 200*time.Millisecond, "router should receive one job per (event, unfiltered destination)")

	expectedProcJobs := 0
	if len(sc.forked) > 0 && sc.forkRetl {
		expectedProcJobs = sc.events
	}
	require.Equal(t, expectedProcJobs, countJobs(t, db, "proc"), "unexpected number of forked proc jobs")

	source, ok := findSourceStatus(status, sourceID)
	require.True(t, ok, "job-status response should contain source %s", sourceID)

	forkedCount := 0
	if sc.forkRetl {
		forkedCount = len(sc.forked)
	}
	droppedCount := len(sc.filtered)
	expectedIn := uint(sc.events) + uint(sc.events*forkedCount) + uint(sc.events*droppedCount)
	expectedOut := uint(sc.events) + uint(sc.events*forkedCount)
	expectedFailed := uint(sc.events * droppedCount)
	require.Equal(t, expectedIn, source.Stats.In, "unexpected source-level In")
	require.Equal(t, expectedOut, source.Stats.Out, "unexpected source-level Out")
	require.Equal(t, expectedFailed, source.Stats.Failed, "unexpected source-level Failed")
	require.True(t, source.Completed, "source should be completed")

	for i := range destIDs {
		destStatus, found := findDestinationStatus(source, destIDs[i])
		switch {
		case filteredSet[i]:
			require.False(t, found, "filtered destination %d should have no destination-level rsources row", i)
		case abortedSet[i]:
			require.True(t, found, "aborted destination %d should have a destination-level rsources row", i)
			require.Equal(t, uint(sc.events), destStatus.Stats.Failed, "aborted destination %d Failed", i)
			require.Equal(t, uint(sc.events), destStatus.Stats.In, "aborted destination %d In", i)
			require.Zero(t, destStatus.Stats.Out, "aborted destination %d Out", i)
		default:
			require.True(t, found, "delivered destination %d should have a destination-level rsources row", i)
			require.Equal(t, uint(sc.events), destStatus.Stats.In, "delivered destination %d In", i)
			require.Equal(t, uint(sc.events), destStatus.Stats.Out, "delivered destination %d Out", i)
			require.Zero(t, destStatus.Stats.Failed, "delivered destination %d Failed", i)
		}
	}

	for i := range destIDs {
		switch {
		case filteredSet[i], abortedSet[i]:
			require.Zero(t, recorders[i].RequestsCount(), "destination %d should not receive any webhook calls", i)
		default:
			require.Equal(t, sc.events, recorders[i].RequestsCount(), "destination %d should receive one webhook call per event", i)
		}
	}

	// Failed records: the gateway never puts record_id in gw job params, and JobsDropped
	// (filtered/dropped events) never calls CollectFailedRecords -- so the source-level
	// records list stays empty even though Failed is non-zero. Only router-aborted
	// destinations produce records, one per event, resolved back to the recordId sent.
	failedRecords := rsourcesFailedRecords(t, gatewayPort, jobRunID, taskRunID)
	failedSource, ok := findSourceFailedRecords(failedRecords, sourceID)
	require.True(t, ok, "failed-records response should contain source %s", sourceID)
	require.Empty(t, failedSource.Records, "source-level failed records should stay empty")

	for i := range destIDs {
		destRecords, found := findDestinationFailedRecords(failedSource, destIDs[i])
		if !abortedSet[i] {
			require.False(t, found, "destination %d should have no failed records", i)
			continue
		}
		require.True(t, found, "aborted destination %d should have failed records", i)
		require.Len(t, destRecords.Records, sc.events, "aborted destination %d failed record count", i)
		gotRecordIDs := make(map[string]bool, len(destRecords.Records))
		for _, r := range destRecords.Records {
			var recordID string
			require.NoError(t, jsonrs.Unmarshal(r.Record, &recordID))
			gotRecordIDs[recordID] = true
		}
		for e := 0; e < sc.events; e++ {
			require.True(t, gotRecordIDs["r"+strconv.Itoa(e)], "aborted destination %d missing failed record for event %d", i, e)
		}
	}

	cancel()
	<-svcDone
}

func toSet(indices []int) map[int]bool {
	set := make(map[int]bool, len(indices))
	for _, i := range indices {
		set[i] = true
	}
	return set
}

func sourceCompleted(status rsources.JobStatus, sourceID string) bool {
	source, ok := findSourceStatus(status, sourceID)
	return ok && source.Completed
}

func findSourceStatus(status rsources.JobStatus, sourceID string) (rsources.SourceStatus, bool) {
	for _, task := range status.TasksStatus {
		for _, source := range task.SourcesStatus {
			if source.ID == sourceID {
				return source, true
			}
		}
	}
	return rsources.SourceStatus{}, false
}

func findDestinationStatus(source rsources.SourceStatus, destinationID string) (rsources.DestinationStatus, bool) {
	for _, dest := range source.DestinationsStatus {
		if dest.ID == destinationID {
			return dest, true
		}
	}
	return rsources.DestinationStatus{}, false
}

func findSourceFailedRecords(records rsources.JobFailedRecordsV2, sourceID string) (rsources.SourceFailedRecords[rsources.FailedRecord], bool) {
	for _, task := range records.Tasks {
		for _, source := range task.Sources {
			if source.ID == sourceID {
				return source, true
			}
		}
	}
	return rsources.SourceFailedRecords[rsources.FailedRecord]{}, false
}

func findDestinationFailedRecords(source rsources.SourceFailedRecords[rsources.FailedRecord], destinationID string) (rsources.DestinationFailedRecords[rsources.FailedRecord], bool) {
	for _, dest := range source.Destinations {
		if dest.ID == destinationID {
			return dest, true
		}
	}
	return rsources.DestinationFailedRecords[rsources.FailedRecord]{}, false
}

// sendRsourcesRetlBatch posts a single batch of `count` retl records (distinct userId,
// recordId) to the internal retl endpoint, all sharing jobRunID/taskRunID -- mirroring a
// single retl task run. No X-Rudder-Destination-Id header is sent, so the source fans out
// to every enabled destination.
func sendRsourcesRetlBatch(t testing.TB, gatewayPort, sourceID, jobRunID, taskRunID string, count int) {
	t.Helper()
	client := &http.Client{}
	reqURL := fmt.Sprintf("http://localhost:%s/internal/v1/retl", gatewayPort)

	var batch bytes.Buffer
	batch.WriteString(`{"batch":[`)
	for i := range count {
		if i > 0 {
			batch.WriteByte(',')
		}
		fmt.Fprintf(&batch,
			`{"userId":%q,"anonymousId":%q,"recordId":%q,"type":"identify","context":{"traits":{"trait1":"v"},"sources":{"job_run_id":%q,"task_run_id":%q}},"timestamp":"2020-02-02T00:23:09.544Z"}`,
			"u"+strconv.Itoa(i), "u"+strconv.Itoa(i), "r"+strconv.Itoa(i), jobRunID, taskRunID)
	}
	batch.WriteString(`]}`)

	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(batch.Bytes()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Rudder-Source-Id", sourceID)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { kithttputil.CloseResponse(resp) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "retl batch should be accepted: %s", body)
}

// rsourcesJobStatus hits the internal job-status endpoint for the given jobRunID, scoped to
// taskRunID. The second return value is false when the job run is not found yet.
func rsourcesJobStatus(t testing.TB, gatewayPort, jobRunID, taskRunID string) (rsources.JobStatus, bool) {
	t.Helper()
	reqURL := fmt.Sprintf("http://localhost:%s/internal/v2/job-status/%s?%s", gatewayPort, jobRunID, url.Values{
		"task_run_id": []string{taskRunID},
	}.Encode())

	resp, err := http.Get(reqURL) //nolint:gosec,noctx // test-only, fixed localhost URL
	require.NoError(t, err)
	defer func() { kithttputil.CloseResponse(resp) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode == http.StatusNotFound {
		return rsources.JobStatus{}, false
	}
	require.Equal(t, http.StatusOK, resp.StatusCode, "job-status request failed: %s", body)

	var status rsources.JobStatus
	require.NoError(t, jsonrs.Unmarshal(body, &status))
	return status, true
}

// rsourcesFailedRecords hits the internal job-status failed-records endpoint for the given
// jobRunID, scoped to taskRunID.
func rsourcesFailedRecords(t testing.TB, gatewayPort, jobRunID, taskRunID string) rsources.JobFailedRecordsV2 {
	t.Helper()
	reqURL := fmt.Sprintf("http://localhost:%s/internal/v2/job-status/%s/failed-records?%s", gatewayPort, jobRunID, url.Values{
		"task_run_id": []string{taskRunID},
	}.Encode())

	resp, err := http.Get(reqURL) //nolint:gosec,noctx // test-only, fixed localhost URL
	require.NoError(t, err)
	defer func() { kithttputil.CloseResponse(resp) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "failed-records request failed: %s", body)

	var records rsources.JobFailedRecordsV2
	require.NoError(t, jsonrs.Unmarshal(body, &records))
	return records
}
