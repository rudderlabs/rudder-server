package router_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo/mutable"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestRouterIsolation(t *testing.T) {
	const (
		workspaces       = 10
		jobsPerWorkspace = 100
	)
	runner := func(mode isolation.Mode) func(t *testing.T) {
		return func(t *testing.T) {
			spec := NewRouterIsolationScenarioSpec(mode, workspaces, jobsPerWorkspace)
			duration := RouterIsolationScenario(t, spec)
			t.Logf("Total processing duration: %v", duration)
		}
	}
	t.Run("no isolation", runner(isolation.ModeNone))
	t.Run("workspace isolation", runner(isolation.ModeWorkspace))
	t.Run("destination isolation", runner(isolation.ModeDestination))
}

// https://snapshots.raintank.io/dashboard/snapshot/CLX01r5Nixc3XCrU2P2s3LMF0ZYr0bv5
//
//	go test \
//		-timeout 3600s \
//		-run=^$ \
//		-bench ^BenchmarkRouterIsolationModes$ \
//		github.com/rudderlabs/rudder-server/router \
//		-v \
//		-count=1 |grep BenchmarkRouterIsolationModes
//
// BenchmarkRouterIsolationModes
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_10_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_10_total_jobs_200000-10         	       1	45812381000 ns/op	        18.87 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_10_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_10_total_jobs_200000-10    	       1	34191585541 ns/op	         9.202 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_10_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_10_total_jobs_200000-10  	       1	33344584833 ns/op	         9.274 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_50_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_50_total_jobs_200000-10         	       1	43069780000 ns/op	        19.53 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_50_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_50_total_jobs_200000-10    	       1	35545520458 ns/op	        11.40 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_50_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_50_total_jobs_200000-10  	       1	33370379667 ns/op	        10.73 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_100_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_100_total_jobs_200000-10        	       1	43294015292 ns/op	        19.31 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_100_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_100_total_jobs_200000-10   	       1	33983658500 ns/op	        12.02 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_100_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_100_total_jobs_200000-10 	       1	32738304666 ns/op	        12.05 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_200_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_200_total_jobs_200000-10        	       1	43608479167 ns/op	        19.31 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_200_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_200_total_jobs_200000-10   	       1	35363338583 ns/op	        13.50 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_200_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_200_total_jobs_200000-10 	       1	35054504167 ns/op	        13.24 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_500_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_500_total_jobs_200000-10        	       1	44139415000 ns/op	        19.14 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_500_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_500_total_jobs_200000-10   	       1	54104970708 ns/op	        34.52 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_500_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_500_total_jobs_200000-10 	       1	54438067375 ns/op	        37.20 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_1000_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_none_cardinality_1000_total_jobs_200000-10       	       1	44462469667 ns/op	        18.96 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_1000_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_workspace_cardinality_1000_total_jobs_200000-10  	       1	103662513458 ns/op	        77.08 overall_duration_sec
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_1000_total_jobs_200000
// BenchmarkRouterIsolationModes/isolation_mode_destination_cardinality_1000_total_jobs_200000-10          1	104854786459 ns/op	        81.27 overall_duration_sec
func BenchmarkRouterIsolationModes(b *testing.B) {
	debug.SetMemoryLimit(2 * bytesize.GB)
	benchAllModes := func(b *testing.B, cardinality, totalJobs int) {
		bench := func(mode isolation.Mode, cardinality, workspacesCount, eventsPerworkspace int) {
			title := fmt.Sprintf("isolation mode %s cardinality %d total jobs %d", mode, cardinality, totalJobs)
			b.Run(title, func(b *testing.B) {
				stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "start"}).Increment()
				defer stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "end"}).Increment()
				spec := NewRouterIsolationScenarioSpec(mode, workspacesCount, eventsPerworkspace)
				overallDuration := RouterIsolationScenario(b, spec)
				b.ReportMetric(overallDuration.Seconds(), "overall_duration_sec")
			})
		}
		bench(isolation.ModeNone, cardinality, cardinality, totalJobs/cardinality)
		bench(isolation.ModeWorkspace, cardinality, cardinality, totalJobs/cardinality)
		bench(isolation.ModeDestination, cardinality, cardinality, totalJobs/cardinality)
	}
	cardinality := 200_000
	benchAllModes(b, 10, cardinality)
	benchAllModes(b, 50, cardinality)
	benchAllModes(b, 100, cardinality)
	benchAllModes(b, 200, cardinality)
	benchAllModes(b, 500, cardinality)
	benchAllModes(b, 1000, cardinality)
}

// NewRouterIsolationScenarioSpec is a specification for a router isolation scenario.
// - isolationMode is the isolation mode to use.
// - workspaces is the number of workspaces to use.
// - eventsPerWorkspace is the number of events to send per workspace.
//
// The generated spec's jobs will be split in one webhook destination for every workspace.
func NewRouterIsolationScenarioSpec(isolationMode isolation.Mode, workspaces, eventsPerWorkspace int) *RtIsolationScenarioSpec {
	var s RtIsolationScenarioSpec
	s.isolationMode = isolationMode
	s.jobs = make([]*rtIsolationJobSpec, workspaces*eventsPerWorkspace)

	var idx int
	for u := 0; u < workspaces; u++ {
		workspaceID := "workspace-" + strconv.Itoa(u)
		s.workspaces = append(s.workspaces, workspaceID)
		for i := 0; i < eventsPerWorkspace; i++ {
			s.jobs[idx] = &rtIsolationJobSpec{
				id:          int64(idx + 1),
				workspaceID: workspaceID,
				userID:      strconv.Itoa(idx + 1),
			}
			idx++
		}
	}
	return &s
}

// RouterIsolationScenario runs a scenario with the given spec which:
// 1. Sends all events to rt tables
// 2. Starts the server
// 3. Waits for the events to be processed by router
// 4. Verifies that all events have been processed successfully
// 5. Verifies that the correct number of events have been delivered to their destination
// 6. Returns the total processing duration (last event time - first event time).
func RouterIsolationScenario(t testing.TB, spec *RtIsolationScenarioSpec) (overallDuration time.Duration) {
	var m rtIsolationMethods

	config.Reset()
	defer logger.Reset()
	defer config.Reset()
	config.Set("LOG_LEVEL", "ERROR")
	logger.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "it should be able to create a new docker pool")
	t.Logf("Starting postgres container")
	postgresContainer, err := postgres.Setup(pool, t, postgres.WithOptions("max_connections=1000"), postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "it should be able to start postgres container without an error")
	transformerContainer, err := transformertest.Setup(pool, t)
	require.NoError(t, err)
	t.Logf("Starting the server")
	webhook := m.newWebhook(t)
	defer webhook.Server.Close()

	t.Logf("Setting up the mock config backend")
	templateCtx := map[string]any{
		"workspaces": spec.workspaces,
		"webhookURL": webhook.Server.URL,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/rtIsolationTestTemplate.json.tpl", templateCtx)
	mockCBE := m.newMockConfigBackend(t, configJsonPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)
	defer mockCBE.Close()

	t.Logf("Preparing the necessary configuration")
	gatewayPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	adminPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	config.Set("Gateway.webPort", gatewayPort)
	config.Set("Gateway.adminWebPort", adminPort)
	config.Set("Profiler.Enabled", false)
	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "rt_isolation_test")
	config.Set("HOSTED_SERVICE_SECRET", "rt_isolation_secret")
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
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")

	config.Set("Router.isolationMode", string(spec.isolationMode))
	config.Set("Router.Limiter.statsPeriod", "1s")

	config.Set("JobsDB.enableWriterQueue", false)
	// config.Set("JobsDB.maxReaders", 10)
	config.Set("RUDDER_TMPDIR", os.TempDir())

	t.Logf("Seeding rt jobsdb with jobs")
	m.seedRtDB(t, spec, webhook.Server.URL)

	t.Logf("Starting rudder server")
	svcDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("rudder-server panicked: %v", r)
				close(svcDone)
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		c := r.Run(ctx, []string{"rt-isolation-test-rudder-server"})
		if c != 0 {
			t.Errorf("rudder-server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()
	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%d/health", gatewayPort),
		20*time.Second,
		10*time.Millisecond,
		t.Name(),
	)
	t.Logf("Rudder server started")

	t.Logf("Waiting for all rt jobs to be successfully processed")
	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('rt',20) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
		return processedJobCount == len(spec.jobs)
	}, 5*time.Minute, 1*time.Second, "all rt jobs should be successfully processed")

	t.Logf("Verifying the destinations")
	for _, workspace := range spec.workspaces {
		workspaceJobs := lo.CountBy(spec.jobs, func(job *rtIsolationJobSpec) bool {
			return job.workspaceID == workspace
		})
		require.EqualValuesf(t, workspaceJobs, webhook.receivedCounters[workspace], "received jobs for workspace %s should be as expected", workspace)
	}

	t.Logf("Destinations verified")

	var minExecTime, maxExecTime time.Time
	require.NoError(t, postgresContainer.DB.QueryRow("SELECT min(exec_time), max(exec_time) FROM unionjobsdbmetadata('rt',20)").Scan(&minExecTime, &maxExecTime), "it should be able to query the min and max execution times")
	overallDuration = maxExecTime.Sub(minExecTime)

	cancel()
	<-svcDone
	return
}

type RtIsolationScenarioSpec struct {
	isolationMode isolation.Mode
	workspaces    []string
	jobs          []*rtIsolationJobSpec
}

type rtIsolationJobSpec struct {
	id          int64
	workspaceID string
	userID      string
}

func (jobSpec *rtIsolationJobSpec) payload(url string) []byte {
	json := fmt.Sprintf(`{
		"userId": %[1]q,
		"anonymousId": %[2]q,
		"testJobId": %[3]d,
		"workspaceID": %[4]q,
		"destType": "WEBHOOK",
		"type": "identify",
		"context":
		{
			"traits":
			{
				"trait1": "new-val"
			},
			"ip": "14.5.67.21",
			"library":
			{
				"name": "http"
			}
		},
		"timestamp": "2020-02-02T00:23:09.544Z",
		"receivedAt": %[5]q
	}`, jobSpec.userID, jobSpec.userID, jobSpec.id, jobSpec.workspaceID, time.Now().Format(misc.RFC3339Milli))

	return []byte(fmt.Sprintf(`{
		"body": {
			"XML": {},
			"FORM": {},
			"JSON": %[1]s,
			"JSON_ARRAY": {}
		},
		"type": "REST",
		"files": {},
		"method": "POST",
		"params": {},
		"userId": "",
		"headers": {
			"content-type": "application/json"
		},
		"version": "1",
		"endpoint": %[2]q
	}`, json, url))
}

// Using a struct to keep router_test package clean and
// avoid function collisions with other tests
type rtIsolationMethods struct{}

// newMockConfigBackend creates a mock config backend server serving the config file at the given path
func (rtIsolationMethods) newMockConfigBackend(t testing.TB, path string) *httptest.Server {
	data, err := os.ReadFile(path)
	require.NoError(t, err, "should be able to read the config file")
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "features") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.Contains(r.URL.Path, "settings") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(data)
		require.NoError(t, err, "should be able to write the response code to the response")
	}))
}

// seedRtDB seeds the router database with jobs based on the provided spec
func (m rtIsolationMethods) seedRtDB(t testing.TB, spec *RtIsolationScenarioSpec, url string) {
	rtJobsDB := jobsdb.NewForWrite("rt", jobsdb.WithStats(stats.NOP))
	require.NoError(t, rtJobsDB.Start(), "it should be able to start the jobsdb")
	defer rtJobsDB.Stop()
	for _, batch := range m.generateJobs(spec.jobs, url, 100) {
		require.NoError(t, rtJobsDB.Store(context.Background(), batch), "it should be able to store the batch of jobs in the jobsdb")
	}
}

// generateJobs creates batches of jobs from the same workspace, shuffled so that
// batches for the same workspace are not consecutive.
func (rtIsolationMethods) generateJobs(jobs []*rtIsolationJobSpec, url string, batchSize int) [][]*jobsdb.JobT {
	wsBatches := map[string][]*jobsdb.JobT{}
	for _, job := range jobs {
		payload := job.payload(url)
		wsBatches[job.workspaceID] = append(wsBatches[job.workspaceID], &jobsdb.JobT{
			UUID:        uuid.New(),
			JobID:       job.id,
			UserID:      job.userID,
			WorkspaceId: job.workspaceID,
			Parameters: []byte(fmt.Sprintf(`{
				"source_id": %[1]q,
				"destination_id": %[1]q,
				"receivedAt": %[2]q
			}`, job.workspaceID, time.Now().Format(misc.RFC3339Milli))),
			CustomVal:    "WEBHOOK",
			EventPayload: payload,
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
		})
	}

	var batches [][]*jobsdb.JobT
	for _, wsBatch := range wsBatches {
		chunks := lo.Chunk(wsBatch, batchSize)
		batches = append(batches, chunks...)
	}
	mutable.Shuffle(batches)
	return batches
}

func (rtIsolationMethods) newWebhook(t testing.TB) *rtIsolationWebhook {
	var wh rtIsolationWebhook
	wh.receivedCounters = map[string]int{}

	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err, "should be able to read the request body")
		workspaceID := gjson.GetBytes(body, "workspaceID")
		require.True(t, workspaceID.Exists(), "should have workspaceID in the request", body)

		wh.mu.Lock()
		defer wh.mu.Unlock()
		wh.receivedCounters[workspaceID.String()]++
		w.WriteHeader(200)
	}))
	return &wh
}

type rtIsolationWebhook struct {
	mu               sync.Mutex
	Server           *httptest.Server
	receivedCounters map[string]int
}
