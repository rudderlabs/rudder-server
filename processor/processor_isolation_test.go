package processor_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo/mutable"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestProcessorIsolation(t *testing.T) {
	const (
		workspaces       = 10
		jobsPerWorkspace = 100
	)
	t.Run("no isolation", func(t *testing.T) {
		spec := NewProcIsolationScenarioSpec(isolation.ModeNone, workspaces, jobsPerWorkspace)
		ProcIsolationScenario(t, spec)
	})

	t.Run("workspace isolation", func(t *testing.T) {
		spec := NewProcIsolationScenarioSpec(isolation.ModeWorkspace, workspaces, jobsPerWorkspace)
		ProcIsolationScenario(t, spec)
	})

	t.Run("source isolation", func(t *testing.T) {
		spec := NewProcIsolationScenarioSpec(isolation.ModeSource, workspaces, jobsPerWorkspace)
		ProcIsolationScenario(t, spec)
	})
}

// go test \
// -timeout 3600s \
// -run=^$ \
// -bench ^BenchmarkProcessorIsolationModes$ \
// github.com/rudderlabs/rudder-server/processor \
// -v \
// -count=1 |grep BenchmarkProcessorIsolationModes
// BenchmarkProcessorIsolationModes
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_10_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_10_total_jobs_200000-10         	       1	73042697042 ns/op	        43.55 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_10_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_10_total_jobs_200000-10    	       1	69209445166 ns/op	        56.55 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_10_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_10_total_jobs_200000-10       	       1	71106239750 ns/op	        56.47 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_50_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_50_total_jobs_200000-10         	       1	82385831083 ns/op	        69.61 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_50_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_50_total_jobs_200000-10    	       1	72104657333 ns/op	        56.94 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_50_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_50_total_jobs_200000-10       	       1	68407738000 ns/op	        54.77 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_100_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_100_total_jobs_200000-10        	       1	77992348375 ns/op	        65.44 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_100_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_100_total_jobs_200000-10   	       1	68580986375 ns/op	        55.83 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_100_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_100_total_jobs_200000-10      	       1	69483415750 ns/op	        55.81 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_200_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_200_total_jobs_200000-10        	       1	83725871750 ns/op	        72.55 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_200_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_200_total_jobs_200000-10   	       1	67370016208 ns/op	        56.02 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_200_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_200_total_jobs_200000-10      	       1	68468731833 ns/op	        57.48 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_500_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_500_total_jobs_200000-10        	       1	112563037291 ns/op	        99.25 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_500_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_500_total_jobs_200000-10   	       1	66369215792 ns/op	        55.00 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_500_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_500_total_jobs_200000-10      	       1	67725539583 ns/op	        56.37 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_1000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_1000_total_jobs_200000-10       	       1	160692931875 ns/op	       148.7 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_1000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_1000_total_jobs_200000-10  	       1	73783787083 ns/op	        57.48 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_1000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_1000_total_jobs_200000-10     	       1	72094473125 ns/op	        57.55 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_10000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_none_workspaces_10000_total_jobs_200000-10      	       1	264327524125 ns/op	       251.5 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_10000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_workspace_workspaces_10000_total_jobs_200000-10 	       1	170777786959 ns/op	       145.3 overall_duration_sec
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_10000_total_jobs_200000
// BenchmarkProcessorIsolationModes/isolation_mode_source_workspaces_10000_total_jobs_200000-10    	       1	167364648542 ns/op	       144.3 overall_duration_sec
// https://snapshots.raintank.io/dashboard/snapshot/xugaw44VT5dYMQ2ynyBbvM4FZfxFvD5D
func BenchmarkProcessorIsolationModes(b *testing.B) {
	benchModes := func(b *testing.B, workspaces, totalJobs int) {
		bench := func(mode isolation.Mode) {
			title := fmt.Sprintf("isolation mode %s workspaces %d total jobs %d", mode, workspaces, totalJobs)
			b.Run(title, func(b *testing.B) {
				stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "start"}).Increment()
				defer stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "end"}).Increment()
				spec := NewProcIsolationScenarioSpec(mode, workspaces, totalJobs/workspaces)
				overallDuration := ProcIsolationScenario(b, spec)
				b.ReportMetric(overallDuration.Seconds(), "overall_duration_sec")
			})
		}
		bench(isolation.ModeNone)
		bench(isolation.ModeWorkspace)
		bench(isolation.ModeSource)
	}

	benchModes(b, 10, 200000)
	benchModes(b, 50, 200000)
	benchModes(b, 100, 200000)
	benchModes(b, 200, 200000)
	benchModes(b, 500, 200000)
	benchModes(b, 1000, 200000)
	benchModes(b, 10000, 200000)
}

// ProcIsolationScenarioSpec is a specification for a processor isolation scenario.
// - isolationMode is the isolation mode to use.
// - workspaces is the number of workspaces to use.
// - eventsPerWorkspace is the number of events to send per workspace.
func NewProcIsolationScenarioSpec(isolationMode isolation.Mode, workspaces, eventsPerWorkspace int) *ProcIsolationScenarioSpec {
	var s ProcIsolationScenarioSpec
	s.isolationMode = isolationMode
	s.jobs = make([]*procIsolationJobSpec, workspaces*eventsPerWorkspace)
	s.received = map[int]struct{}{}

	var idx int
	for u := 0; u < workspaces; u++ {
		workspaceID := "workspace-" + strconv.Itoa(u)
		s.workspaces = append(s.workspaces, workspaceID)
		for i := 0; i < eventsPerWorkspace; i++ {
			jobID := idx + 1
			js := procIsolationJobSpec{
				id:          jobID,
				workspaceID: workspaceID,
				userID:      strconv.Itoa(jobID),
			}
			s.jobs[idx] = &js
			idx++
		}
	}
	return &s
}

// ProcIsolationScenario runs a scenario with the given spec which:
// 1. Sends all events to gateway
// 2. Waits for the events to be processed by processor
// 3. Verifies that the correct number of events have been sent to router's jobsdb
// 4. Returns the total processing duration (last event time - first event time).
func ProcIsolationScenario(t testing.TB, spec *ProcIsolationScenarioSpec) (overallDuration time.Duration) {
	var m procIsolationMethods

	config.Reset()
	defer logger.Reset()
	defer config.Reset()
	config.Set("LOG_LEVEL", "ERROR")
	logger.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		postgresContainer    *postgres.Resource
		transformerContainer *transformertest.Resource
		gatewayPort          string
	)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	containersGroup, _ := errgroup.WithContext(ctx)
	containersGroup.Go(func() (err error) {
		postgresContainer, err = postgres.Setup(pool, t, postgres.WithOptions("max_connections=1000"))
		return err
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		return err
	})
	require.NoError(t, containersGroup.Wait())

	destinationID := trand.String(27)

	templateCtx := map[string]any{
		"webhookUrl":    "http://localhost:1234", // not important
		"workspaces":    spec.workspaces,
		"destinationId": destinationID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/procIsolationTestTemplate.json.tpl", templateCtx)
	mockCBE := m.newMockConfigBackend(t, configJsonPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)

	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "proc_isolation_test")
	config.Set("HOSTED_SERVICE_SECRET", "proc_isolation_secret")
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
	config.Set("Router.toAbortDestinationIDs", destinationID)
	config.Set("archival.Enabled", false)
	config.Set("enableStats", false)

	config.Set("Processor.isolationMode", string(spec.isolationMode))

	config.Set("JobsDB.enableWriterQueue", false)

	// find free port for gateway http server to listen on
	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(t, err)
	gatewayPort = strconv.Itoa(httpPortInt)

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
		c := r.Run(ctx, []string{"proc-isolation-test-rudder-server"})
		if c != 0 {
			t.Errorf("rudder-server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()

	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%s/health", gatewayPort),
		200*time.Second,
		100*time.Millisecond,
		t.Name(),
	)

	batchSize := 5
	batches := m.splitInBatches(spec.jobs, batchSize)

	t.Logf("sending %d events in %d batches", len(spec.jobs), len(batches))
	gzipPayload := func(data []byte) (io.Reader, error) {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, err = gz.Write(data)
		if err != nil {
			return nil, err
		}

		if err = gz.Flush(); err != nil {
			return nil, err
		}

		if err = gz.Close(); err != nil {
			return nil, err
		}

		return &b, nil
	}
	g := &errgroup.Group{}
	g.SetLimit(100)
	client := &http.Client{}
	url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
	for _, payload := range batches {
		payload := payload
		g.Go(func() error {
			writeKey := gjson.GetBytes(payload, "batch.0.workspaceID").String()
			p, err := gzipPayload(payload)
			require.NoError(t, err)
			req, err := http.NewRequest("POST", url, p)
			req.Header.Add("Content-Encoding", "gzip")
			require.NoError(t, err, "should be able to create a new request")
			req.SetBasicAuth(writeKey, "password")
			resp, err := client.Do(req)
			require.NoError(t, err, "should be able to send the request to gateway")
			require.Equal(t, http.StatusOK, resp.StatusCode, "should be able to send the request to gateway successfully", payload)
			func() { kithttputil.CloseResponse(resp) }()
			return nil
		})
	}
	require.NoError(t, g.Wait())

	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',5) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
		return processedJobCount == len(spec.jobs)
	}, 300*time.Second, 1*time.Second, "all batches should be successfully processed")

	var failedJobs int
	require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('proc_error',5) where parameters->>'stage' != 'router'").Scan(&failedJobs))
	require.Equal(t, 0, failedJobs, "should not have any failed jobs")

	// count gw min and max job times
	var gwMinJobTime, gwMaxJobTime time.Time
	require.NoError(t, postgresContainer.DB.QueryRow("SELECT min(created_at), max(created_at) FROM unionjobsdbmetadata('gw',5)").Scan(&gwMinJobTime, &gwMaxJobTime))

	// count min and max job times
	var minJobTime, maxJobTime time.Time
	var totalJobsCount int
	require.NoError(t, postgresContainer.DB.QueryRow("SELECT min(created_at), max(created_at), count(*) FROM unionjobsdbmetadata('rt',5)").Scan(&minJobTime, &maxJobTime, &totalJobsCount))
	require.Equal(t, len(spec.jobs), totalJobsCount)
	overallDuration = maxJobTime.Sub(gwMinJobTime)

	require.Eventually(t, func() bool {
		return rmetrics.PendingEvents("rt", rmetrics.All, rmetrics.All).IntValue() == 0
	}, 100*time.Second, 1*time.Second, "all rt jobs should be aborted")
	cancel()
	<-svcDone
	return
}

type ProcIsolationScenarioSpec struct {
	isolationMode isolation.Mode
	workspaces    []string
	jobs          []*procIsolationJobSpec
	received      map[int]struct{}
}

type procIsolationJobSpec struct {
	id          int
	workspaceID string
	userID      string
}

func (jobSpec *procIsolationJobSpec) payload() string {
	template := `{
				"userId": %q,
				"anonymousId": %q,
				"testJobId": %d,
				"workspaceID": %q,
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
				"timestamp": "2020-02-02T00:23:09.544Z"
			}`
	return fmt.Sprintf(template, jobSpec.userID, jobSpec.userID, jobSpec.id, jobSpec.workspaceID)
}

// Using a struct to keep processor_test package clean and
// avoid function collisions with other tests
type procIsolationMethods struct{}

func (procIsolationMethods) newMockConfigBackend(t testing.TB, path string) *httptest.Server {
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

// splitInBatches creates batches of jobs from the same workspace, shuffled so that
// batches for the same workspace are not consecutive.
func (procIsolationMethods) splitInBatches(jobs []*procIsolationJobSpec, batchSize int) [][]byte {
	payloads := map[string][]string{}
	for _, job := range jobs {
		payloads[job.workspaceID] = append(payloads[job.workspaceID], job.payload())
	}

	var batches [][]byte
	for _, payload := range payloads {
		chunks := lo.Chunk(payload, batchSize)
		batches = append(batches, lo.Map(chunks, func(chunk []string, _ int) []byte {
			return []byte(fmt.Sprintf(`{"batch":[%s]}`, strings.Join(chunk, ",")))
		})...)
	}
	mutable.Shuffle(batches)
	return batches
}
