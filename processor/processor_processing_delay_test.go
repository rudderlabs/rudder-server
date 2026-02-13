package processor_test

import (
	"bytes"
	"context"
	"fmt"
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
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestProcessorProcessingDelay(t *testing.T) {
	ProcProcessingDelayScenario(t, NewProcProcessingDelayScenarioSpec())
}

// ProcProcessingDelayScenarioSpec is a specification for a processor processing delay scenario.
func NewProcProcessingDelayScenarioSpec() *ProcProcessingDelayScenarioSpec {
	var s ProcProcessingDelayScenarioSpec
	const batchSize = 10
	s.batch1 = make([]*procProcessingDelayJobSpec, batchSize)
	s.batch2 = make([]*procProcessingDelayJobSpec, batchSize)
	s.received = map[int]struct{}{}

	var idx int
	workspaceID := "workspace-0"
	for range batchSize {
		jobID1 := idx + 1
		js1 := procProcessingDelayJobSpec{
			id:          jobID1,
			workspaceID: workspaceID,
			userID:      strconv.Itoa(jobID1),
		}
		s.batch1[idx] = &js1

		jobID2 := jobID1 + 10000
		js2 := procProcessingDelayJobSpec{
			id:          jobID2,
			workspaceID: workspaceID,
			userID:      strconv.Itoa(jobID2),
		}
		s.batch2[idx] = &js2
		idx++
	}
	return &s
}

// ProcProcessingDelayScenario runs a scenario with the given spec which:
// 1. Sends batch1 events to gateway
// 2. Waits for the events to be processed by processor
// 3. Verifies that the correct number of events have been processed with a delay of 10s
// 4. Sends batch2 events to gateway
// 5. Waits for 3s and stops the server
// 6. Verifies that the events in batch2 are left at executing stage due to shutdown while sleeping
// 7. Starts the server again
// 8. Waits for the events to be processed by processor
func ProcProcessingDelayScenario(t testing.TB, spec *ProcProcessingDelayScenarioSpec) {
	const processingDelay = 10 * time.Second

	var m procProcessingDelayMethods

	config.Reset()
	defer jsonrs.Reset()
	defer logger.Reset()
	defer config.Reset()
	config.Set("LOG_LEVEL", "WARN")
	logger.Reset()
	jsonrs.Reset()
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
		postgresContainer, err = postgres.Setup(pool, t, postgres.WithOptions("max_connections=1000"), postgres.WithTag("17-alpine"))
		return err
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		return err
	})
	require.NoError(t, containersGroup.Wait())

	destinationID := "destination-0"

	templateCtx := map[string]any{
		"webhookUrl": "http://localhost:1234", // not important
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/procProcessingDelayTestTemplate.json.tpl", templateCtx)
	mockCBE := m.newMockConfigBackend(t, configJsonPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)

	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "proc_processing_delay_test")
	config.Set("HOSTED_SERVICE_SECRET", "proc_processing_delay_secret")
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
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("JobsDB.payloadColumnType", "text")
	config.Set("Router.toAbortDestinationIDs", []string{destinationID})
	config.Set("archival.Enabled", false)
	config.Set("enableStats", false)

	config.Set("Processor.pipelinesPerPartition", 1)
	config.Set("Processor.maxLoopSleep", 500*time.Millisecond)
	config.Set("Processor.pingerSleep", 100*time.Millisecond)
	config.Set("Processor.readLoopSleep", 100*time.Millisecond)
	config.Set("Processor.maxLoopProcessEvents", 10000)
	config.Set("Processor.isolationMode", string(isolation.ModeSource))
	config.Set("Processor.preprocessDelay.source-0", processingDelay)

	config.Set("JobsDB.enableWriterQueue", false)

	// find free port for gateway http server to listen on
	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(t, err)
	gatewayPort = strconv.Itoa(httpPortInt)

	config.Set("Gateway.webPort", gatewayPort)
	config.Set("RUDDER_TMPDIR", os.TempDir())

	startServer := func() (stop func(), stopped <-chan struct{}) {
		ctx, cancel := context.WithCancel(ctx)
		svcDone := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("rudder-server panicked: %v", r)
					close(svcDone)
				}
			}()
			r := runner.New(runner.ReleaseInfo{})
			c := r.Run(ctx, cancel, []string{"proc-isolation-test-rudder-server"})
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
		return cancel, svcDone
	}
	stopServer, serverStopped := startServer()

	sendBatches := func(batches [][]byte) {
		g := &errgroup.Group{}
		g.SetLimit(10)
		client := &http.Client{}
		url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
		for _, payload := range batches {
			g.Go(func() error {
				writeKey := "source-0"
				req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
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
	}

	batches := m.splitInBatches(spec.batch1, len(spec.batch1))
	t.Logf("sending %d events in %d batches", len(spec.batch1), len(batches))
	sendBatches(batches)

	t.Log("waiting for all events to be processed")
	start := time.Now()
	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',5) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
		return processedJobCount == len(spec.batch1)
	}, 600*time.Second, 1*time.Second, "all batches should be successfully processed")

	delay := time.Since(start)
	require.Greater(t, delay, 10*time.Second, "processing should take at least 10s due to the configured delay")

	// send the second batch of events
	batches = m.splitInBatches(spec.batch2, len(spec.batch2))
	t.Logf("sending %d events in %d batches", len(spec.batch2), len(batches))
	sendBatches(batches)

	// wait for 3 seconds and then stop the server
	waitTime := 3 * time.Second
	time.Sleep(waitTime)
	t.Logf("stopping server after waiting for %v", waitTime)
	stopServer()
	start = time.Now()
	<-serverStopped
	shutdownDuration := time.Since(start)
	require.Less(t, shutdownDuration, processingDelay-2*waitTime, "server should shut down before the processing delay times out")

	var executingJobsCount int
	require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',5) WHERE job_state = 'executing'").Scan(&executingJobsCount))
	require.Equal(t, len(spec.batch2), executingJobsCount, "batch2 should be left at executing stage due to shutdown while sleeping")

	// start the server again to process the remaining events
	stopServer, serverStopped = startServer()
	t.Log("waiting for all events to be processed after restart")
	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',5) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
		return processedJobCount == len(spec.batch1)+len(spec.batch2)
	}, 600*time.Second, 1*time.Second, "all batches should be successfully processed")

	stopServer()
	<-serverStopped
}

type ProcProcessingDelayScenarioSpec struct {
	batch1   []*procProcessingDelayJobSpec
	batch2   []*procProcessingDelayJobSpec
	received map[int]struct{}
}

type procProcessingDelayJobSpec struct {
	id          int
	workspaceID string
	userID      string
}

func (jobSpec *procProcessingDelayJobSpec) payload() string {
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
type procProcessingDelayMethods struct{}

func (procProcessingDelayMethods) newMockConfigBackend(t testing.TB, path string) *httptest.Server {
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
func (procProcessingDelayMethods) splitInBatches(jobs []*procProcessingDelayJobSpec, batchSize int) [][]byte {
	payloads := map[string][]string{}
	for _, job := range jobs {
		payloads[job.workspaceID] = append(payloads[job.workspaceID], job.payload())
	}

	var batches [][]byte
	for _, payload := range payloads {
		chunks := lo.Chunk(payload, batchSize)
		batches = append(batches, lo.Map(chunks, func(chunk []string, _ int) []byte {
			return fmt.Appendf(nil, `{"batch":[%s]}`, strings.Join(chunk, ","))
		})...)
	}
	mutable.Shuffle(batches)
	return batches
}
