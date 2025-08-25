package router_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	proctypes "github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// TestThrottlePerEventType tests the delivery throttler in the router.
//
//   - It first sends a number of jobs to the gateway.
//   - It checks that all jobs are delivered to the webhook destination at the expected rate.
//   - The test uses a webhook server to receive the jobs and track their counts and the time it received each event.
func TestDeliveryThrottler(t *testing.T) {
	// necessary until we move away from a singleton config
	config.Reset()
	defer config.Reset()

	const (
		rp10s         = 20                   // how many jobs per 10 seconds we will allow for identify events
		totalJobs     = 30                   // how many jobs we will send in total
		batchSize     = 10                   // how many jobs are we going to send in each gateway request
		endpointLabel = "rateLimitedWebhook" // the endpoint label for the webhook for which we will enable throttling
	)
	var (
		m      deliveryThrottlerMethods
		passed bool
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// generate the test specification
	spec := m.newTestSpec(totalJobs)

	var (
		postgresContainer *postgres.Resource
		gatewayPort       string
	)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	t.Logf("Starting postgresql")
	postgresContainer, err = postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err)

	t.Logf("Starting webhook destination server")
	webhook := m.newWebhook(t, spec)
	defer webhook.Server.Close()

	t.Logf("Starting test transformer server")
	trServer := transformertest.NewBuilder().
		WithDestTransformHandler(
			"WEBHOOK",
			transformertest.DestTransformerHandler(func(event proctypes.TransformerEvent) integrations.PostParametersT {
				return integrations.PostParametersT{
					Type:          "REST",
					URL:           webhook.Server.URL,
					EndpointLabel: endpointLabel, // custom transformer adds the endpoint label for throttling
					RequestMethod: http.MethodPost,
					Body: map[string]interface{}{
						"JSON": event.Message,
					},
				}
			}),
		).
		Build()
	t.Cleanup(trServer.Close)

	t.Logf("Preparing rudder-server config")
	writeKey := trand.String(27)
	workspaceID := trand.String(27)
	destinationID := trand.String(27)
	templateCtx := map[string]any{
		"webhookUrl":    webhook.Server.URL,
		"writeKey":      writeKey,
		"workspaceId":   workspaceID,
		"destinationId": destinationID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/deliveryThrottlerTestTemplate.json", templateCtx)
	config.Set("BackendConfig.configFromFile", true)
	config.Set("BackendConfig.configJSONPath", configJsonPath)

	t.Logf("Starting rudder-server")
	config.Set("DEPLOYMENT_TYPE", deployment.DedicatedType)
	config.Set("recovery.storagePath", path.Join(t.TempDir(), "/recovery_data.json"))

	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", trServer.URL)

	config.Set("Warehouse.mode", "off")
	config.Set("enableStats", false)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("Router.jobIterator.maxQueries", 1)
	config.Set("Router.jobIterator.discardedPercentageTolerance", 100)
	config.Set("Router.failingJobsPenaltySleep", "0s")
	config.Set("Router.noOfWorkers", 64)

	// throttling config
	config.Set("Router.throttler.delivery.WEBHOOK."+endpointLabel+".limit", rp10s)
	config.Set("Router.throttler.delivery.WEBHOOK."+endpointLabel+".timeWindow", "10s")

	// generatorLoop
	config.Set("Router.jobQueryBatchSize", totalJobs*2) // be able to query all jobs at once
	config.Set("Router.readSleep", "10ms")
	config.Set("Router.minRetryBackoff", "5ms")
	config.Set("Router.maxRetryBackoff", "10ms")

	// statusInsertLoop
	config.Set("Router.updateStatusBatchSize", totalJobs*2) // be able to insert all statuses at once
	config.Set("Router.maxStatusUpdateWait", "10ms")

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
				if passed {
					t.Logf("server panicked: %v", r)
					close(svcDone)
				} else {
					t.Errorf("server panicked: %v", r)
				}
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		c := r.Run(ctx, []string{"delivery-throttler-test-rudder-server"})
		t.Logf("server stopped: %d", c)
		if c != 0 {
			t.Errorf("server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()

	t.Logf("waiting rudder-server to start properly")
	health.WaitUntilReady(ctx, t,
		fmt.Sprintf("http://localhost:%s/health", gatewayPort),
		200*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	t.Logf("rudder-server started")

	batches := m.splitInBatches(spec.jobsOrdered, batchSize)

	go func() {
		t.Logf("Sending %d total events in %d batches", len(spec.jobsOrdered), len(batches))
		client := &http.Client{}
		url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
		for _, payload := range batches {
			require.NoError(t, err)
			req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
			require.NoError(t, err, "should be able to create a new request")
			req.SetBasicAuth(writeKey, "password")
			resp, err := client.Do(req)
			require.NoError(t, err, "should be able to send the request to gateway")
			require.Equal(t, http.StatusOK, resp.StatusCode, "should be able to send the request to gateway successfully", payload)
			func() { kithttputil.CloseResponse(resp) }()
		}
	}()

	t.Logf("Waiting for the magic to happen... eventually")

	var done int
	require.Eventually(t, func() bool {
		select {
		case <-svcDone: // server has exited, no point in keep trying
			return true
		default:
		}
		if t.Failed() { // webhook failed, no point in keep retrying
			return true
		}
		total := len(spec.jobsOrdered)
		spec.mu.Lock()
		defer spec.mu.Unlock()
		if done != spec.received {
			done = spec.received
			t.Logf("%d/%d done", done, total)
		}
		return done == total
	}, 120*time.Second, 2*time.Second, "webhook should receive all events and process them till the end")
	require.False(t, t.Failed(), "webhook shouldn't have failed")

	// make sure received times are within throttling limits, but first ignore the first timestamps to account for gcra burst
	receivedExcludingBurst := spec.receivedTimes[rp10s:]

	// Verify that the received events respect the rate limit (with some slack for timing variance)
	if len(receivedExcludingBurst) > 1 {
		expectedMinInterval := 10 * time.Second / time.Duration(rp10s) // minimum interval between events at the given RPS
		tolerance := expectedMinInterval / 2                           // allow 50% tolerance for timing variance

		for i := 1; i < len(receivedExcludingBurst); i++ {
			actualInterval := receivedExcludingBurst[i].Sub(receivedExcludingBurst[i-1])
			minAllowedInterval := expectedMinInterval - tolerance

			require.GreaterOrEqual(t, actualInterval, minAllowedInterval,
				"Event %d arrived too quickly: interval=%v, expected>=%v (RP10S=%d)",
				i+rp10s, actualInterval, minAllowedInterval, rp10s)
		}
		t.Logf("Rate limiting verification passed: %d events after burst checked against %d RP10S limit",
			len(receivedExcludingBurst), rp10s)
	}

	t.Logf("All jobs arrived in expected rate - test passed!")
	passed = true
	cancel()
	<-svcDone
}

// Using a struct to keep main_test package clean and
// avoid function collisions with other tests
// TODO: Move server's Run() out of main package
type deliveryThrottlerMethods struct{}

// newTestSpec creates a new deliveryThrottlerSpec with the given number of jobs.
// It generates jobs for event type "identify".
func (m deliveryThrottlerMethods) newTestSpec(jobsCount int) *deliveryThrottlerSpec {
	var s deliveryThrottlerSpec
	s.jobsOrdered = make([]*deliveryThrottlerJobSpec, jobsCount)

	for i := 0; i < jobsCount; i++ {
		js := deliveryThrottlerJobSpec{
			userID:    uuid.New().String(),
			eventType: "identify",
		}
		s.jobsOrdered[i] = &js
	}
	return &s
}

// newWebhook creates a new webhook server that will receive events and track their counts.
// The server will respond with a 200 status code and the response body will contain the status code.
// It will also update the spec with the received counts and received time for each event
func (deliveryThrottlerMethods) newWebhook(t *testing.T, spec *deliveryThrottlerSpec) *deliveryThrottlerWebhook {
	var wh deliveryThrottlerWebhook
	wh.spec = spec

	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAt := time.Now()
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err, "should be able to read the request body")

		wh.spec.mu.Lock()
		defer wh.spec.mu.Unlock()
		wh.spec.received++

		wh.spec.receivedTimes = append(wh.spec.receivedTimes, receivedAt)
		t.Logf("Received event at %s, total received: %d", receivedAt, wh.spec.received)
		w.WriteHeader(200)
	}))
	return &wh
}

// splitInBatches creates batches of jobs with the specified batch size
func (deliveryThrottlerMethods) splitInBatches(jobs []*deliveryThrottlerJobSpec, batchSize int) [][]byte {
	payloads := lo.Map(jobs, func(job *deliveryThrottlerJobSpec, _ int) string { return job.payload() })
	batches := lo.Chunk(payloads, batchSize)
	jsonBatches := lo.Map(batches, func(batch []string, _ int) []byte {
		return []byte(fmt.Sprintf(`{"batch":[%s]}`, strings.Join(batch, ",")))
	})
	return jsonBatches
}

type deliveryThrottlerWebhook struct {
	Server *httptest.Server
	spec   *deliveryThrottlerSpec
}

type deliveryThrottlerSpec struct {
	jobsOrdered []*deliveryThrottlerJobSpec

	mu            sync.Mutex
	received      int         // number of received events
	receivedTimes []time.Time // timestamps of received events
}

type deliveryThrottlerJobSpec struct {
	userID    string
	eventType string
}

func (jobSpec *deliveryThrottlerJobSpec) payload() string {
	template := `{
				"userId": %q,
				"anonymousId": %q,
				"type": "%s",
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
	return fmt.Sprintf(template, jobSpec.userID, jobSpec.userID, jobSpec.eventType)
}
