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
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

const (
	identifyEventType = "identify"
	trackEventType    = "track"
)

// TestThrottlePerEventType tests the throttling per event type functionality in the router.
//
//   - It simulates sending a number of jobs for two event types: "identify" and "track". First sends all "identify" jobs and then all "track" jobs.
//   - It checks that the jobs are processed in the expected order based on the throttling configuration, where "identify" events have a lower RPS than "track" events.
//   - The test uses a webhook server to receive the jobs and track their counts and last received time for each event type.
//   - It also checks that the last received identify event is after the last received track event when throttling per event type is enabled.
func TestThrottlePerEventType(t *testing.T) {
	testScenario := func(expectTrackBeforeIdentify bool, throttlingConfigOverrides func()) func(t *testing.T) {
		return func(t *testing.T) {
			// necessary until we move away from a singleton config
			config.Reset()
			defer config.Reset()

			const (
				rpsForIdentify   = 200  // how many jobs per second we will allow for identify events
				rpsForTrack      = 1000 // how many jobs per second we will allow for track events
				jobsPerEventType = 1000 // how many jobs per event type we will send
				batchSize        = 200  // how many jobs are we going to send in each request
			)
			var (
				m      throttlingPerEventTypeMethods
				passed bool
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// generate the test specification
			// this will create jobs for two event types: "identify" and "track"
			spec := m.newTestSpec(jobsPerEventType)

			t.Logf("Starting docker services (postgres & transformer)")
			var (
				postgresContainer    *postgres.Resource
				transformerContainer *transformertest.Resource
				gatewayPort          string
			)
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			containersGroup, _ := errgroup.WithContext(ctx)
			containersGroup.Go(func() (err error) {
				postgresContainer, err = postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
				return err
			})
			containersGroup.Go(func() (err error) {
				transformerContainer, err = transformertest.Setup(pool, t)
				return err
			})
			require.NoError(t, containersGroup.Wait())
			t.Logf("Started docker services")

			t.Logf("Starting webhook destination server")
			webhook := m.newWebhook(t, spec)
			defer webhook.Server.Close()

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
			configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/throttlingPerEventTypeTestTemplate.json", templateCtx)
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
			config.Set("DEST_TRANSFORM_URL", transformerContainer.TransformerURL)

			config.Set("Warehouse.mode", "off")
			config.Set("enableStats", false)
			config.Set("JobsDB.backup.enabled", false)
			config.Set("Router.jobIterator.maxQueries", 1)
			config.Set("Router.jobIterator.discardedPercentageTolerance", 100)
			config.Set("Router.failingJobsPenaltySleep", "0s")
			config.Set("Router.noOfWorkers", 1)

			// throttling config

			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.WEBHOOK.throttlerPerEventType", true)
			config.Set("Router.throttler.WEBHOOK.pickupQueryThrottlingEnabled", true)
			config.Set("Router.throttler.WEBHOOK."+identifyEventType+".limit", rpsForIdentify)
			config.Set("Router.throttler.WEBHOOK."+identifyEventType+".maxLimit", rpsForIdentify)
			config.Set("Router.throttler.WEBHOOK."+identifyEventType+".minLimit", rpsForIdentify)
			config.Set("Router.throttler.WEBHOOK."+trackEventType+".limit", rpsForTrack)
			config.Set("Router.throttler.WEBHOOK."+trackEventType+".maxLimit", rpsForTrack)
			config.Set("Router.throttler.WEBHOOK."+trackEventType+".minLimit", rpsForTrack)
			config.Set("Router.throttler.WEBHOOK.limit", rpsForTrack) // use the higher limit as a fallback
			config.Set("Router.throttler.WEBHOOK.timeWindow", "1s")
			throttlingConfigOverrides()

			// generatorLoop
			config.Set("Router.jobQueryBatchSize", jobsPerEventType*2) // be able to query all jobs at once
			config.Set("Router.readSleep", "10ms")
			config.Set("Router.minRetryBackoff", "5ms")
			config.Set("Router.maxRetryBackoff", "10ms")

			// statusInsertLoop
			config.Set("Router.updateStatusBatchSize", jobsPerEventType*2) // be able to insert all statuses at once
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
				c := r.Run(ctx, []string{"throttling-per-event-type-test-rudder-server"})
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
			require.Equal(t, spec.receivedPerEventType[identifyEventType], spec.receivedPerEventType[trackEventType], "webhook should have received same number of identify and track events")
			if expectTrackBeforeIdentify {
				require.Greater(t, spec.lastReceivedAt[identifyEventType], spec.lastReceivedAt[trackEventType], "last received identify event should be after last received track event because rps for identify is less than for track")
			} else {
				require.Less(t, spec.lastReceivedAt[identifyEventType], spec.lastReceivedAt[trackEventType], "last received identify event should be before last received track event even though rps for track is less than for identify")
			}

			t.Logf("All jobs arrived in expected order - test passed!")
			passed = true
			cancel()
			<-svcDone
		}
	}

	// when throttling per event type is disabled, then we expect track events to be processed after identify events, because iteration will be stopping
	t.Run("throttling per event type disabled", testScenario(false, func() {
		config.Set("Router.isolationMode", "destination")
		config.Set("Router.throttler.adaptiveEnabled", false)
		config.Set("Router.throttler.WEBHOOK.throttlerPerEventType", false)
	}))

	// when throttling per event type is enabled, then we expect track events to be processed before identify events, because iteration will not be stopping
	// and the rate limit for track events is higher than that for identify events
	t.Run("static throttling with destination level isolation", testScenario(true, func() {
		config.Set("Router.isolationMode", "destination")
		config.Set("Router.throttler.adaptiveEnabled", false)
	}))
	t.Run("adaptive throttling with destination level isolation", testScenario(true, func() {
		config.Set("Router.isolationMode", "destination")
		config.Set("Router.throttler.adaptiveEnabled", true)
	}))
	t.Run("static throttling with workspace level isolation", testScenario(true, func() {
		config.Set("Router.isolationMode", "workspace")
		config.Set("Router.throttler.adaptiveEnabled", false)
	}))
}

// Using a struct to keep main_test package clean and
// avoid function collisions with other tests
// TODO: Move server's Run() out of main package
type throttlingPerEventTypeMethods struct{}

// newTestSpec creates a new throttlingPerEventTypeSpec with the given number of jobs per event type.
// It generates jobs for two event types: "identify" and "track", first for "identify" and then for "track".
func (m throttlingPerEventTypeMethods) newTestSpec(jobsPerEventType int) *throttlingPerEventTypeSpec {
	var s throttlingPerEventTypeSpec
	s.jobsOrdered = make([]*throttlingPerEventTypeJobSpec, jobsPerEventType*2)
	s.receivedPerEventType = map[string]int{}
	s.lastReceivedAt = map[string]time.Time{}

	var idx int
	for _, eventType := range []string{"identify", "track"} {
		for i := 0; i < jobsPerEventType; i++ {
			js := throttlingPerEventTypeJobSpec{
				userID:    uuid.New().String(),
				eventType: eventType,
			}
			s.jobsOrdered[idx] = &js
			idx++
		}
	}
	return &s
}

// newWebhook creates a new webhook server that will receive events and track their counts.
// It will also check that the "type" field in the request body is present and is a string.
// The server will respond with a 200 status code and the response body will contain the status code.
// It will also update the spec with the received counts and last received time for each event type
func (throttlingPerEventTypeMethods) newWebhook(t *testing.T, spec *throttlingPerEventTypeSpec) *throttlingPerEventTypeWebhook {
	var wh throttlingPerEventTypeWebhook
	wh.spec = spec

	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err, "should be able to read the request body")
		typeResult := gjson.GetBytes(body, "type")
		require.True(t, typeResult.Exists(), "should have type in the request", body)
		require.Equal(t, gjson.String, typeResult.Type, "type field should be a string", body)
		eventType := typeResult.String()

		wh.spec.mu.Lock()
		defer wh.spec.mu.Unlock()
		wh.spec.received++
		wh.spec.receivedPerEventType[eventType]++
		wh.spec.lastReceivedAt[eventType] = time.Now()
		t.Logf("Received %s event, total received: %d", eventType, wh.spec.receivedPerEventType[eventType])
		w.WriteHeader(200)
	}))
	return &wh
}

// splitInBatches creates batches of jobs with the specified batch size
func (throttlingPerEventTypeMethods) splitInBatches(jobs []*throttlingPerEventTypeJobSpec, batchSize int) [][]byte {
	payloads := lo.Map(jobs, func(job *throttlingPerEventTypeJobSpec, _ int) string { return job.payload() })
	batches := lo.Chunk(payloads, batchSize)
	jsonBatches := lo.Map(batches, func(batch []string, _ int) []byte {
		return []byte(fmt.Sprintf(`{"batch":[%s]}`, strings.Join(batch, ",")))
	})
	return jsonBatches
}

type throttlingPerEventTypeWebhook struct {
	Server *httptest.Server
	spec   *throttlingPerEventTypeSpec
}

type throttlingPerEventTypeSpec struct {
	jobsOrdered []*throttlingPerEventTypeJobSpec

	mu                   sync.Mutex
	received             int
	receivedPerEventType map[string]int
	lastReceivedAt       map[string]time.Time
}

type throttlingPerEventTypeJobSpec struct {
	userID    string
	eventType string
}

func (jobSpec *throttlingPerEventTypeJobSpec) payload() string {
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
