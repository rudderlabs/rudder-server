package main_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/testhelper/health"

	"github.com/ory/dockertest/v3"
	main "github.com/rudderlabs/rudder-server"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	trand "github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

// TestEventOrderGuarantee tests that order delivery guarantees are honoured by the server
// regardless the behaviour of the destination.
// To achieve that we generate a number of jobs per user with each job having a prescribed number
// of responses until it is done (succeeded or aborted)

// e.g.
//
//	user1, job1, responses: [500, 500, 200]
//	user1, job2, responses: [200]
//	user1, job3, responses: [400]
//	user1, job4, responses: [500, 400]

// After sending the jobs to the server, we verify that the destination has received the jobs in the
// correct order. We also verify that the server has not sent any job twice.
func TestEventOrderGuarantee(t *testing.T) {
	const (
		users         = 50                   // how many userIDs we will send jobs for
		jobsPerUser   = 40                   // how many jobs per user we will send
		batchSize     = 10                   // how many jobs for the same user we will send in each batch request
		responseDelay = 0 * time.Millisecond // how long we will the webhook wait before sending a response
	)
	var (
		m      eventOrderMethods
		passed bool
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// generate the spec we are going to use
	// this will create a number of jobs for a number of users
	// and prescribe the sequence of status codes that the webhook will return for each one e.g. 500, 500, 200
	spec := m.newTestSpec(users, jobsPerUser)
	spec.responseDelay = responseDelay

	t.Logf("Starting docker services (postgres & transformer)")
	var (
		postgresContainer    *destination.PostgresResource
		transformerContainer *destination.TransformerResource
		gatewayPort          string
	)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	containersGroup, _ := errgroup.WithContext(ctx)
	containersGroup.Go(func() (err error) {
		postgresContainer, err = destination.SetupPostgres(pool, t)
		return err
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
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
	templateCtx := map[string]string{
		"webhookUrl":  webhook.Server.URL,
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/eventOrderTestTemplate.json", templateCtx)
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", configJsonPath)

	t.Logf("Starting rudder-server")
	t.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))
	t.Setenv("RSERVER_RECOVERY_STORAGE_PATH", path.Join(t.TempDir(), "/recovery_data.json"))

	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("JOBS_DB_USER", postgresContainer.User)
	t.Setenv("JOBS_DB_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgresContainer.Password)
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)

	t.Setenv("RSERVER_WAREHOUSE_MODE", "off")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "false")

	// generatorLoop
	t.Setenv("RSERVER_ROUTER_JOB_QUERY_BATCH_SIZE", "500")
	t.Setenv("RSERVER_ROUTER_READ_SLEEP", "10ms")
	t.Setenv("RSERVER_ROUTER_MIN_RETRY_BACKOFF", "5ms")
	t.Setenv("RSERVER_ROUTER_MAX_RETRY_BACKOFF", "10ms")
	t.Setenv("RSERVER_ROUTER_ALLOW_ABORTED_USER_JOBS_COUNT_FOR_PROCESSING", "1")

	// worker
	t.Setenv("RSERVER_ROUTER_NO_OF_JOBS_PER_CHANNEL", "100")
	t.Setenv("RSERVER_ROUTER_JOBS_BATCH_TIMEOUT", "100ms")

	// statusInsertLoop
	t.Setenv("RSERVER_ROUTER_UPDATE_STATUS_BATCH_SIZE", "100")
	t.Setenv("RSERVER_ROUTER_MAX_STATUS_UPDATE_WAIT", "10ms")

	// hack
	// t.Setenv("RSERVER_ROUTER_GUARANTEE_USER_EVENT_ORDER", "false")

	// find free port for gateway http server to listen on
	httpPortInt, err := testhelper.GetFreePort()
	require.NoError(t, err)
	gatewayPort = strconv.Itoa(httpPortInt)

	t.Setenv("RSERVER_GATEWAY_WEB_PORT", gatewayPort)
	t.Setenv("RUDDER_TMPDIR", os.TempDir())

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
		_ = main.Run(ctx)
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
	t.Logf("Sending %d total events for %d total users in %d batches", len(spec.jobsOrdered), users, len(batches))
	client := &http.Client{}
	for _, payload := range batches {
		url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		require.NoError(t, err, "should be able to create a new request")
		req.SetBasicAuth(writeKey, "password")
		resp, err := client.Do(req)
		require.NoError(t, err, "should be able to send the request to gateway")
		require.Equal(t, http.StatusOK, resp.StatusCode, "should be able to send the request to gateway successfully", payload)
		resp.Body.Close()
	}

	t.Logf("Waiting for the magic to happen... eventually")

	var done int
	require.Eventually(t, func() bool {
		if t.Failed() { // webhook failed, no point in keep retrying
			return true
		}
		total := len(spec.jobs)
		if done != len(spec.done) {
			done = len(spec.done)
			t.Logf("%d/%d done", done, total)
		}
		return done == total
	}, 300*time.Second, 2*time.Second, "webhook should receive all events and process them till the end")

	require.False(t, t.Failed(), "webhook shouldn't have failed")

	for userID, jobs := range spec.doneOrdered {
		var previousJobID int
		for _, jobID := range jobs {
			require.Greater(t, jobID, previousJobID, "%s: jobID %d should be greater than previous jobID %d", userID, jobID, previousJobID)
			previousJobID = jobID
		}
	}

	t.Logf("All jobs arrived in expected order - test passed!")
	passed = true
	cancel()
	<-svcDone
}

// Using a struct to keep main_test package clean and
// avoid method collisions with other tests
// TODO: Move server's Run() out of main package
type eventOrderMethods struct{}

func (m eventOrderMethods) newTestSpec(users, jobsPerUser int) *eventOrderSpec {
	rand.Seed(time.Now().UnixNano())
	var s eventOrderSpec
	s.jobsOrdered = make([]*eventOrderJobSpec, jobsPerUser*users)
	s.jobs = map[int]*eventOrderJobSpec{}
	s.received = map[int]int{}
	s.done = map[int]struct{}{}
	s.doneOrdered = map[string][]int{}

	var idx int
	for u := 0; u < users; u++ {
		userID := trand.String(27)
		for i := 0; i < jobsPerUser; i++ {
			jobID := idx + 1

			var statuses []int
			var terminal bool
			for !terminal {
				var status int
				status, terminal = m.randomStatus()
				statuses = append(statuses, status)
			}
			js := eventOrderJobSpec{
				id:        jobID,
				userID:    userID,
				responses: statuses,
			}
			s.jobsOrdered[idx] = &js
			s.jobs[jobID] = &js
			idx++
		}
	}
	return &s
}

func (eventOrderMethods) randomStatus() (status int, terminal bool) {
	// playing with probabilities: 50% HTTP 500, 40% HTTP 200, 10% HTTP 400
	statuses := []int{
		http.StatusBadRequest, http.StatusBadRequest, http.StatusBadRequest, http.StatusBadRequest,
		http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK,
		http.StatusInternalServerError,
	}
	status = statuses[rand.Intn(len(statuses))]
	terminal = status != http.StatusInternalServerError
	return status, terminal
}

func (eventOrderMethods) newWebhook(t *testing.T, spec *eventOrderSpec) *eventOrderWebhook {
	var wh eventOrderWebhook
	wh.spec = spec

	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err, "should be able to read the request body")
		testJobId := gjson.GetBytes(body, "testJobId")
		require.True(t, testJobId.Exists(), "should have testJobId in the request", body)
		require.Equal(t, gjson.Number, testJobId.Type, "testJobId should be a number", body)
		jobID := int(gjson.GetBytes(body, "testJobId").Int())

		userIdResult := gjson.GetBytes(body, "userId")
		require.True(t, userIdResult.Exists(), "should have userId in the request", body)
		require.Equal(t, gjson.String, userIdResult.Type, "userId should be a string", body)
		userID := userIdResult.String()

		wh.mu.Lock()
		defer wh.mu.Unlock()

		jobSpec, ok := wh.spec.jobs[jobID]
		require.True(t, ok, "should be able to find the job spec for job: %d", jobID)
		_, ok = wh.spec.done[jobID]
		require.False(t, ok, "shouldn't receive a request for a job that is already done: %d", jobID)
		times := wh.spec.received[jobID]
		require.True(t, times < len(jobSpec.responses), "shouldn't receive more requests than the number of responses for job: %d", jobID)
		responseCode := jobSpec.responses[times]
		require.NotEqual(t, 0, responseCode, "should be able to find the next response code to send for job: %d", jobID)
		wh.spec.received[jobID] = times + 1

		if responseCode != 500 { // job is now done
			require.Equal(t, len(jobSpec.responses), wh.spec.received[jobID], "should have received all the expected requests for job %d before marking it as done", jobID)

			var lastDoneId int
			if len(wh.spec.doneOrdered[userID]) > 0 {
				lastDoneId = wh.spec.doneOrdered[userID][len(wh.spec.doneOrdered[userID])-1]
			}
			require.Greater(t, jobID, lastDoneId, "received out-of-order event for user %s: job %d after jobs %+v", userID, jobID, wh.spec.doneOrdered[userID])
			wh.spec.done[jobID] = struct{}{}
			wh.spec.doneOrdered[userID] = append(wh.spec.doneOrdered[userID], jobID)
			// t.Logf("job %d done", jobID)
		}
		if spec.responseDelay > 0 {
			time.Sleep(time.Duration(rand.Intn(int(spec.responseDelay.Milliseconds()))) * time.Millisecond)
		}

		w.WriteHeader(responseCode)
		_, error := w.Write([]byte(fmt.Sprintf("%d", responseCode)))
		require.NoError(t, error, "should be able to write the response code to the response")
	}))
	return &wh
}

// splitInBatches creates batches of jobs from the same user and shuffles them so that batches
// for the same user do not appear one after the other. The shuffling algorithm respects ordering of
// batches at user level
func (eventOrderMethods) splitInBatches(jobs []*eventOrderJobSpec, batchSize int) [][]byte {
	payloads := map[string][]string{}
	for _, job := range jobs {
		payloads[job.userID] = append(payloads[job.userID], job.payload())
	}

	batches := map[string][][]byte{}
	for userID, userPayloads := range payloads {
		var userBatches [][]string

		for batchSize < len(userPayloads) {
			userPayloads, userBatches = userPayloads[batchSize:], append(userBatches, userPayloads[0:batchSize])
		}
		userBatches = append(userBatches, userPayloads)
		var userJsonBatches [][]byte
		for _, batch := range userBatches {
			userJsonBatches = append(userJsonBatches, []byte(fmt.Sprintf(`{"batch":[%s]}`, strings.Join(batch, ","))))
		}
		batches[userID] = userJsonBatches
	}
	var jsonBatches [][]byte
	var done bool
	for i := 0; !done; i++ {
		previousLength := len(jsonBatches)
		for _, userJsonBatches := range batches {
			if len(userJsonBatches) > i {
				jsonBatches = append(jsonBatches, userJsonBatches[i])
			}
		}
		if len(jsonBatches) == previousLength {
			done = true
		}
	}

	return jsonBatches
}

type eventOrderWebhook struct {
	mu     sync.Mutex
	Server *httptest.Server
	spec   *eventOrderSpec
}

type eventOrderSpec struct {
	jobs          map[int]*eventOrderJobSpec
	jobsOrdered   []*eventOrderJobSpec
	received      map[int]int
	responseDelay time.Duration
	done          map[int]struct{}
	doneOrdered   map[string][]int
}

type eventOrderJobSpec struct {
	id        int
	userID    string
	responses []int
}

func (jobSpec *eventOrderJobSpec) payload() string {
	template := `{
				"userId": %q,
				"anonymousId": %q,
				"testJobId": %d,
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
	return fmt.Sprintf(template, jobSpec.userID, jobSpec.userID, jobSpec.id)
}
