package router_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
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
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
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
//
// A second scenario verifies that when [eventOrderKeyThreshold] is enabled jobs are delivered out-of-order even if event ordering is enabled
func TestEventOrderGuarantee(t *testing.T) {
	eventOrderTest := func(eventOrderKeyThreshold bool) func(t *testing.T) {
		return func(t *testing.T) {
			// necessary until we move away from a singleton config
			config.Reset()
			defer config.Reset()

			const (
				users         = 50                   // how many userIDs we will send jobs for
				jobsPerUser   = 40                   // how many jobs per user we will send
				batchSize     = 10                   // how many jobs for the same user we will send in each batch request
				responseDelay = 2 * time.Millisecond // how long we will the webhook wait before sending a response
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
			spec.eventOrderKeyThreshold = eventOrderKeyThreshold
			spec.responseDelay = responseDelay

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
			configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/eventOrderTestTemplate.json", templateCtx)
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
			config.Set("Router.jobIterator.maxQueries", 100)
			config.Set("Router.jobIterator.discardedPercentageTolerance", 0)
			if eventOrderKeyThreshold {
				config.Set("Router.eventOrderKeyThreshold", 1)
			} else {
				config.Set("Router.eventOrderKeyThreshold", 0)
			}

			// generatorLoop
			config.Set("Router.jobQueryBatchSize", 500)
			config.Set("Router.readSleep", "10ms")
			config.Set("Router.minRetryBackoff", "5ms")
			config.Set("Router.maxRetryBackoff", "10ms")

			// worker
			config.Set("Router.noOfJobsPerChannel", "100")
			config.Set("Router.jobsBatchTimeout", "100ms")

			// statusInsertLoop
			config.Set("Router.updateStatusBatchSize", 100)
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
				c := r.Run(ctx, []string{"eventorder-test-rudder-server"})
				t.Logf("server stopped: %d", c)
				if c != 0 {
					t.Errorf("server exited with a non-0 exit code: %d", c)
				}
				close(svcDone)
			}()

			go func() { // randomly drain jobs
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(150 * time.Millisecond):
						key := fmt.Sprintf("Router.%s.jobRetention", destinationID)
						config.Set(key, "1s")
						time.Sleep(1 * time.Millisecond)
						config.Set(key, "10m")
					}
				}
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

			go func() {
				t.Logf("Sending %d total events for %d total users in %d batches", len(spec.jobsOrdered), users, len(batches))
				client := &http.Client{}
				url := fmt.Sprintf("http://localhost:%s/v1/batch", gatewayPort)
				for _, payload := range batches {
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
				total := len(spec.jobs)
				drained := m.countDrainedJobs(postgresContainer.DB)
				if done != len(spec.done)+drained {
					done = len(spec.done) + drained
					t.Logf("%d/%d done (%d drained)", done, total, drained)
				}
				return done == total
			}, 120*time.Second, 2*time.Second, "webhook should receive all events and process them till the end")

			require.False(t, t.Failed(), "webhook shouldn't have failed")

			if !eventOrderKeyThreshold {
				for userID, jobs := range spec.doneOrdered {
					var previousJobID int
					for _, jobID := range jobs {
						require.Greater(t, jobID, previousJobID, "%s: jobID %d should be greater than previous jobID %d", userID, jobID, previousJobID)
						previousJobID = jobID
					}
				}
			} else {
				unsorted := lo.Filter(lo.Values(spec.doneOrdered), func(jobs []int, _ int) bool {
					return !lo.IsSorted(jobs)
				})
				require.Greater(t, len(unsorted), 0, "some jobs should be received out-of-order even if event ordering is enabled due to eventOrderKeyThreshold > 0")
			}

			t.Logf("All jobs arrived in expected order - test passed!")
			passed = true
			cancel()
			<-svcDone
		}
	}

	t.Run("pickup with eventOrderKeyThreshold disabled", eventOrderTest(false))

	t.Run("pickup with eventOrderKeyThreshold enabled", eventOrderTest(true))
}

// Using a struct to keep main_test package clean and
// avoid function collisions with other tests
// TODO: Move server's Run() out of main package
type eventOrderMethods struct{}

func (m eventOrderMethods) newTestSpec(users, jobsPerUser int) *eventOrderSpec {
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
	newRand := rand.New(rand.NewSource(time.Now().UnixNano())) // skipcq: GSC-G404
	statuses := []int{
		http.StatusBadRequest, http.StatusBadRequest, http.StatusBadRequest, http.StatusBadRequest,
		http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK,
		http.StatusInternalServerError,
	}
	status = statuses[newRand.Intn(len(statuses))] // skipcq: GSC-G404
	terminal = status != http.StatusInternalServerError
	return status, terminal
}

func (eventOrderMethods) newWebhook(t *testing.T, spec *eventOrderSpec) *eventOrderWebhook {
	var wh eventOrderWebhook
	wh.spec = spec

	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newRand := rand.New(rand.NewSource(time.Now().UnixNano())) // skipcq: GSC-G404
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
			if !wh.spec.eventOrderKeyThreshold {
				require.Greater(t, jobID, lastDoneId, "received out-of-order event for user %s: job %d after jobs %+v", userID, jobID, wh.spec.doneOrdered[userID])
			}
			wh.spec.done[jobID] = struct{}{}
			wh.spec.doneOrdered[userID] = append(wh.spec.doneOrdered[userID], jobID)
			// t.Logf("job %d done", jobID)
		}
		if spec.responseDelay > 0 {
			time.Sleep(time.Duration(newRand.Intn(int(spec.responseDelay.Milliseconds()))) * time.Millisecond) // skipcq: GSC-G404
		}
		w.WriteHeader(responseCode)
		_, err = fmt.Fprintf(w, "%d", responseCode)
		require.NoError(t, err, "should be able to write the response code to the response")
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

func (eventOrderMethods) countDrainedJobs(db *sql.DB) int {
	var tables []string
	var count int

	rows, err := db.Query(`SELECT tablename
									FROM pg_catalog.pg_tables
									WHERE tablename like 'rt_job_status_%'`)
	if err == nil {
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			if err == nil {
				tables = append(tables, table)
			}
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}
	}

	for _, table := range tables {
		var dsCount int
		_ = db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE error_code = $1`, utils.DRAIN_ERROR_CODE).Scan(&count)
		count += dsCount
	}
	return count
}

type eventOrderWebhook struct {
	mu     sync.Mutex
	Server *httptest.Server
	spec   *eventOrderSpec
}

type eventOrderSpec struct {
	eventOrderKeyThreshold bool
	jobs                   map[int]*eventOrderJobSpec
	jobsOrdered            []*eventOrderJobSpec
	received               map[int]int
	responseDelay          time.Duration
	done                   map[int]struct{}
	doneOrdered            map[string][]int
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
