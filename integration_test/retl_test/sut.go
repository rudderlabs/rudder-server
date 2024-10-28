package retltest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

type dst interface {
	ID() string
	Name() string
	TypeName() string
	Config() map[string]interface{}

	Start(t *testing.T)
	Shutdown(t *testing.T)

	Count() int
}

type src interface {
	ID() string
}

// SUT is a System Under Test, running a rudder-server instance and a set of destinations.
type SUT struct {
	cancel context.CancelFunc
	done   chan struct{}

	URL string

	workspaceID string
	Sources     []srcWithDst
}

type srcWithDst struct {
	source       src
	destinations []dst
}

type batch struct {
	Batch []record `json:"batch"`
}

type record struct {
	Context recordContext `json:"context"`

	Type      string    `json:"type"`
	MessageID string    `json:"messageId"`
	UserID    string    `json:"userId"`
	SentAt    time.Time `json:"sentAt"`
	Timestamp time.Time `json:"timestamp"`
}

type rudderSource struct {
	JobID     string `json:"job_id"`
	JobRunID  string `json:"job_run_id"`
	TaskRunID string `json:"task_run_id"`
}

type recordContext struct {
	Sources rudderSource `json:"sources"`
}

func Connect(s src, d ...dst) srcWithDst {
	return srcWithDst{
		source:       s,
		destinations: d,
	}
}

// Start rudder-server, its dependencies and the destinations. It blocks until rudder-server and destinations are ready.
func (s *SUT) Start(t *testing.T) {
	svcCtx, svcCancel := context.WithCancel(context.Background())
	s.cancel = svcCancel
	s.done = make(chan struct{})

	s.workspaceID = rand.String(27)

	setupStart := time.Now()
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	config.Reset()
	logger.Reset()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	containersGroup, _ := errgroup.WithContext(context.TODO())

	var postgresContainer *postgres.Resource
	var transformerContainer *transformertest.Resource

	containersGroup.Go(func() (err error) {
		postgresContainer, err = postgres.Setup(pool, t)
		if err != nil {
			return err
		}
		return nil
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		return err
	})

	for _, srcWithDst := range s.Sources {
		for _, dst := range srcWithDst.destinations {
			dst := dst
			containersGroup.Go(func() (err error) {
				dst.Start(t)
				return nil
			})
		}
	}
	require.NoError(t, containersGroup.Wait())

	if err := godotenv.Load("../../testhelper/.env"); err != nil {
		t.Log("INFO: No .env file found.")
	}
	t.Setenv("JOBS_DB_HOST", postgresContainer.Host)
	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformerURL)
	t.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))

	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(t, err)

	httpPort := strconv.Itoa(httpPortInt)
	s.URL = fmt.Sprintf("http://localhost:%s", httpPort)

	t.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")

	// quick looops
	t.Setenv("RSERVER_PROCESSOR_READ_LOOP_SLEEP", "50ms")
	t.Setenv("RSERVER_PROCESSOR_MAX_LOOP_SLEEP", "100ms")
	t.Setenv("RSERVER_ROUTER_READ_SLEEP", "50ms")
	t.Setenv("RSERVER_ROUTER_UPDATE_STATUS_BATCH_SIZE", "5")
	t.Setenv("RSERVER_ROUTER_MAX_STATUS_UPDATE_WAIT", "50ms")

	tmpFile, err := os.CreateTemp("", "workspaceConfig.*.json")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	require.NoError(t, json.NewEncoder(tmpFile).Encode(s.generateConfig()))
	require.NoError(t, tmpFile.Close())

	workspaceConfigPath := tmpFile.Name()

	if testing.Verbose() {
		data, err := os.ReadFile(workspaceConfigPath)
		require.NoError(t, err)
		t.Logf("Workspace config: %s", string(data))
	}

	t.Log("workspace config path:", workspaceConfigPath)
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	t.Logf("--- Setup done (%s)", time.Since(setupStart))

	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(svcCtx, []string{"retl-test-rudder-server"})
		close(s.done)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	t.Log("serviceHealthEndpoint", serviceHealthEndpoint)
	health.WaitUntilReady(
		context.Background(), t,
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
		"serviceHealthEndpoint",
	)
}

func (s *SUT) generateConfig() map[string]any {
	ss := make([]map[string]any, len(s.Sources))
	for i, srcWithDst := range s.Sources {
		ss[i] = map[string]any{
			"id":                 srcWithDst.source.ID(),
			"name":               fmt.Sprintf("test_source_ %d", i),
			"writeKey":           "{{.writeKey}}",
			"enabled":            true,
			"sourceDefinitionId": "xxxyyyzzpWDzNxgGUYzq9sZdZZB",
			"createdBy":          "xxxyyyzzueyoBz4jb7bRdOzDxai",
			"workspaceId":        s.workspaceID,
			"createdAt":          "2021-08-27T06:33:00.305Z",
			"updatedAt":          "2021-08-27T06:33:00.305Z",
			"destinations":       []any{},
		}
		dd := make([]map[string]any, len(srcWithDst.destinations))
		for i, dst := range srcWithDst.destinations {
			dd[i] = map[string]any{
				"id":                      dst.ID(),
				"name":                    dst.TypeName(),
				"dispalyName":             dst.Name(),
				"config":                  dst.Config(),
				"enabled":                 true,
				"destinationDefinitionId": "xxxyyyzzSOU9pLRavMf0GuVnWV3",
				"createdBy":               "xxxyyyzzueyoBz4jb7bRdOzDxai",
				"workspaceId":             s.workspaceID,
				"createdAt":               "2021-08-27T06:33:00.305Z",
				"updatedAt":               "2021-08-27T06:33:00.305Z",
				"isConnectionEnabled":     true,
				"isProcessorEnabled":      true,
				"destinationDefinition": map[string]any{
					"config": map[string]any{
						"transformAt":   "processor",
						"transformAtV1": "processor",
						"supportedMessageTypes": []string{
							"alias",
							"group",
							"identify",
							"page",
							"screen",
							"track",
						},
						"saveDestinationResponse": false,
					},
					"id":          "xxxyyyzzSOU9pLRavMf0GuVnWV3",
					"name":        dst.TypeName(),
					"displayName": strings.ToLower(dst.TypeName()),
					"createdAt":   "2020-03-16T19:25:28.141Z",
					"updatedAt":   "2021-08-26T07:06:01.445Z",
				},
			}
		}
		ss[i]["destinations"] = dd
	}
	c := map[string]any{
		"enableMetrics": false,
		"workspaceId":   s.workspaceID,
		"sources":       ss,
		"libraries":     []any{},
	}
	return c
}

func (s *SUT) Shutdown(t *testing.T) {
	for _, srcWithDst := range s.Sources {
		for _, dst := range srcWithDst.destinations {
			dst.Shutdown(t)
		}
	}

	s.cancel()
	<-s.done
}

// SendRETL sends a batch of records to the rETL endpoint.
func (s *SUT) SendRETL(t *testing.T, sourceID, destinationID string, payload batch) {
	t.Helper()
	t.Logf("Sending rETL Events: %s -> %s", sourceID, destinationID)

	var (
		httpClient = &http.Client{}
		retlURL    = fmt.Sprintf("%s/internal/v1/retl", s.URL)
	)

	b, err := json.MarshalIndent(payload, "", "  ")
	require.NoError(t, err)

	t.Logf("sending records: %s", b)

	req, err := http.NewRequest(http.MethodPost, retlURL, bytes.NewReader(b))
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Rudder-Source-Id", sourceID)
	req.Header.Add("X-Rudder-Destination-Id", destinationID)

	res, err := httpClient.Do(req)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		t.Fail()
		return
	}
	if res.Status != "200 OK" {
		t.Logf("sendEvent error: %v", string(body))
		t.Fail()
		return
	}

	t.Logf("Event Sent Successfully: (%s)", body)
}

// JobStatus hits the job-status endpoint and returns the status for a sourceID, jobRunID, jobTaskID.
// If the job is not found, the second return value is false. Any other error is logged and the test is failed.
func (s *SUT) JobStatus(t *testing.T, sourceID, jobRunID, jobTaskID string) (rsources.JobStatus, bool) {
	var (
		httpClient   = &http.Client{}
		jobStatusURL = fmt.Sprintf("%s/v1/job-status/%s?%s", s.URL, jobRunID, url.Values{
			"task_run_id": []string{jobTaskID},
		}.Encode()) // job_run_id
	)

	var status rsources.JobStatus

	res, err := httpClient.Get(jobStatusURL)
	if err != nil {
		t.Logf("job-status error: %v", err)
		return status, false
	}
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Logf("job-status error: %v", err)
		t.Fail()
		return status, false
	}

	switch res.StatusCode {
	case http.StatusOK:
		err = json.Unmarshal(body, &status)
		if err != nil {
			t.Logf("job-status error: %v", err)
			t.Fail()
			return status, false
		}

		t.Logf("Status response: %s", body)
		return status, true
	case http.StatusNotFound:
		t.Logf("job-status not found: %v", string(body))
		return status, false
	default:
		t.Logf("job-status unexpected error: %v", string(body))
		t.FailNow()
		return status, false
	}
}
