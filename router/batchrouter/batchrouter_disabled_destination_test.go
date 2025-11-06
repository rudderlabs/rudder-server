package batchrouter_test

import (
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

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/testhelper/transformertest"

	"github.com/samber/lo/mutable"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/isolation"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestBatchrouterDisabledDestination(t *testing.T) {
	const (
		noOfJobs = 100
	)
	runner := func(mode isolation.Mode) func(t *testing.T) {
		return func(t *testing.T) {
			spec := NewBatchrouterDisabledDestinationScenarioSpec(mode, noOfJobs)
			BatchrouterDisabledDestinationScenario(t, spec)
		}
	}
	t.Run("no isolation", runner(isolation.ModeNone))
	t.Run("workspace isolation", runner(isolation.ModeWorkspace))
	t.Run("destination isolation", runner(isolation.ModeDestination))
}

// NewBatchrouterDisabledDestinationScenarioSpec is a specification for a batchrouter disabled destination scenario.
// - isolationMode is the isolation mode to use.
// - noOfEvents is the number of events to create in jobsdb.
func NewBatchrouterDisabledDestinationScenarioSpec(isolationMode isolation.Mode, noOfEvents int) *BrtDisabledDestinationScenarioSpec {
	var s BrtDisabledDestinationScenarioSpec
	s.jobQueryBatchSize = noOfEvents
	s.isolationMode = isolationMode
	s.jobs = make([]*brtDisabledDestinationJobSpec, noOfEvents)

	var idx int
	workspaceID := "workspace-0"
	for i := 0; i < noOfEvents; i = i + 1 {
		s.jobs[idx] = &brtDisabledDestinationJobSpec{
			id:          int64(idx + 1),
			workspaceID: workspaceID,
			userID:      strconv.Itoa(idx + 1),
			destType:    "MINIO",
		}
		idx++
	}
	return &s
}

// BatchrouterDisabledDestinationScenario runs a scenario with the given spec which:
// 1. Sends all events to batch_rt tables
// 2. Starts the server
// 3. Waits for the events to be aborted by batch router
func BatchrouterDisabledDestinationScenario(t testing.TB, spec *BrtDisabledDestinationScenarioSpec) {
	var m brtDisabledDestinationMethods

	config.Reset()
	defer logger.Reset()
	defer config.Reset()
	config.Set("LOG_LEVEL", "ERROR")
	logger.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Logf("Starting docker containers")
	var (
		postgresContainer *postgres.Resource
		minioDestination  *minio.Resource
	)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "it should be able to create a new docker pool")
	containersGroup, _ := errgroup.WithContext(ctx)
	containersGroup.Go(func() (err error) {
		t.Logf("Starting postgres container")
		postgresContainer, err = postgres.Setup(pool, t, postgres.WithOptions("max_connections=1000"), postgres.WithShmSize(256*bytesize.MB))
		return err
	})
	containersGroup.Go(func() (err error) {
		t.Logf("Starting minio container")
		minioDestination, err = minio.Setup(pool, t)
		return err
	})
	require.NoError(t, containersGroup.Wait(), "it should be able to start all containers without an error")
	t.Logf("Docker containers started")

	t.Logf("Setting up the mock config backend")
	templateCtx := map[string]any{
		"workspace":            "workspace-0",
		"minioEndpoint":        minioDestination.Endpoint,
		"minioBucket":          minioDestination.BucketName,
		"minioAccessKeyID":     minioDestination.AccessKeyID,
		"minioSecretAccessKey": minioDestination.AccessKeySecret,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/brtDisabledDestinationTestTemplate.json.tpl", templateCtx)
	mockCBE := m.newMockConfigBackend(t, configJsonPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)
	defer mockCBE.Close()

	trServer := transformertest.NewBuilder().Build()
	defer trServer.Close()
	config.Set("DEST_TRANSFORM_URL", trServer.URL)

	t.Logf("Setting up the mock warehouse")
	mockWH := m.newMockWarehouse()
	config.Set("WAREHOUSE_URL", mockWH.URL)
	defer mockWH.Close()

	t.Logf("Preparing the necessary configuration")
	gatewayPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	config.Set("Gateway.webPort", gatewayPort)
	config.Set("Profiler.Enabled", false)
	config.Set("forceStaticModeProvider", true)
	config.Set("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	config.Set("WORKSPACE_NAMESPACE", "brt_isolation_test")
	config.Set("HOSTED_SERVICE_SECRET", "brt_isolation_secret")
	config.Set("recovery.storagePath", path.Join(t.TempDir(), "/recovery_data.json"))

	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DB.host", postgresContainer.Host)
	config.Set("enableStats", false)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")

	config.Set("BatchRouter.isolationMode", string(spec.isolationMode))
	config.Set("BatchRouter.jobQueryBatchSize", spec.jobQueryBatchSize)
	config.Set("BatchRouter.minIdleSleep", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("BatchRouter.Limiter.statsPeriod", "1s")

	config.Set("JobsDB.enableWriterQueue", false)
	config.Set("RUDDER_TMPDIR", os.TempDir())

	t.Logf("Seeding batch_rt jobsdb with jobs")
	m.seedBrtDB(t, spec)

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
		c := r.Run(ctx, []string{"brt-isolation-test-rudder-server"})
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

	t.Logf("Waiting for all batch_rt jobs to be aborted")
	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',20) WHERE job_state = 'aborted' and error_code = '410'").Scan(&processedJobCount))
		return processedJobCount == len(spec.jobs)
	}, 5*time.Minute, 1*time.Second, "all batch_rt jobs should be aborted")

	cancel()
	<-svcDone
}

type BrtDisabledDestinationScenarioSpec struct {
	isolationMode     isolation.Mode
	jobs              []*brtDisabledDestinationJobSpec
	jobQueryBatchSize int
}

type brtDisabledDestinationJobSpec struct {
	id          int64
	workspaceID string
	userID      string
	destType    string
}

func (jobSpec *brtDisabledDestinationJobSpec) payload() []byte {
	var template string
	if jobSpec.destType == "MINIO" {
		template = `{"userId": %[1]q,"anonymousId": %[2]q,"testJobId": %[3]d,"workspaceID": %[4]q,"destType": %[6]q,"type": "identify","context":{"traits":{"trait1": "new-val"},"ip": "14.5.67.21","library":{"name": "http"}},"timestamp": "2020-02-02T00:23:09.544Z","receivedAt": %[5]q}`
	} else {
		template = `{"data": {"userId": %[1]q,"anonymousId": %[2]q,"testJobId": %[3]d,"workspaceID": %[4]q,"destType": %[6]q,"type": "identify","context_traits_trait1": "new-val","context_ip": "14.5.67.21","context_library_name": "http","timestamp": "2020-02-02T00:23:09.544Z"},"userId": %[1]q,"metadata": {"table": "pages","columns": {"userId": "string","anonymousId": "string","testJobId": "int","workspaceID": "string","destType": "string","type": "string","context_traits_trait1": "string","context_ip": "string","context_library_name": "string","timestamp": "string"},"receivedAt": %[5]q}}`
	}
	return []byte(fmt.Sprintf(template, jobSpec.userID, jobSpec.userID, jobSpec.id, jobSpec.workspaceID, time.Now().Format(misc.RFC3339Milli), jobSpec.destType))
}

// Using a struct to keep batchrouter_test package clean and
// avoid function collisions with other tests
type brtDisabledDestinationMethods struct{}

// newMockConfigBackend creates a mock config backend server serving the config file at the given path
func (brtDisabledDestinationMethods) newMockConfigBackend(t testing.TB, path string) *httptest.Server {
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

// newMockWarehouse creates a mock warehouse server responding with 200 OK to every request
func (brtDisabledDestinationMethods) newMockWarehouse() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

// seedBrtDB seeds the batch router database with jobs based on the provided spec
func (m brtDisabledDestinationMethods) seedBrtDB(t testing.TB, spec *BrtDisabledDestinationScenarioSpec) {
	brtJobsDB := jobsdb.NewForWrite("batch_rt", jobsdb.WithStats(stats.NOP))
	require.NoError(t, brtJobsDB.Start(), "it should be able to start the jobsdb")
	defer brtJobsDB.Stop()
	for _, batch := range m.generateJobs(spec.jobs, 100) {
		require.NoError(t, brtJobsDB.Store(context.Background(), batch), "it should be able to store the batch of jobs in the jobsdb")
	}
}

// generateJobs creates batches of jobs from the same workspace, shuffled so that
// batches for the same workspace are not consecutive.
func (brtDisabledDestinationMethods) generateJobs(jobs []*brtDisabledDestinationJobSpec, batchSize int) [][]*jobsdb.JobT {
	wsBatches := map[string][]*jobsdb.JobT{}
	for _, job := range jobs {
		payload := job.payload()
		wsBatches[job.workspaceID] = append(wsBatches[job.workspaceID], &jobsdb.JobT{
			UUID:        uuid.New(),
			JobID:       job.id,
			UserID:      job.userID,
			WorkspaceId: job.workspaceID,
			Parameters: []byte(fmt.Sprintf(`{
				"source_id": %[1]q,
				"destination_id": "%[1]s-%[2]s",
				"receivedAt": %[3]q
			}`, job.workspaceID, job.destType, time.Now().Format(misc.RFC3339Milli))),
			CustomVal:    job.destType,
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
