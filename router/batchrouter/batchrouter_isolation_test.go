package batchrouter_test

import (
	"bufio"
	"compress/gzip"
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

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
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

func TestBatchrouterIsolation(t *testing.T) {
	const (
		workspaces       = 10
		jobsPerWorkspace = 100
	)
	runner := func(mode isolation.Mode) func(t *testing.T) {
		return func(t *testing.T) {
			spec := NewBatchrouterIsolationScenarioSpec(mode, workspaces, jobsPerWorkspace)
			spec.verifyDestinations = true
			spec.jobQueryBatchSize = jobsPerWorkspace
			duration := BatchrouterIsolationScenario(t, spec)
			t.Logf("Total processing duration: %v", duration)
		}
	}
	t.Run("no isolation", runner(isolation.ModeNone))
	t.Run("workspace isolation", runner(isolation.ModeWorkspace))
	t.Run("destination isolation", runner(isolation.ModeDestination))
}

// https://snapshots.raintank.io/dashboard/snapshot/r3Yfqat9xfyLvhUFK6jTtH1FZ81S6duh
//
//	go test \
//	  -timeout 3600s \
//	  -run=^$ \
//	  -bench ^BenchmarkBatchrouterIsolationModes$ \
//	  github.com/rudderlabs/rudder-server/router/batchrouter \
//	  -v \
//	  -count=1 |grep BenchmarkBatchrouterIsolationModes
//
// BenchmarkBatchrouterIsolationModes
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_10_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_10_total_jobs_200000-10         	       1	27219135333 ns/op	         6.493 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_10_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_10_total_jobs_200000-10    	       1	26298908458 ns/op	         3.406 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_10_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_10_total_jobs_200000-10  	       1	27923416291 ns/op	         3.907 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_50_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_50_total_jobs_200000-10         	       1	35471540917 ns/op	         8.273 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_50_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_50_total_jobs_200000-10    	       1	37861437542 ns/op	         7.072 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_50_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_50_total_jobs_200000-10  	       1	27295381542 ns/op	         4.545 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_100_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_100_total_jobs_200000-10        	       1	36523980083 ns/op	        10.17 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_100_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_100_total_jobs_200000-10   	       1	36947614792 ns/op	         8.291 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_100_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_100_total_jobs_200000-10 	       1	26676121292 ns/op	         6.603 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_200_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_200_total_jobs_200000-10        	       1	35818803958 ns/op	        12.99 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_200_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_200_total_jobs_200000-10   	       1	35744669750 ns/op	        14.30 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_200_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_200_total_jobs_200000-10 	       1	35955372084 ns/op	        10.29 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_500_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_500_total_jobs_200000-10        	       1	35869996041 ns/op	        14.13 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_500_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_500_total_jobs_200000-10   	       1	65436097084 ns/op	        37.93 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_500_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_500_total_jobs_200000-10 	       1	48872914125 ns/op	        20.38 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_1000_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_none_cardinality_1000_total_jobs_200000-10       	       1	35783291959 ns/op	        15.24 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_1000_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_workspace_cardinality_1000_total_jobs_200000-10  	       1	85305588583 ns/op	        62.92 overall_duration_sec
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_1000_total_jobs_200000
// BenchmarkBatchrouterIsolationModes/isolation_mode_destination_cardinality_1000_total_jobs_200000-10         1	57300762042 ns/op	        37.00 overall_duration_sec
func BenchmarkBatchrouterIsolationModes(b *testing.B) {
	benchAllModes := func(b *testing.B, cardinality, totalJobs int) {
		bench := func(mode isolation.Mode, cardinality, workspacesCount, eventsPerworkspace int) {
			title := fmt.Sprintf("isolation mode %s cardinality %d total jobs %d", mode, cardinality, totalJobs)
			b.Run(title, func(b *testing.B) {
				stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "start"}).Increment()
				defer stats.Default.NewTaggedStat("benchmark", stats.CountType, stats.Tags{"title": title, "action": "end"}).Increment()
				spec := NewBatchrouterIsolationScenarioSpec(mode, workspacesCount, eventsPerworkspace)
				overallDuration := BatchrouterIsolationScenario(b, spec)
				b.ReportMetric(overallDuration.Seconds(), "overall_duration_sec")
			})
		}
		workspacesCount := cardinality
		bench(isolation.ModeNone, cardinality, workspacesCount, totalJobs/workspacesCount)
		bench(isolation.ModeWorkspace, cardinality, workspacesCount, totalJobs/workspacesCount)
		workspacesCount = cardinality / 2
		bench(isolation.ModeDestination, cardinality, workspacesCount, totalJobs/workspacesCount)
	}

	benchAllModes(b, 10, 200000)
	benchAllModes(b, 50, 200000)
	benchAllModes(b, 100, 200000)
	benchAllModes(b, 200, 200000)
	benchAllModes(b, 500, 200000)
	benchAllModes(b, 1000, 200000)
}

// NewBatchrouterIsolationScenarioSpec is a specification for a batchrouter isolation scenario.
// - isolationMode is the isolation mode to use.
// - workspaces is the number of workspaces to use.
// - eventsPerWorkspace is the number of events to send per workspace.
//
// The generated spec's jobs will be split in two destinations for every workspace, one for postgres and another one for minio.
// Thus, the number of destinations will be equal to workspaces * 2.
func NewBatchrouterIsolationScenarioSpec(isolationMode isolation.Mode, workspaces, eventsPerWorkspace int) *BrtIsolationScenarioSpec {
	var s BrtIsolationScenarioSpec
	s.jobQueryBatchSize = 10000
	s.isolationMode = isolationMode
	s.jobs = make([]*brtIsolationJobSpec, workspaces*eventsPerWorkspace)

	var idx int
	for u := 0; u < workspaces; u++ {
		workspaceID := "workspace-" + strconv.Itoa(u)
		s.workspaces = append(s.workspaces, workspaceID)
		for i := 0; i < eventsPerWorkspace; i = i + 2 { // send 50% of events to postgres and 50% to minio
			s.jobs[idx] = &brtIsolationJobSpec{
				id:          int64(idx + 1),
				workspaceID: workspaceID,
				userID:      strconv.Itoa(idx + 1),
				destType:    "POSTGRES",
			}
			idx++
			s.jobs[idx] = &brtIsolationJobSpec{
				id:          int64(idx + 1),
				workspaceID: workspaceID,
				userID:      strconv.Itoa(idx + 2),
				destType:    "MINIO",
			}
			idx++
		}
	}
	return &s
}

// BatchrouterIsolationScenario runs a scenario with the given spec which:
// 1. Sends all events to batch_rt tables
// 2. Starts the server
// 3. Waits for the events to be processed by batch router(s)
// 4. Verifies that all events have been processed
// 5. Optional if [spec.verifyDestinations == true]: Verifies that the correct number of events have been delivered to the appropriate object storage locations
// 6. Returns the total processing duration (last event time - first event time).
func BatchrouterIsolationScenario(t testing.TB, spec *BrtIsolationScenarioSpec) (overallDuration time.Duration) {
	var m brtIsolationMethods

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
		"workspaces":           spec.workspaces,
		"minioEndpoint":        minioDestination.Endpoint,
		"minioBucket":          minioDestination.BucketName,
		"minioAccessKeyID":     minioDestination.AccessKeyID,
		"minioSecretAccessKey": minioDestination.AccessKeySecret,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/brtIsolationTestTemplate.json.tpl", templateCtx)
	mockCBE := m.newMockConfigBackend(t, configJsonPath)
	config.Set("CONFIG_BACKEND_URL", mockCBE.URL)
	defer mockCBE.Close()

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

	t.Logf("Waiting for all batch_rt jobs to be successfully processed")
	require.Eventually(t, func() bool {
		var processedJobCount int
		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',20) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
		return processedJobCount == len(spec.jobs)
	}, 5*time.Minute, 1*time.Second, "all batch_rt jobs should be successfully processed")

	if spec.verifyDestinations {
		t.Logf("Verifying the destinations")
		verify := func(prefix, workspaceID, destType string, count int) {
			fm, err := filemanager.New(&filemanager.Settings{
				Provider: "MINIO",
				Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
					Provider: "MINIO",
					Config: map[string]interface{}{
						"bucketName":      minioDestination.BucketName,
						"prefix":          "",
						"endPoint":        minioDestination.Endpoint,
						"accessKeyID":     minioDestination.AccessKeyID,
						"secretAccessKey": minioDestination.AccessKeySecret,
					},
				}),
			})
			require.NoError(t, err, "it should be able to create a file manager")
			fileObjects, err := fm.ListFilesWithPrefix(context.Background(), "", prefix+"/"+workspaceID+"/", int64(count)).Next()
			require.NoError(t, err, "it should be able to list files")
			var eventCount int
			for _, fileObject := range fileObjects {
				file, err := os.CreateTemp(t.TempDir(), "")
				require.NoError(t, err, "it should be able to create temp file")
				require.NoError(t, file.Close())
				require.NoError(t, fm.Download(context.Background(), file, fileObject.Key))
				file, err = os.Open(file.Name())
				defer func() { _ = file.Close() }()
				require.NoError(t, err, "it should be able to open the file")
				reader, err := gzip.NewReader(file)
				defer func() { _ = reader.Close() }()
				require.NoError(t, err, "it should be able to create gzip reader")
				sc := bufio.NewScanner(reader)
				maxCapacity := 10240 * 1024
				buf := make([]byte, maxCapacity)
				sc.Buffer(buf, maxCapacity)
				for sc.Scan() {
					eventCount++
					lineBytes := sc.Bytes()
					res := gjson.GetManyBytes(lineBytes, "destType", "data.destType", "workspaceID", "data.workspaceID")
					actualDestType := res[0].String()
					if actualDestType == "" {
						actualDestType = res[1].String()
					}
					actualWorkspaceID := res[2].String()
					if actualWorkspaceID == "" {
						actualWorkspaceID = res[3].String()
					}
					require.Equalf(t, workspaceID, actualWorkspaceID, "workspaceID should match for destType %s", destType)
					require.Equalf(t, destType, actualDestType, "destType should match for workspace %s", workspaceID)
				}
			}
			require.Equalf(t, count, eventCount, "it should have the correct total number of events for workspace %s and destination type %s", workspaceID, destType)
		}
		for _, workspace := range spec.workspaces {
			workspaceJobs := lo.CountBy(spec.jobs, func(job *brtIsolationJobSpec) bool {
				return job.workspaceID == workspace
			})
			verify("rudder-logs", workspace, "MINIO", workspaceJobs/2)
			verify("rudder-warehouse-staging-logs", workspace, "POSTGRES", workspaceJobs/2)
		}

		t.Logf("Destinations verified")
	}

	var minExecTime, maxExecTime time.Time
	require.NoError(
		t,
		postgresContainer.DB.QueryRowContext(
			ctx,
			"SELECT min(exec_time), max(exec_time) FROM unionjobsdbmetadata('batch_rt',20)",
		).Scan(&minExecTime, &maxExecTime),
		"it should be able to query the min and max execution times",
	)
	overallDuration = maxExecTime.Sub(minExecTime)

	cancel()
	<-svcDone
	return
}

type BrtIsolationScenarioSpec struct {
	isolationMode      isolation.Mode
	workspaces         []string
	jobs               []*brtIsolationJobSpec
	jobQueryBatchSize  int
	verifyDestinations bool
}

type brtIsolationJobSpec struct {
	id          int64
	workspaceID string
	userID      string
	destType    string
}

func (jobSpec *brtIsolationJobSpec) payload() []byte {
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
type brtIsolationMethods struct{}

// newMockConfigBackend creates a mock config backend server serving the config file at the given path
func (brtIsolationMethods) newMockConfigBackend(t testing.TB, path string) *httptest.Server {
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
func (brtIsolationMethods) newMockWarehouse() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

// seedBrtDB seeds the batch router database with jobs based on the provided spec
func (m brtIsolationMethods) seedBrtDB(t testing.TB, spec *BrtIsolationScenarioSpec) {
	brtJobsDB := jobsdb.NewForWrite("batch_rt", jobsdb.WithStats(stats.NOP))
	require.NoError(t, brtJobsDB.Start(), "it should be able to start the jobsdb")
	defer brtJobsDB.Stop()
	for _, batch := range m.generateJobs(spec.jobs, 100) {
		require.NoError(t, brtJobsDB.Store(context.Background(), batch), "it should be able to store the batch of jobs in the jobsdb")
	}
}

// generateJobs creates batches of jobs from the same workspace, shuffled so that
// batches for the same workspace are not consecutive.
func (brtIsolationMethods) generateJobs(jobs []*brtIsolationJobSpec, batchSize int) [][]*jobsdb.JobT {
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
