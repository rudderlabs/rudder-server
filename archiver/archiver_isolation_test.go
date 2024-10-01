package archiver_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestArchiverIsolation(t *testing.T) {
	ArchivalScenario(t, 20, 15, 1000, true)
}

func BenchmarkArchiverIsolation(b *testing.B) {
	benchFunc := func(b *testing.B, numWorkspace, numSourcesPerWorkspace, numJobsPerSource int) {
		title := fmt.Sprintf(
			"numWorkspaces: %d - numSourcePerWorkspace: %d - numJobsPerSource: %d - totalJobs: %d",
			numWorkspace,
			numSourcesPerWorkspace,
			numJobsPerSource,
			numWorkspace*numSourcesPerWorkspace*numJobsPerSource,
		)
		b.Run(title, func(b *testing.B) {
			start := time.Now()
			ArchivalScenario(b, numWorkspace, numSourcesPerWorkspace, numJobsPerSource, false)
			b.ReportMetric(time.Since(start).Seconds(), "overall_duration_seconds")
		})
	}

	benchFunc(b, 25, 4, 1000)
	benchFunc(b, 100, 5, 1000)
}

func dummyConfig(
	numWorkspace,
	numSourcesPerWorkspace int,
	minio *minio.Resource,
) map[string]backendconfig.ConfigT {
	configMap := map[string]backendconfig.ConfigT{}
	for i := 0; i < numWorkspace; i++ {
		workspaceID := trand.String(10)
		wConfig := backendconfig.ConfigT{
			EnableMetrics: false,
			WorkspaceID:   workspaceID,
			UpdatedAt:     time.Now(),
			Settings: backendconfig.Settings{
				DataRetention: backendconfig.DataRetention{
					UseSelfStorage: true,
					StorageBucket: backendconfig.StorageBucket{
						Type: "MINIO",
						Config: map[string]interface{}{
							"bucketName":      minio.BucketName,
							"prefix":          "rudder-archives",
							"endPoint":        minio.Endpoint,
							"accessKeyID":     minio.AccessKeyID,
							"secretAccessKey": minio.AccessKeySecret,
						},
					},
					StoragePreferences: backendconfig.StoragePreferences{
						GatewayDumps: true,
					},
				},
			},
		}
		for j := 0; j < numSourcesPerWorkspace; j++ { // rand.Intn(n) = [0, n)
			wConfig.Sources = append(wConfig.Sources, backendconfig.SourceT{
				ID:   trand.String(10),
				Name: trand.String(10),
				SourceDefinition: backendconfig.SourceDefinitionT{
					ID:       "someSourceDefinitionID",
					Category: "someCategory",
				},
				WriteKey:    trand.String(10),
				Transient:   false,
				Enabled:     true,
				WorkspaceID: workspaceID,
			})
		}
		configMap[workspaceID] = wConfig
	}
	return configMap
}

// 1. inserts jobs into gw_jobsdb for numWorkspaces and [1, maxSourcesPerWorkspace] sources per workspace
// 2. run rudder-server with a config for above workspaces
// 3. checks for archives in minio for each source
func ArchivalScenario(
	t testing.TB,
	numWorkspace,
	numSourcesPerWorkspace,
	numJobsPerSource int,
	verifyArchives bool,
) {
	ctx, cancel := context.WithCancel(context.Background())

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")

	postgresContainer, err := postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "failed to setup postgres container")

	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err, "failed to setup minio container")
	transformerContainer, err := transformertest.Setup(pool, t)
	require.NoError(t, err, "failed to setup transformer container")

	configMap := dummyConfig(numWorkspace, numSourcesPerWorkspace, minioResource)

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
	config.Set("DEST_TRANSFORM_URL", transformerContainer.TransformerURL)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("JobsDB.enableWriterQueue", false)
	config.Set("RUDDER_TMPDIR", os.TempDir())
	config.Set("archival.ArchiveSleepDuration", "1s")
	config.Set("archival.MinWorkerSleep", "100ms")
	config.Set("archival.UploadFrequency", "1s")
	config.Set("Processor.isolationMode", "none")

	configServer := configBackendServer(t, configMap)
	defer configServer.Close()
	config.Set("CONFIG_BACKEND_URL", configServer.URL)

	jobMap, numJobs := insertJobs(t, configMap, numJobsPerSource)

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

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		t.Logf("Waiting for all gw jobs to be successfully processed")
		require.Eventually(t, func() bool {
			var processedJobCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',20) WHERE job_state = 'succeeded'").Scan(&processedJobCount))
			return processedJobCount == numJobs
		}, 5*time.Minute, 1*time.Second, "all gw jobs should be successfully processed")
		return nil
	})

	g.Go(func() error {
		require.Eventually(t, func() bool {
			var archivedJobCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('arc',20) WHERE job_state = 'succeeded'").Scan(&archivedJobCount))
			return archivedJobCount == numJobs
		}, 5*time.Minute, 1*time.Second, "all jobs should be successfully archived")
		return nil
	})

	require.NoError(t, g.Wait(), "all jobs should've been successfully processed and then archived")

	t.Log("All jobs successfully processed and archived")
	t.Log("verifying archives...")

	verify := func(sourceID string) {
		fm, err := filemanager.New(&filemanager.Settings{
			Provider: "MINIO",
			Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
				Provider: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource.BucketName,
					"prefix":          "",
					"endPoint":        minioResource.Endpoint,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
				},
			}),
		})
		require.NoError(t, err, "it should be able to create a file manager")
		fileObjects, err := fm.ListFilesWithPrefix(
			context.Background(),
			"",
			"rudder-archives"+"/"+sourceID+"/"+"gw"+"/",
			int64(numJobs),
		).Next()
		require.NoError(t, err, "it should be able to list files")
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
			eventCount := 0
			for sc.Scan() {
				lineBytes := sc.Bytes()
				messageID := gjson.GetBytes(lineBytes, "messageId").String()
				originalMessageID := gjson.GetBytes(jobMap[sourceID][eventCount].EventPayload, "batch.0.messageId").String()
				require.Equal(t, originalMessageID, messageID, "it should have the same message ID")
				eventCount++
			}
			require.Equal(
				t, len(jobMap[sourceID]), eventCount,
				sourceID,
				"it should have the same number of events as the number of jobs")
		}
	}

	if verifyArchives {
		for _, wsConfig := range configMap {
			for _, source := range wsConfig.Sources {
				verify(source.ID)
			}
		}
	}

	cancel()
	<-svcDone
}

func configBackendServer(
	t testing.TB,
	configMap map[string]backendconfig.ConfigT,
) *httptest.Server {
	data, err := json.Marshal(configMap)
	require.NoError(t, err, "failed to marshal config map")
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

func insertJobs(
	t testing.TB,
	configMap map[string]backendconfig.ConfigT,
	numJobsPerSource int,
) (map[string][]*jobsdb.JobT, int) {
	gwJobsDB := jobsdb.NewForWrite("gw", jobsdb.WithStats(stats.NOP))
	require.NoError(t, gwJobsDB.Start(), "it should be able to start the jobsdb")
	defer gwJobsDB.Stop()

	payload := map[string]interface{}{
		"batch": []map[string]interface{}{
			{
				"a": 123,
				"b": "abc",
			},
		},
	}

	jobs := make(map[string][]*jobsdb.JobT)
	jobCount := 0
	for ws, config := range configMap {
		for _, source := range config.Sources {
			sourceID := source.ID
			writeKey := source.WriteKey
			payload["writeKey"] = writeKey
			receivedAt := time.Now()
			payload["receivedAt"] = receivedAt
			params := map[string]interface{}{
				"source_id":  sourceID,
				"receivedAt": receivedAt,
			}
			parameters, err := json.Marshal(params)
			require.NoError(t, err, "should be able to marshal the params")
			jobs[sourceID] = []*jobsdb.JobT{}
			for j := 0; j < numJobsPerSource; j++ {
				payload["messageId"] = trand.String(10)
				eventPayload, err := json.Marshal(payload)
				require.NoError(t, err, "should be able to marshal the event payload")
				job := &jobsdb.JobT{
					UUID:         uuid.New(),
					UserID:       trand.String(10),
					CreatedAt:    time.Now(),
					ExpireAt:     time.Now(),
					EventCount:   1,
					Parameters:   parameters,
					EventPayload: eventPayload,
					WorkspaceId:  ws,
					CustomVal:    "GW",
				}
				jobs[sourceID] = append(jobs[sourceID], job)
				jobCount++
			}
		}
	}
	for _, j := range jobs {
		require.NoError(t, gwJobsDB.Store(context.Background(), j), "should be able to insert the jobs")
	}
	return jobs, jobCount
}
