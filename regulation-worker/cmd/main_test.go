package main_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/redis"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
)

var (
	singleTenantTestData   []test
	multiTenantTestData    []test
	testDataMu             sync.Mutex
	manager                kvstoremanager.KVStoreManager
	fieldCountBeforeDelete []int
	fieldCountAfterDelete  []int
	redisInputTestData     []struct {
		key    string
		fields map[string]interface{}
	}
	uploadOutputs []filemanager.UploadedFile

	fileList []string

	regexRequiredSuffix = regexp.MustCompile(".json.gz$")
)

const (
	namespaceID = "spaghetti"
	searchDir   = "./testData"
	goldenDir   = "./goldenDir"
)

func TestRegulationWorkerFlow(t *testing.T) {
	defer blockOnHold(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// starting redis server to mock redis-destination
	redisResource, err := redis.Setup(context.Background(), pool, t)
	require.NoError(t, err)
	insertRedisData(t, redisResource.Addr)
	t.Log("Redis server is up and running")

	// starting minio server for batch-destination
	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)
	minioConfig := map[string]interface{}{
		"bucketName":       minioResource.BucketName,
		"accessKeyID":      minioResource.AccessKeyID,
		"accessKey":        minioResource.AccessKeySecret,
		"enableSSE":        false,
		"prefix":           "some-prefix",
		"endPoint":         minioResource.Endpoint,
		"s3ForcePathStyle": true,
		"disableSSL":       true,
		"region":           minioResource.Region,
	}
	insertMinioData(t, minioConfig)

	// starting http server to mock regulation-manager
	srv := httptest.NewServer(handler(t, minioConfig, redisResource.Addr))
	t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("WORKSPACE_NAMESPACE", "216Co97d9So9TkqphM0cxBzRxc3")

	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("DEST_TRANSFORM_URL", srv.URL)
	t.Setenv("URL_PREFIX", srv.URL)
	t.Setenv("RSERVER_BACKEND_CONFIG_POLL_INTERVAL", "0.1")
	t.Setenv("REGULATION_WORKER_BATCH_DESTINATIONS_ENABLED", "true")

	t.Run("single tenant", func(t *testing.T) {
		singleTenantTestData = []test{
			{
				respBody:          `{"jobId":"1","destinationId":"destId-redis-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            model.JobStatus{Status: model.JobStatusPending},
			},
			{
				respBody:          `{"jobId":"2","destinationId":"destId-s3-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            model.JobStatus{Status: model.JobStatusPending},
			},
		}

		done := make(chan struct{})
		svcCtx, svcCancel := context.WithCancel(context.Background())
		t.Cleanup(func() { svcCancel(); <-done })
		go func() {
			defer close(done)
			defer svcCancel()
			err := main.Run(svcCtx)
			require.NoError(t, err, "error while running regulation-worker")
		}()
		require.Eventually(t, func() bool {
			testDataMu.Lock()
			defer testDataMu.Unlock()
			for _, test := range singleTenantTestData {
				if test.status.Status == model.JobStatusPending && test.getJobRespCode == 200 {
					return false
				}
			}
			return true
		}, 2*time.Minute, 100*time.Millisecond)
	})

	t.Run("multi tenant", func(t *testing.T) {
		multiTenantTestData = []test{
			{
				respBody:          `{"jobId":"1","destinationId":"destId-redis-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            model.JobStatus{Status: model.JobStatusPending},
			},
			{
				respBody:          `{"jobId":"2","destinationId":"destId-s3-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            model.JobStatus{Status: model.JobStatusPending},
			},
		}

		t.Setenv("DEPLOYMENT_TYPE", "MULTITENANT")
		t.Setenv("WORKSPACE_NAMESPACE", namespaceID)
		t.Setenv("HOSTED_SERVICE_SECRET", "foobar")

		done := make(chan struct{})
		svcCtx, svcCancel := context.WithCancel(context.Background())
		t.Cleanup(func() { svcCancel(); <-done })
		go func() {
			defer close(done)
			defer svcCancel()
			err := main.Run(svcCtx)
			require.NoError(t, err, "error while running regulation-worker")
		}()
		require.Eventually(t, func() bool {
			testDataMu.Lock()
			defer testDataMu.Unlock()
			for _, test := range multiTenantTestData {
				if test.status.Status == model.JobStatusPending && test.getJobRespCode == 200 {
					return false
				}
			}
			return true
		}, 2*time.Minute, 100*time.Millisecond)
	})
	verifyRedisDeletion(t)
	verifyBatchDeletion(t, minioConfig)
}

func verifyRedisDeletion(t *testing.T) {
	fieldCountAfterDelete = make([]int, len(redisInputTestData))
	for i, test := range redisInputTestData {
		key := fmt.Sprintf("user:%s", test.key)
		result, err := manager.HGetAll(key)
		if err != nil {
			t.Logf("Error while getting data from redis using HMGET: %v", err)
		}
		fieldCountAfterDelete[i] = len(result)
	}
	for i := 1; i < len(redisInputTestData); i++ {
		require.Equal(t, fieldCountBeforeDelete[i], fieldCountAfterDelete[i], "expected no deletion for this key")
	}
	require.NotEqual(t, fieldCountBeforeDelete[0], fieldCountAfterDelete[0], "key found, expected no key")
}

func getSingleTenantJob(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	testDataMu.Lock()
	defer testDataMu.Unlock()
	for i, test := range singleTenantTestData {
		if test.status.Status == model.JobStatusPending {
			w.WriteHeader(singleTenantTestData[i].getJobRespCode)
			_, _ = w.Write([]byte(singleTenantTestData[i].respBody))
			return
		}
	}
	// for the time when testData is not initialized.
	w.WriteHeader(204)
}

func getMultiTenantJob(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	testDataMu.Lock()
	defer testDataMu.Unlock()
	for i, test := range multiTenantTestData {
		if test.status.Status == model.JobStatusPending {
			w.WriteHeader(multiTenantTestData[i].getJobRespCode)
			_, _ = w.Write([]byte(multiTenantTestData[i].respBody))
			return
		}
	}
	// for the time when testData is not initialized.
	w.WriteHeader(204)
}

func updateSingleTenantJobStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID, _ := strconv.Atoi(chi.URLParam(r, "job_id"))
	var status statusJobSchema
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	testDataMu.Lock()
	if status.Status == string(model.JobStatusComplete) {
		singleTenantTestData[jobID-1].status.Status = model.JobStatusComplete
	}
	updateJobRespCode := singleTenantTestData[jobID-1].updateJobRespCode
	testDataMu.Unlock()
	w.WriteHeader(updateJobRespCode)

	body, err := json.Marshal(struct{}{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(body)
}

func updateMultiTenantJobStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID, _ := strconv.Atoi(chi.URLParam(r, "job_id"))
	var status statusJobSchema
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	testDataMu.Lock()
	if status.Status == string(model.JobStatusComplete) {
		multiTenantTestData[jobID-1].status.Status = model.JobStatusComplete
	}
	updateJobRespCode := multiTenantTestData[jobID-1].updateJobRespCode
	testDataMu.Unlock()
	w.WriteHeader(updateJobRespCode)

	body, err := json.Marshal(struct{}{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(body)
}

func getSingleTenantWorkspaceConfig(minioConfig map[string]interface{}, redisAddress string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		config := backendconfig.ConfigT{
			WorkspaceID: "reg-test-workspaceId",
			Sources: []backendconfig.SourceT{
				{
					Destinations: []backendconfig.DestinationT{
						{
							ID:     "destId-s3-test",
							Config: minioConfig,
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: "S3",
							},
						},
						{
							ID: "destId-redis-test",
							Config: map[string]interface{}{
								"address":     redisAddress,
								"clusterMode": false,
								"secure":      false,
							},
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: "REDIS",
							},
						},
					},
				},
			},
		}
		body, err := json.Marshal(config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = w.Write(body)
	}
}

func getMultiTenantNamespaceConfig(minioConfig map[string]interface{}, redisAddress string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		config := map[string]backendconfig.ConfigT{namespaceID: {
			WorkspaceID: "reg-test-workspaceId",
			Sources: []backendconfig.SourceT{
				{
					Destinations: []backendconfig.DestinationT{
						{
							ID:     "destId-s3-test",
							Config: minioConfig,
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: "S3",
							},
						},
						{
							ID: "destId-redis-test",
							Config: map[string]interface{}{
								"address":     redisAddress,
								"clusterMode": false,
								"secure":      false,
							},
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: "REDIS",
							},
						},
					},
				},
			},
		}}

		body, err := json.Marshal(config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = w.Write(body)
	}
}

func verifyBatchDeletion(t *testing.T, minioConfig map[string]interface{}) {
	t.Helper()
	var goldenFileList []string
	err := filepath.Walk(goldenDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.MatchString(path) {
			goldenFileList = append(goldenFileList, path)
		}
		return nil
	})
	require.NoError(t, err, "batch verification failed")
	if len(goldenFileList) == 0 {
		t.Fatalf("Expected no error but found golden file list empty")
	}

	filePtr, err := os.Open(goldenFileList[0])
	require.NoError(t, err, "batch verification failed")
	gzipReader, err := gzip.NewReader(filePtr)
	require.NoError(t, err, "batch verification failed")
	defer func() { _ = gzipReader.Close() }()
	goldenFile, err := io.ReadAll(gzipReader)
	require.NoError(t, err, "batch verification failed")

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: "S3",
		Config:   minioConfig,
		Logger:   logger.NOP,
	})
	require.NoError(t, err, "batch verification failed")

	DownloadedFileName := "TmpDownloadedFile"
	filePtr, err = os.OpenFile(DownloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err, "batch verification failed")
	defer func() { _ = os.Remove(DownloadedFileName) }()
	key := fm.GetDownloadKeyFromFileLocation(uploadOutputs[0].Location)
	err = fm.Download(context.TODO(), filePtr, key)
	require.NoError(t, err, "batch verification failed")
	require.NoError(t, filePtr.Close())

	filePtr, err = os.Open(DownloadedFileName)
	require.NoError(t, err, "batch verification failed")
	defer func() { _ = filePtr.Close() }()
	gzipReader, err = gzip.NewReader(filePtr)
	require.NoError(t, err, "batch verification failed")
	downloadedFile, err := io.ReadAll(gzipReader)
	require.NoError(t, err, "batch verification failed")
	require.Equal(t, string(goldenFile), string(downloadedFile), "downloaded file different than golden file")
}

func handler(t *testing.T, minioConfig map[string]interface{}, redisAddress string) http.Handler {
	t.Helper()
	srvMux := chi.NewRouter()

	srvMux.Get("/dataplane/workspaces/{workspace_id}/regulations/workerJobs", getSingleTenantJob)
	srvMux.Patch("/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}", updateSingleTenantJobStatus)
	srvMux.Get("/dataplane/namespaces/{workspace_id}/regulations/workerJobs", getMultiTenantJob)
	srvMux.Patch("/dataplane/namespaces/{workspace_id}/regulations/workerJobs/{job_id}", updateMultiTenantJobStatus)
	srvMux.Get("/workspaceConfig", getSingleTenantWorkspaceConfig(minioConfig, redisAddress))
	srvMux.Get("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig(minioConfig, redisAddress))
	srvMux.Get("/features", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"regulations": ["BRAZE", "AM", "INTERCOM", "CLEVERTAP", "AF", "MP", "GA", "ITERABLE", "ENGAGE", "CUSTIFY", "SENDGRID", "SPRIG"],
			"supportSourceTransformV1": true
		}`))
	})
	return srvMux
}

func insertRedisData(t *testing.T, address string) {
	redisInputTestData = []struct {
		key    string
		fields map[string]interface{}
	}{
		{
			key: "Jermaine1473336609491897794707338",
			fields: map[string]interface{}{
				"Phone": "6463633841",
				"Email": "dorowane8n285680461479465450293436@gmail.com",
			},
		},
	}

	destName := "REDIS"
	destConfig := map[string]interface{}{
		"clusterMode": false,
		"address":     address,
	}
	manager = kvstoremanager.New(destName, destConfig)

	// inserting test data in Redis
	for _, test := range redisInputTestData {
		err := manager.HMSet(test.key, test.fields)
		if err != nil {
			t.Logf("Error while inserting into redis using HMSET: %v", err)
		}
	}

	fieldCountBeforeDelete = make([]int, len(redisInputTestData))
	for i, test := range redisInputTestData {
		result, err := manager.HGetAll(test.key)
		if err != nil {
			t.Logf("Error while getting data from redis using HMGET: %v", err)
		}
		fieldCountBeforeDelete[i] = len(result)
	}
}

func insertMinioData(t *testing.T, minioConfig map[string]interface{}) {
	// getting list of files in `testData` directory which will be used to test filemanager.
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.MatchString(path) {
			fileList = append(fileList, path)
		}
		return nil
	})
	require.NoError(t, err)
	if len(fileList) == 0 {
		t.Fatal("File list empty, no data to test")
	}

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: "S3",
		Config:   minioConfig,
		Logger:   logger.NOP,
	})
	require.NoError(t, err)

	// upload all files
	for _, file := range fileList {
		filePtr, err := os.Open(file)
		require.NoError(t, err)
		uploadOutput, err := fm.Upload(context.TODO(), filePtr)
		require.NoError(t, err)
		uploadOutputs = append(uploadOutputs, uploadOutput)
		require.NoError(t, filePtr.Close())
	}

	t.Log("Test files upload to minio mock bucket successful")
}

func blockOnHold(t *testing.T) {
	t.Helper()
	if os.Getenv("HOLD") != "1" {
		return
	}

	t.Log("Test on hold, before cleanup")
	t.Log("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

type test struct {
	respBody          string
	getJobRespCode    int
	updateJobRespCode int
	status            model.JobStatus
}

type statusJobSchema struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}
