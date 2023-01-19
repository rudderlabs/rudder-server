package main_test

import (
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

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/utils/httputil"
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
	uploadOutputs []filemanager.UploadOutput

	redisAddress, minioEndpoint string
	fileList                    []string

	minioAccessKeyId     = "MYACCESSKEY"
	minioSecretAccessKey = "MYSECRETKEY"
	minioRegion          = "us-east-1"
	minioBucket          = "filemanager-test-1"
	regexRequiredSuffix  = regexp.MustCompile(".json.gz$")
	minioConfig          = map[string]interface{}{
		"bucketName":       minioBucket,
		"accessKeyID":      minioAccessKeyId,
		"accessKey":        minioSecretAccessKey,
		"enableSSE":        false,
		"prefix":           "some-prefix",
		"endPoint":         minioEndpoint,
		"s3ForcePathStyle": true,
		"disableSSL":       true,
		"region":           minioRegion,
	}
)

const (
	namespaceID = "spaghetti"
	searchDir   = "./testData"
	goldenDir   = "./goldenDir"
)

func startRedisServer(t *testing.T, pool *dockertest.Pool) {
	resource, err := pool.Run("redis", "alpine3.14", []string{})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Could not purge resource: %s", err)
		}
	})
	redisAddress = fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp"))
	err = pool.Retry(func() error {
		client := redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: "",
			DB:       0,
		})
		return client.Ping().Err()
	})
	require.NoError(t, err)
	t.Log("Redis server is up and running")
}

func startMinioServer(t *testing.T, pool *dockertest.Pool) {
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", minioAccessKeyId),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", minioSecretAccessKey),
			fmt.Sprintf("MINIO_SITE_REGION=%s", minioRegion),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(minioResource); err != nil {
			t.Logf("Could not purge resource: %s", err)
		}
	})

	minioEndpoint = fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))
	minioConfig["endPoint"] = minioEndpoint
	// check if minio server is up & running.
	err = pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() { httputil.CloseResponse(resp) }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	})
	require.NoError(t, err)

	minioClient, err := minio.New(minioEndpoint, minioAccessKeyId, minioSecretAccessKey, false)
	require.NoError(t, err)

	// creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(minioBucket, minioRegion)
	require.NoError(t, err)
}

func TestFlow(t *testing.T) {
	defer blockOnHold(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Preparing data for testing
	singleTenantTestData = []test{
		{
			respBody:          `{"jobId":"1","destinationId":"destId-redis-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
			getJobRespCode:    200,
			updateJobRespCode: 201,
			status:            "pending",
		},
		{
			respBody:          `{"jobId":"2","destinationId":"destId-s3-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
			getJobRespCode:    200,
			updateJobRespCode: 201,
			status:            "pending",
		},
	}
	multiTenantTestData = []test{
		{
			respBody:          `{"jobId":"1","destinationId":"destId-redis-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
			getJobRespCode:    200,
			updateJobRespCode: 201,
			status:            "pending",
		},
		{
			respBody:          `{"jobId":"2","destinationId":"destId-s3-test","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
			getJobRespCode:    200,
			updateJobRespCode: 201,
			status:            "pending",
		},
	}
	// starting http server to mock regulation-manager
	srv := httptest.NewServer(handler(t))
	t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("WORKSPACE_NAMESPACE", "216Co97d9So9TkqphM0cxBzRxc3")

	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("DEST_TRANSFORM_URL", "http://localhost:9090")
	t.Setenv("URL_PREFIX", srv.URL)
	t.Setenv("RSERVER_BACKEND_CONFIG_POLL_INTERVAL", "0.1")
	t.Setenv("REGULATION_WORKER_BATCH_DESTINATIONS_ENABLED", "true")

	// starting redis server to mock redis-destination
	startRedisServer(t, pool)
	insertRedisData(t, redisAddress)
	t.Log("Redis server is up and running")

	// starting minio server for batch-destination
	startMinioServer(t, pool)
	insertMinioData(t)
	t.Log("Minio server is up and running")

	t.Run("Single-tenant: test complete worker flow", func(t *testing.T) {
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
				if test.status == "pending" && test.getJobRespCode == 200 {
					return false
				}
			}
			return true
		}, 3*time.Minute, 150*time.Millisecond)
	})

	t.Run("Multi-tenant: test complete worker flow", func(t *testing.T) {
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
				if test.status == "pending" && test.getJobRespCode == 200 {
					return false
				}
			}
			return true
		}, 3*time.Minute, 150*time.Millisecond)
	})
	verifyRedisDeletion(t)
	verifyBatchDeletion(t)
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
		if test.status == "pending" {
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
		if test.status == "pending" {
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
	jobID, _ := strconv.Atoi(mux.Vars(r)["job_id"])
	var status statusJobSchema
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	testDataMu.Lock()
	if status.Status == "complete" {
		singleTenantTestData[jobID-1].status = "complete"
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
	jobID, _ := strconv.Atoi(mux.Vars(r)["job_id"])
	var status statusJobSchema
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	testDataMu.Lock()
	if status.Status == "complete" {
		multiTenantTestData[jobID-1].status = "complete"
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

func getSingleTenantWorkspaceConfig(w http.ResponseWriter, _ *http.Request) {
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

func getMultiTenantNamespaceConfig(w http.ResponseWriter, _ *http.Request) {
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

func verifyBatchDeletion(t *testing.T) {
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
	goldenFile, err := io.ReadAll(filePtr)
	require.NoError(t, err, "batch verification failed")
	require.NoError(t, filePtr.Close())

	var fmFactory filemanager.FileManagerFactoryT
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   minioConfig,
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
	downloadedFile, err := io.ReadAll(filePtr)
	require.NoError(t, err, "batch verification failed")
	require.NoError(t, filePtr.Close())
	require.Equal(t, string(goldenFile), string(downloadedFile), "downloaded file different than golden file")
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/workerJobs", getSingleTenantJob).Methods(http.MethodGet)
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}", updateSingleTenantJobStatus).Methods(http.MethodPatch)
	srvMux.HandleFunc("/dataplane/namespaces/{workspace_id}/regulations/workerJobs", getMultiTenantJob).Methods(http.MethodGet)
	srvMux.HandleFunc("/dataplane/namespaces/{workspace_id}/regulations/workerJobs/{job_id}", updateMultiTenantJobStatus).Methods(http.MethodPatch)
	srvMux.HandleFunc("/workspaceConfig", getSingleTenantWorkspaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig).Methods(http.MethodGet)

	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})

	return srvMux
}

func insertRedisData(t *testing.T, address string) {
	t.Helper()

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

func insertMinioData(t *testing.T) {
	t.Helper()

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

	var fmFactory filemanager.FileManagerFactoryT
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   minioConfig,
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
}
