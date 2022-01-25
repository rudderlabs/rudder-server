package main_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/stretchr/testify/require"
)

var (
	testData               []test
	mu                     sync.Mutex
	testDataInitialized    = make(chan string)
	manager                kvstoremanager.KVStoreManager
	fieldCountBeforeDelete []int
	fieldCountAfterDelete  []int
	redisInputTestData     []struct {
		key    string
		fields map[string]interface{}
	}
	uploadOutputs []filemanager.UploadOutput

	redisAddress, minioEndpoint string
	hold                        bool
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

const searchDir = "./testData"
const goldenDir = "./goldenDir"

func TestMain(m *testing.M) {
	initialize.Init()
	os.Exit(run(m))
}

func handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/workerJobs", getJob).Methods("GET")
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}", updateJobStatus).Methods("PATCH")
	srvMux.HandleFunc("/workspaceConfig", getWorkspaceConfig).Methods("GET")

	return srvMux
}

func insertRedisData(address string) {
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

	//inserting test data in Redis
	for _, test := range redisInputTestData {
		err := manager.HMSet(test.key, test.fields)
		if err != nil {
			fmt.Println("error while inserting into redis using HMSET: ", err)
		}
	}

	fieldCountBeforeDelete = make([]int, len(redisInputTestData))
	for i, test := range redisInputTestData {
		result, err := manager.HGetAll(test.key)
		if err != nil {
			fmt.Println("error while getting data from redis using HMGET: ", err)
		}
		fieldCountBeforeDelete[i] = len(result)
	}
}

func insertMinioData() {

	//getting list of files in `testData` directory while will be used to testing filemanager.
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.Match([]byte(path)) {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if len(fileList) == 0 {
		panic("file list empty, no data to test.")
	}
	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   minioConfig,
	})
	if err != nil {
		panic(err)
	}

	//upload all files
	for _, file := range fileList {
		filePtr, err := os.Open(file)
		if err != nil {
			panic(err)
		}
		uploadOutput, err := fm.Upload(filePtr)
		if err != nil {
			panic(err)
		}
		uploadOutputs = append(uploadOutputs, uploadOutput)
		filePtr.Close()
	}
	fmt.Println("test files upload to minio mock bucket successful")
}
func run(m *testing.M) int {

	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	//starting redis server to mock redis-destination
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("redis", "alpine3.14", []string{})
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	redisAddress = fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp"))
	if err := pool.Retry(func() error {
		var err error
		client := redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: "",
			DB:       0,
		})
		if err != nil {
			return err
		}
		return client.Ping().Err()
	}); err != nil {
		log.Panicf("Could not connect to docker: %s", err)
	}
	insertRedisData(redisAddress)

	//starting minio server for batch-destination
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
	if err != nil {
		panic(fmt.Errorf("Could not start resource: %s", err))
	}
	defer func() {
		if err := pool.Purge(minioResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint = fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))
	minioConfig["endPoint"] = minioEndpoint
	//check if minio server is up & running.
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	fmt.Println("minio is up & running properly")

	minioClient, err := minio.New(minioEndpoint, minioAccessKeyId, minioSecretAccessKey, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("minioClient created successfully")

	//creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(minioBucket, minioRegion)
	if err != nil {
		panic(err)
	}
	fmt.Println("bucket created successfully")
	insertMinioData()

	//starting http server to mock regulation-manager
	svr := httptest.NewServer(handler())
	defer svr.Close()
	svcCtx, svcCancel := context.WithCancel(context.Background())
	code := make(chan int, 1)
	go func() {
		os.Setenv("CONFIG_BACKEND_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
		os.Setenv("CONFIG_BACKEND_URL", svr.URL)
		os.Setenv("DEST_TRANSFORM_URL", "http://localhost:9090")
		backendconfig.Init()
		c := m.Run()
		svcCancel()
		code <- c

	}()
	<-testDataInitialized
	_ = os.Setenv("URL_PREFIX", svr.URL)
	main.Run(svcCtx)
	statusCode := <-code

	blockOnHold()
	return statusCode
}

type test struct {
	respBody          string
	getJobRespCode    int
	updateJobRespCode int
	status            model.JobStatus
}

func TestFlow(t *testing.T) {

	t.Run("TestFlow", func(t *testing.T) {
		testData = []test{
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
		testDataInitialized <- "done"
		require.Eventually(t, func() bool {
			for _, test := range testData {
				mu.Lock()
				status := test.status
				mu.Unlock()
				if status == "pending" && test.getJobRespCode == 200 {
					return false
				}
			}
			return true
		}, time.Minute*3, time.Second*2)

		fieldCountAfterDelete = make([]int, len(redisInputTestData))
		for i, test := range redisInputTestData {
			key := fmt.Sprintf("user:%s", test.key)
			result, err := manager.HGetAll(key)
			if err != nil {
				fmt.Println("error while getting data from redis using HMGET: ", err)
			}
			fieldCountAfterDelete[i] = len(result)
		}
		for i := 1; i < len(redisInputTestData); i++ {
			require.Equal(t, fieldCountBeforeDelete[i], fieldCountAfterDelete[i], "expected no deletion for this key")
		}

		require.NotEqual(t, fieldCountBeforeDelete[0], fieldCountAfterDelete[0], "key found, expected no key")

		verifyBatchDelete(t)
	})

}

func getJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	for i, test := range testData {
		status := test.status
		if status == "pending" {
			w.WriteHeader(testData[i].getJobRespCode)
			_, _ = w.Write([]byte(testData[i].respBody))
			return
		}
	}
	//for the time when testData is not initialized.
	w.WriteHeader(404)
}

func updateJobStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID, _ := strconv.Atoi(mux.Vars(r)["job_id"])
	var status statusJobSchema
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	if status.Status == "complete" {
		mu.Lock()
		testData[jobID-1].status = "complete"
		mu.Unlock()
	}
	w.WriteHeader(testData[jobID-1].updateJobRespCode)

	body, err := json.Marshal(struct{}{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(body)
}

func getWorkspaceConfig(w http.ResponseWriter, r *http.Request) {
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

func verifyBatchDelete(t *testing.T) {
	t.Helper()
	var goldenFileList []string
	err := filepath.Walk(goldenDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.Match([]byte(path)) {
			goldenFileList = append(goldenFileList, path)
		}
		return nil
	})
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	if len(goldenFileList) == 0 {
		require.NoError(t, fmt.Errorf("golden file list empty."), "expected no error")
	}

	filePtr, err := os.Open(goldenFileList[0])
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	goldenFile, err := io.ReadAll(filePtr)
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	filePtr.Close()

	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   minioConfig,
	})
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}

	DownloadedFileName := "TmpDownloadedFile"
	filePtr, err = os.OpenFile(DownloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	defer os.Remove(DownloadedFileName)
	key := fm.GetDownloadKeyFromFileLocation(uploadOutputs[0].Location)
	err = fm.Download(filePtr, key)
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	filePtr.Close()

	filePtr, err = os.Open(DownloadedFileName)
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	downloadedFile, err := io.ReadAll(filePtr)
	if err != nil {
		require.NoError(t, err, "batch verification failed")
	}
	filePtr.Close()
	equal := strings.Compare(string(goldenFile), string(downloadedFile))
	if equal != 0 {
		require.NoError(t, fmt.Errorf("downloaded file different than golden file"), "batch verification failed")
	}
}

func blockOnHold() {
	if !hold {
		return
	}

	log.Println("Test on hold, before cleanup")
	log.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	close(c)
}
