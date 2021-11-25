package main_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var (
	c        = make(chan string)
	testData []test
	mu       sync.Mutex
)

func TestMain(m *testing.M) {

	os.Exit(run(m))
}

func handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/worker/workspaces/{workspace_id}/regulations/worker-job", getJob).Methods("GET")
	srvMux.HandleFunc("/worker/workspaces/{workspace_id}/regulations/worker-job/{job_id}", updateJobStatus).Methods("PATCH")

	return srvMux
}

func run(m *testing.M) int {
	svr := httptest.NewServer(handler())
	defer svr.Close()
	workspaceID := "216Co97d9So9TkqphM0cxBzRxc3"
	svcCtx, svcCancel := context.WithCancel(context.Background())

	go func() {
		_ = os.Setenv("workspaceID", workspaceID)
		_ = os.Setenv("urlPrefix", svr.URL)
		main.Run(svcCtx)
		c <- "done"
	}()
	os.Setenv("CONFIG_BACKEND_URL", "https://api.dev.rudderlabs.com")
	os.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	os.Setenv("CONFIG_PATH", "./test_config.yaml")
	config.Load()
	logger.Init()
	backendconfig.Init()
	code := m.Run()
	svcCancel()
	<-c
	return code
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
				respBody:          `{"jobId":"1","destinationId":"216GUF0fW9z6JfRhW3pvGBEQpyQ","userAttributes":[{"userId":"Jermaine1473336609491897794707338","phone":"6463633841","email":"dorowane8n285680461479465450293436@gmail.com"},{"userId":"Mercie8221821544021583104106123","email":"dshirilad8536019424659691213279980@gmail.com"},{"userId":"Claiborn443446989226249191822329","phone":"8782905113"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
		}

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
	var status client.StatusJobSchema
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
