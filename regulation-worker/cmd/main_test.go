package main_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var (
	c        = make(chan os.Signal, 1)
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
	workspaceID := "1zzAn8ZshcdkLN5TvP86VqLMT90"
	svcCtx, svcCancel := context.WithCancel(context.Background())

	go func() {
		_ = os.Setenv("workspaceID", workspaceID)
		_ = os.Setenv("urlPrefix", svr.URL)
		main.Run(svcCtx)
		<-c
		svcCancel()
	}()
	os.Setenv("CONFIG_BACKEND_URL", "https://api.dev.rudderlabs.com")
	os.Setenv("WORKSPACE_TOKEN", "1zzAnCvbknUuGogjQIBhkST0O4K")
	os.Setenv("CONFIG_PATH", "./test_config.yaml")
	config.Load()
	logger.Init()
	backendconfig.Init()
	code := m.Run()

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
				respBody:          `{"jobId":"1","destinationId":"1zzK2ZRgKofS6nxfcJ2nthi0Cme","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
			{
				respBody:          `{"jobId":"2","destinationId":"1zzK2ZRgKofS6nxfcJ2nthi0Cme","userAttributes":[{"userId":"3","phone":"555-555-666"},{"userId":"4","email":"johnny@example.com"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
			{
				respBody:          `{"jobId":"3","destinationId":"1zzK2ZRgKofS6nxfcJ2nthi0Cme","userAttributes":[{"userId":"3","phone":"555-555-666"},{"userId":"4","email":"johnny@example.com"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
			{
				getJobRespCode: 404,
				status:         "pending",
			},
		}

		require.Eventually(t, func() bool {
			for _, test := range testData {
				mu.Lock()
				status := test.status
				mu.Unlock()
				if status == "pending " && test.getJobRespCode == 200 {
					return false
				}
			}
			return true

		}, time.Minute, time.Second*2)
		c <- os.Interrupt
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
	var status model.JobStatus
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		return
	}

	if status == "complete" {
		mu.Lock()
		testData[jobID-1].status = "complete"
		mu.Unlock()
		fmt.Println("testData[jobID-1].Status=", "complete", "jobID-1=", jobID-1)
	}
	w.WriteHeader(testData[jobID-1].updateJobRespCode)

	body, err := json.Marshal(struct{}{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(body)
}
