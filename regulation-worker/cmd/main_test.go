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
	main "github.com/rudderlabs/rudder-server/regulation-worker/cmd"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
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
	workspaceID := "1001"
	svcCtx, svcCancel := context.WithCancel(context.Background())

	go func() {
		_ = os.Setenv("workspaceID", workspaceID)
		_ = os.Setenv("urlPrefix", svr.URL)
		main.Run(svcCtx)
		<-c
		svcCancel()
	}()

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
				respBody:          `{"jobId":"1","destinationId":"23","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
			{
				respBody:          `{"jobId":"2","destinationId":"123","userAttributes":[{"userId":"3","phone":"555-555-666"},{"userId":"4","email":"johnny@example.com"}]}`,
				getJobRespCode:    200,
				updateJobRespCode: 201,
				status:            "pending",
			},
			{
				respBody:          `{"jobId":"3","destinationId":"123","userAttributes":[{"userId":"3","phone":"555-555-666"},{"userId":"4","email":"johnny@example.com"}]}`,
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
		mu.Lock()
		status := test.status
		mu.Unlock()
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
	mu.Lock()
	if status == "complete" {
		testData[jobID-1].status = "complete"
	}
	mu.Unlock()
	w.WriteHeader(testData[jobID-1].updateJobRespCode)

	body, err := json.Marshal(struct{}{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(body)
}
