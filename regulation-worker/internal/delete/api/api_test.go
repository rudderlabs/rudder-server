package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func (d *deleteAPI) handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/deleteUsers", d.deleteMockServer).Methods("POST")

	return srvMux
}

func TestDelete(t *testing.T) {
	initialize.Init()
	tests := []struct {
		name                 string
		job                  model.Job
		destConfig           map[string]interface{}
		destName             string
		respCode             int
		respBodyStatus       model.JobStatus
		respBodyErr          error
		expectedDeleteStatus model.JobStatus
		expectedPayload      string
	}{
		{
			name: "test deleter API client with expected status complete",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293436@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad8536019424659691213279980@gmail.com",
						},
					},
					{
						ID: "Claiborn443446989226249191822329",
						Attributes: map[string]string{
							"phone": "8782905113",
						},
					},
				},
			},
			destConfig: map[string]interface{}{
				"bucketName":  "regulation-test-data",
				"accessKeyID": "abc",
				"accessKey":   "xyz",
				"enableSSE":   false,
				"prefix":      "reg-original",
			},
			destName:             "amplitude",
			respCode:             200,
			respBodyStatus:       "complete",
			expectedDeleteStatus: model.JobStatusComplete,
			expectedPayload:      `[{"jobId":"1","destType":"amplitude","config":{"accessKey":"xyz","accessKeyID":"abc","bucketName":"regulation-test-data","enableSSE":false,"prefix":"reg-original"},"userAttributes":[{"email":"dorowane8n285680461479465450293436@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279980@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 429",
			destName:             "amplitude",
			respCode:             429,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed-error returned 408",
			destName:             "amplitude",
			respCode:             408,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 504",
			destName:             "amplitude",
			respCode:             504,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 400",
			destName:             "amplitude",
			respCode:             400,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusAborted,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 401",
			destName:             "amplitude",
			respCode:             401,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusAborted,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 405",
			destName:             "amplitude",
			respCode:             405,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusNotSupported,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := deleteAPI{
				respStatusCode: tt.respCode,
			}
			ctx := context.Background()
			svr := httptest.NewServer(d.handler())

			defer svr.Close()
			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			api := api.APIManager{
				Client:           &http.Client{},
				DestTransformURL: svr.URL,
			}
			status := api.Delete(ctx, tt.job, tt.destConfig, tt.destName)
			require.Equal(t, tt.expectedDeleteStatus, status)
			require.Equal(t, tt.expectedPayload, d.payload)
		})
	}
}

type deleteAPI struct {
	payload        string
	respStatusCode int
	respBodyStatus model.JobStatus
	respBodyErr    error
}

func (d *deleteAPI) deleteMockServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	d.payload = buf.String()

	w.WriteHeader(d.respStatusCode)

	var resp api.JobRespSchema
	resp.Status = string(d.respBodyStatus)
	if d.respBodyErr != nil {
		resp.Error = d.respBodyErr.Error()
	}

	body, err := json.Marshal([]api.JobRespSchema{resp})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
