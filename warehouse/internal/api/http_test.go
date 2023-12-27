package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

type memRepo struct {
	files []model.StagingFileWithSchema
	err   error
}

func (m *memRepo) Insert(_ context.Context, stagingFile *model.StagingFileWithSchema) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}

	m.files = append(m.files, *stagingFile)
	return int64(len(m.files)), nil
}

func loadFile(t *testing.T, path string) string {
	t.Helper()

	b, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(b)
}

func filterPayload(text, match string) string {
	output := ""
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, match) {
			output += line + "\n"
		}
	}
	return output
}

func TestAPI_Process(t *testing.T) {
	body := loadFile(t, "./testdata/process_request.json")
	expectedStagingFile := model.StagingFileWithSchema{
		StagingFile: model.StagingFile{
			ID:                    0,
			WorkspaceID:           "279L3V7FSpx43LaNJ0nIs9KRaNC",
			Location:              "rudder-warehouse-staging-logs/279L3gEKqwruBoKGsXZtSVX7vIy/2022-11-08/1667913810.279L3gEKqwruBoKGsXZtSVX7vIy.7a6e7785-7a75-4345-8d3c-d7a1ce49a43f.json.gz",
			SourceID:              "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID:         "27CHciD6leAhurSyFAeN4dp14qZ",
			Status:                "",
			Error:                 nil,
			FirstEventAt:          time.Date(2022, time.November, 8, 13, 23, 7, 0, time.UTC),
			LastEventAt:           time.Date(2022, time.November, 8, 13, 23, 7, 0, time.UTC),
			UseRudderStorage:      false,
			DestinationRevisionID: "2H1cLBvL3v0prRBNzpe8D34XTzU",
			TotalEvents:           2,
			TotalBytes:            2000,
			SourceTaskRunID:       "<source-task-run-id>",
			SourceJobID:           "<source-job-id>",
			SourceJobRunID:        "<source-job-run-id>",
			TimeWindow:            time.Date(1, 1, 1, 0, 40, 0, 0, time.UTC),
			CreatedAt:             time.Time{},
			UpdatedAt:             time.Time{},
		},
		Schema: json.RawMessage("{\"product_track\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"product_id\":\"string\",\"rating\":\"int\",\"received_at\":\"datetime\",\"revenue\":\"float\",\"review_body\":\"string\",\"review_id\":\"string\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"},\"tracks\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"received_at\":\"datetime\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"}}"),
	}

	testcases := []struct {
		name                 string
		reqBody              string
		degradedWorkspaceIDs []string
		storeErr             error

		storage []model.StagingFileWithSchema

		respBody string
		respCode int
	}{
		{
			name:    "normal process request",
			reqBody: body,

			storage: []model.StagingFileWithSchema{expectedStagingFile},

			respCode: http.StatusOK,
		},
		{
			name:     "process request storage error",
			reqBody:  body,
			storeErr: fmt.Errorf("internal warehouse error"),

			respCode: http.StatusInternalServerError,
			respBody: "can't insert staging file\n",
		},
		{
			name:                 "process degraded workspace",
			reqBody:              body,
			degradedWorkspaceIDs: []string{"279L3V7FSpx43LaNJ0nIs9KRaNC"},

			respCode: http.StatusServiceUnavailable,
			respBody: "workspace is degraded\n",
		},
		{
			name: "invalid request body missing",

			respCode: http.StatusBadRequest,
			respBody: "invalid JSON in request body\n",
		},
		{
			name:    "invalid request workspace id missing",
			reqBody: filterPayload(body, "279L3V7FSpx43LaNJ0nIs9KRaNC"),

			respCode: http.StatusBadRequest,
			respBody: "invalid payload: workspaceId is required\n",
		},
		{
			name:    "invalid request location missing",
			reqBody: filterPayload(body, "rudder-warehouse-staging-logs"),

			respCode: http.StatusBadRequest,
			respBody: "invalid payload: location is required\n",
		},
		{
			name:    "invalid request source id missing",
			reqBody: filterPayload(body, "\"279L3gEKqwruBoKGsXZtSVX7vIy\""),

			respCode: http.StatusBadRequest,
			respBody: "invalid payload: batchDestination.source.id is required\n",
		},
		{
			name:    "invalid request destination id missing",
			reqBody: filterPayload(body, "\"27CHciD6leAhurSyFAeN4dp14qZ\""),

			respCode: http.StatusBadRequest,
			respBody: "invalid payload: batchDestination.destination.id is required\n",
		},
		{
			name:    "invalid request schema missing",
			reqBody: loadFile(t, "./testdata/process_request_missing_schema.json"),

			respCode: http.StatusBadRequest,
			respBody: "invalid payload: schema is required\n",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			r := &memRepo{
				err: tc.storeErr,
			}

			c := config.New()
			c.Set("Warehouse.degradedWorkspaceIDs", tc.degradedWorkspaceIDs)

			m := multitenant.New(c, backendconfig.DefaultBackendConfig)

			wAPI := api.WarehouseAPI{
				Repo:        r,
				Logger:      logger.NOP,
				Stats:       stats.NOP,
				Multitenant: m,
			}

			req, err := http.NewRequest(http.MethodPost, "https://localhost:8080/v1/process", bytes.NewBuffer([]byte(tc.reqBody)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			h := wAPI.Handler()
			h.ServeHTTP(resp, req)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			t.Log(string(body))

			require.Equal(t, tc.respCode, resp.Code)
			require.Equal(t, tc.respBody, string(body))

			require.Equal(t, tc.storage, r.files)
		})
	}
}
