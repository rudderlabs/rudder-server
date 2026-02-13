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

	"github.com/google/uuid"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

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

type mockStagingFileSchemaSnapshotGetter struct {
	getFunc func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error)
}

func (m *mockStagingFileSchemaSnapshotGetter) GetOrCreate(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
	return m.getFunc(ctx, sourceID, destinationID, workspaceID, schemaBytes)
}

func filterPayload(text, match string) string {
	var output strings.Builder
	for line := range strings.SplitSeq(text, "\n") {
		if !strings.Contains(line, match) {
			output.WriteString(line + "\n")
		}
	}
	return output.String()
}

func TestAPI_Process(t *testing.T) {
	body := loadFile(t, "./testdata/process_request.json")

	expectedSnapshot := &model.StagingFileSchemaSnapshot{
		ID:            uuid.New(),
		Schema:        json.RawMessage("{\"product_track\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"product_id\":\"string\",\"rating\":\"int\",\"received_at\":\"datetime\",\"revenue\":\"float\",\"review_body\":\"string\",\"review_id\":\"string\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"},\"tracks\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"received_at\":\"datetime\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"}}"),
		SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
		DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
		WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
		CreatedAt:     time.Now().Add(-time.Hour),
	}
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
			BytesPerTable:         map[string]int64{"product_track": 1000, "tracks": 1000},
			SourceTaskRunID:       "<source-task-run-id>",
			SourceJobID:           "<source-job-id>",
			SourceJobRunID:        "<source-job-run-id>",
			TimeWindow:            time.Date(1, 1, 1, 0, 40, 0, 0, time.UTC),
			CreatedAt:             time.Time{},
			UpdatedAt:             time.Time{},
			ServerInstanceID:      "test-instance-id-v0",
		},
		Schema:        json.RawMessage("{\"product_track\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"product_id\":\"string\",\"rating\":\"int\",\"received_at\":\"datetime\",\"revenue\":\"float\",\"review_body\":\"string\",\"review_id\":\"string\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"},\"tracks\":{\"context_destination_id\":\"string\",\"context_destination_type\":\"string\",\"context_ip\":\"string\",\"context_library_name\":\"string\",\"context_passed_ip\":\"string\",\"context_request_ip\":\"string\",\"context_source_id\":\"string\",\"context_source_type\":\"string\",\"event\":\"string\",\"event_text\":\"string\",\"id\":\"string\",\"original_timestamp\":\"datetime\",\"received_at\":\"datetime\",\"sent_at\":\"datetime\",\"timestamp\":\"datetime\",\"user_id\":\"string\",\"uuid_ts\":\"datetime\"}}"),
		SnapshotID:    expectedSnapshot.ID,
		SnapshotPatch: json.RawMessage("[]"),
	}

	testcases := []struct {
		name                 string
		reqBody              string
		degradedWorkspaceIDs []string
		storeErr             error

		storage []model.StagingFileWithSchema

		expectedRespBody    string
		expectedRespCode    int
		expectedSchemaSize  int
		expectedTotalEvents int
	}{
		{
			name:    "normal process request",
			reqBody: body,

			storage: []model.StagingFileWithSchema{expectedStagingFile},

			expectedRespCode:    http.StatusOK,
			expectedSchemaSize:  1003,
			expectedTotalEvents: 2,
		},
		{
			name:     "process request storage error",
			reqBody:  body,
			storeErr: fmt.Errorf("internal warehouse error"),

			expectedRespCode: http.StatusInternalServerError,
			expectedRespBody: "can't insert staging file\n",
		},
		{
			name:                 "process degraded workspace",
			reqBody:              body,
			degradedWorkspaceIDs: []string{"279L3V7FSpx43LaNJ0nIs9KRaNC"},

			expectedRespCode: http.StatusServiceUnavailable,
			expectedRespBody: "workspace is degraded\n",
		},
		{
			name: "invalid request body missing",

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid JSON in request body\n",
		},
		{
			name:    "invalid request workspace id missing",
			reqBody: filterPayload(body, "279L3V7FSpx43LaNJ0nIs9KRaNC"),

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid payload: workspaceId is required\n",
		},
		{
			name:    "invalid request location missing",
			reqBody: filterPayload(body, "rudder-warehouse-staging-logs"),

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid payload: location is required\n",
		},
		{
			name:    "invalid request source id missing",
			reqBody: filterPayload(body, "\"279L3gEKqwruBoKGsXZtSVX7vIy\""),

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid payload: batchDestination.source.id is required\n",
		},
		{
			name:    "invalid request destination id missing",
			reqBody: filterPayload(body, "\"27CHciD6leAhurSyFAeN4dp14qZ\""),

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid payload: batchDestination.destination.id is required\n",
		},
		{
			name:    "invalid request schema missing",
			reqBody: loadFile(t, "./testdata/process_request_missing_schema.json"),

			expectedRespCode: http.StatusBadRequest,
			expectedRespBody: "invalid payload: schema is required\n",
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

			statsStore, err := memstats.New()
			require.NoError(t, err)

			wAPI := api.WarehouseAPI{
				Repo:        r,
				Logger:      logger.NOP,
				Stats:       statsStore,
				Multitenant: m,
				SchemaSnapshotHandler: &api.StagingFileSchemaSnapshotHandler{
					Snapshots: &mockStagingFileSchemaSnapshotGetter{
						getFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
							return expectedSnapshot, nil
						},
					},
					PatchGen: whutils.GenerateJSONPatch,
				},
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

			require.Equal(t, tc.expectedRespCode, resp.Code)
			require.Equal(t, tc.expectedRespBody, string(body))
			require.Equal(t, tc.storage, r.files)
			if tc.expectedRespCode == http.StatusOK {
				require.EqualValues(t, tc.expectedSchemaSize, statsStore.Get("warehouse_staged_schema_size", stats.Tags{
					"module":        "warehouse",
					"workspaceId":   "279L3V7FSpx43LaNJ0nIs9KRaNC",
					"sourceId":      "279L3gEKqwruBoKGsXZtSVX7vIy",
					"destinationId": "27CHciD6leAhurSyFAeN4dp14qZ",
					"warehouseID":   "__VX7vIy_dp14qZ",
				}).LastValue())
				require.EqualValues(t, tc.expectedTotalEvents, statsStore.Get("rows_staged", stats.Tags{
					"module":        "warehouse",
					"workspaceId":   "279L3V7FSpx43LaNJ0nIs9KRaNC",
					"sourceId":      "279L3gEKqwruBoKGsXZtSVX7vIy",
					"destinationId": "27CHciD6leAhurSyFAeN4dp14qZ",
					"warehouseID":   "__VX7vIy_dp14qZ",
				}).LastValue())
			}
		})
	}
}

func TestAPI_Process_WithSchemaPatch(t *testing.T) {
	body := loadFile(t, "./testdata/process_request.json")
	processRequestSchema := json.RawMessage(`{"product_track":{"context_destination_id":"string","context_destination_type":"string","context_ip":"string","context_library_name":"string","context_passed_ip":"string","context_request_ip":"string","context_source_id":"string","context_source_type":"string","event":"string","event_text":"string","id":"string","original_timestamp":"datetime","product_id":"string","rating":"int","received_at":"datetime","revenue":"float","review_body":"string","review_id":"string","sent_at":"datetime","timestamp":"datetime","user_id":"string","uuid_ts":"datetime"},"tracks":{"context_destination_id":"string","context_destination_type":"string","context_ip":"string","context_library_name":"string","context_passed_ip":"string","context_request_ip":"string","context_source_id":"string","context_source_type":"string","event":"string","event_text":"string","id":"string","original_timestamp":"datetime","received_at":"datetime","sent_at":"datetime","timestamp":"datetime","user_id":"string","uuid_ts":"datetime"}}`)

	createSnapshot := func(schema json.RawMessage) *model.StagingFileSchemaSnapshot {
		return &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now().Add(-time.Hour),
		}
	}

	mutateSchema := func(t *testing.T, baseSchema json.RawMessage, ops ...func([]byte) ([]byte, error)) json.RawMessage {
		t.Helper()
		b := []byte(baseSchema)
		for _, op := range ops {
			var err error
			b, err = op(b)
			require.NoError(t, err)
		}
		return json.RawMessage(b)
	}

	snapshot := createSnapshot(json.RawMessage("{\"product_track\":{\"id\":\"string\"},\"tracks\":{\"id\":\"string\"}}"))
	emptyPathSnapshot := createSnapshot(processRequestSchema)
	newColumnPatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema, func(b []byte) ([]byte, error) {
		return sjson.DeleteBytes(b, "product_track.event_text")
	}))
	newTablePatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema, func(b []byte) ([]byte, error) {
		return sjson.DeleteBytes(b, "tracks")
	}))
	modifiedColumnPatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema, func(b []byte) ([]byte, error) {
		return sjson.SetBytes(b, "product_track.event_text", "int")
	}))
	removedColumnPatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema, func(b []byte) ([]byte, error) {
		return sjson.SetBytes(b, "product_track.new_col", "string")
	}))
	removedTablePatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema, func(b []byte) ([]byte, error) {
		return sjson.SetBytes(b, "new_table.id", "string")
	}))
	multipleChangesPatchSnapshot := createSnapshot(mutateSchema(t, processRequestSchema,
		func(b []byte) ([]byte, error) { return sjson.SetBytes(b, "product_track.event_text", "int") },
		func(b []byte) ([]byte, error) { return sjson.SetBytes(b, "product_track.new_col", "string") },
		func(b []byte) ([]byte, error) { return sjson.SetBytes(b, "new_table.id", "string") },
	))

	testcases := []struct {
		name            string
		getSnapshotFunc func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error)
		patchGenerator  func(original, modified json.RawMessage) (json.RawMessage, error)

		expectedRespCode      int
		expectedRespBody      string
		expectedSnapshotPatch json.RawMessage
		expectedSnapshotID    uuid.UUID
	}{
		{
			name: "success with snapshot and patch",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return snapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"add","path":"/product_track/event_text","value":"string"},{"op":"add","path":"/product_track/review_id","value":"string"},{"op":"add","path":"/product_track/sent_at","value":"datetime"},{"op":"add","path":"/product_track/timestamp","value":"datetime"},{"op":"add","path":"/product_track/user_id","value":"string"},{"op":"add","path":"/product_track/context_destination_type","value":"string"},{"op":"add","path":"/product_track/context_source_type","value":"string"},{"op":"add","path":"/product_track/original_timestamp","value":"datetime"},{"op":"add","path":"/product_track/received_at","value":"datetime"},{"op":"add","path":"/product_track/revenue","value":"float"},{"op":"add","path":"/product_track/review_body","value":"string"},{"op":"add","path":"/product_track/uuid_ts","value":"datetime"},{"op":"add","path":"/product_track/context_destination_id","value":"string"},{"op":"add","path":"/product_track/context_library_name","value":"string"},{"op":"add","path":"/product_track/product_id","value":"string"},{"op":"add","path":"/product_track/rating","value":"int"},{"op":"add","path":"/product_track/context_ip","value":"string"},{"op":"add","path":"/product_track/context_passed_ip","value":"string"},{"op":"add","path":"/product_track/context_request_ip","value":"string"},{"op":"add","path":"/product_track/context_source_id","value":"string"},{"op":"add","path":"/product_track/event","value":"string"},{"op":"add","path":"/tracks/timestamp","value":"datetime"},{"op":"add","path":"/tracks/context_source_id","value":"string"},{"op":"add","path":"/tracks/received_at","value":"datetime"},{"op":"add","path":"/tracks/user_id","value":"string"},{"op":"add","path":"/tracks/uuid_ts","value":"datetime"},{"op":"add","path":"/tracks/context_request_ip","value":"string"},{"op":"add","path":"/tracks/event_text","value":"string"},{"op":"add","path":"/tracks/context_destination_type","value":"string"},{"op":"add","path":"/tracks/context_source_type","value":"string"},{"op":"add","path":"/tracks/event","value":"string"},{"op":"add","path":"/tracks/context_destination_id","value":"string"},{"op":"add","path":"/tracks/context_ip","value":"string"},{"op":"add","path":"/tracks/context_library_name","value":"string"},{"op":"add","path":"/tracks/context_passed_ip","value":"string"},{"op":"add","path":"/tracks/original_timestamp","value":"datetime"},{"op":"add","path":"/tracks/sent_at","value":"datetime"}]`),
			expectedSnapshotID:    snapshot.ID,
		},
		{
			name: "error on snapshot retrieval",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return nil, fmt.Errorf("db error")
			},
			expectedRespCode: http.StatusInternalServerError,
			expectedRespBody: "Unable to process schema snapshot: get schema snapshot: db error\n",
		},
		{
			name: "error on patch generation",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return snapshot, nil
			},
			patchGenerator: func(original, modified json.RawMessage) (json.RawMessage, error) {
				return nil, fmt.Errorf("patch error")
			},
			expectedRespCode: http.StatusInternalServerError,
			expectedRespBody: "Unable to process schema snapshot: generate schema patch: patch error\n",
		},
		{
			name: "empty patch (should succeed)",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return emptyPathSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[]`),
			expectedSnapshotID:    emptyPathSnapshot.ID,
		},
		{
			name: "new column added to existing table",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return newColumnPatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"add","path":"/product_track/event_text","value":"string"}]`),
			expectedSnapshotID:    newColumnPatchSnapshot.ID,
		},
		{
			name: "new table added",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return newTablePatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"add","path":"/tracks","value":{"context_destination_id":"string","context_destination_type":"string","context_ip":"string","context_library_name":"string","context_passed_ip":"string","context_request_ip":"string","context_source_id":"string","context_source_type":"string","event":"string","event_text":"string","id":"string","original_timestamp":"datetime","received_at":"datetime","sent_at":"datetime","timestamp":"datetime","user_id":"string","uuid_ts":"datetime"}}]`),
			expectedSnapshotID:    newTablePatchSnapshot.ID,
		},
		{
			name: "column type changed",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return modifiedColumnPatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"replace","path":"/product_track/event_text","value":"string"}]`),
			expectedSnapshotID:    modifiedColumnPatchSnapshot.ID,
		},
		{
			name: "column removed",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return removedColumnPatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"remove","path":"/product_track/new_col"}]`),
			expectedSnapshotID:    removedColumnPatchSnapshot.ID,
		},
		{
			name: "table removed",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return removedTablePatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"remove","path":"/new_table"}]`),
			expectedSnapshotID:    removedTablePatchSnapshot.ID,
		},
		{
			name: "multiple changes",
			getSnapshotFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
				return multipleChangesPatchSnapshot, nil
			},
			patchGenerator:        whutils.GenerateJSONPatch,
			expectedRespCode:      http.StatusOK,
			expectedSnapshotPatch: json.RawMessage(`[{"op":"replace","path":"/product_track/event_text","value":"string"},{"op":"remove","path":"/product_track/new_col"},{"op":"remove","path":"/new_table"}]`),
			expectedSnapshotID:    multipleChangesPatchSnapshot.ID,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			statsStore, err := memstats.New()
			require.NoError(t, err)

			r := &memRepo{}
			c := config.New()
			m := multitenant.New(c, backendconfig.DefaultBackendConfig)
			w := api.WarehouseAPI{
				Repo:        r,
				Logger:      logger.NOP,
				Stats:       statsStore,
				Multitenant: m,
				SchemaSnapshotHandler: &api.StagingFileSchemaSnapshotHandler{
					Snapshots: &mockStagingFileSchemaSnapshotGetter{
						getFunc: tc.getSnapshotFunc,
					},
					PatchGen: tc.patchGenerator,
				},
			}

			req, err := http.NewRequest(http.MethodPost, "https://localhost:8080/v1/process", bytes.NewBuffer([]byte(body)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			h := w.Handler()
			h.ServeHTTP(resp, req)
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRespCode, resp.Code)
			require.Equal(t, tc.expectedRespBody, string(respBody))

			if tc.expectedRespCode == http.StatusOK {
				require.Len(t, r.files, 1)
				file := r.files[0]
				require.Equal(t, tc.expectedSnapshotID, file.SnapshotID)
				require.ElementsMatch(t, tc.expectedSnapshotPatch, file.SnapshotPatch)
			}
		})
	}
}
