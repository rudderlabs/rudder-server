package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"
)

func TestValidatePayload(t *testing.T) {
	testCases := []struct {
		name          string
		payload       insertJobRequest
		expectedError error
	}{
		{
			name: "invalid source (empty string)",
			payload: insertJobRequest{
				JobRunID:      "job_run_id",
				TaskRunID:     "task_run_id",
				SourceID:      "",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
			},
			expectedError: errors.New("source_id is required"),
		},

		{
			name: "invalid source (empty string with spaces)",
			payload: insertJobRequest{
				JobRunID:      "job_run_id",
				TaskRunID:     "task_run_id",
				SourceID:      "   ",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
			},
			expectedError: errors.New("source_id is required"),
		},
		{
			name: "invalid destination",
			payload: insertJobRequest{
				JobRunID:      "job_run_id",
				TaskRunID:     "task_run_id",
				SourceID:      "source_id",
				DestinationID: "",
				WorkspaceID:   "workspace_id",
			},
			expectedError: errors.New("destination_id is required"),
		},
		{
			name: "invalid task run",
			payload: insertJobRequest{
				JobRunID:      "job_run_id",
				TaskRunID:     "",
				SourceID:      "source_id",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
			},
			expectedError: errors.New("task_run_id is required"),
		},
		{
			name: "invalid job run",
			payload: insertJobRequest{
				JobRunID:      "",
				TaskRunID:     "task_run_id",
				SourceID:      "source_id",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
			},
			expectedError: errors.New("job_run_id is required"),
		},
		{
			name: "valid payload",
			payload: insertJobRequest{
				JobRunID:      "job_run_id",
				TaskRunID:     "task_run_id",
				SourceID:      "source_id",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedError, validatePayload(&tc.payload))
		})
	}
}

func TestManager_InsertJobHandler(t *testing.T) {
	const (
		workspaceID     = "test_workspace_id"
		sourceID        = "test_source_id"
		destinationID   = "test_destination_id"
		namespace       = "test_namespace"
		destinationType = "test_destination_type"
		sourceTaskRunID = "test_source_task_run_id"
		sourceJobID     = "test_source_job_id"
		sourceJobRunID  = "test_source_job_run_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	db, ctx := setupDB(t, pool), context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	uploadsRepo := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	tableUploadsRepo := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	stagingRepo := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	stagingFile := model.StagingFile{
		WorkspaceID:           workspaceID,
		Location:              "s3://bucket/path/to/file",
		SourceID:              sourceID,
		DestinationID:         destinationID,
		Status:                whutils.StagingFileWaitingState,
		Error:                 fmt.Errorf("dummy error"),
		FirstEventAt:          now.Add(time.Second),
		UseRudderStorage:      true,
		DestinationRevisionID: "destination_revision_id",
		TotalEvents:           100,
		SourceTaskRunID:       sourceTaskRunID,
		SourceJobID:           sourceJobID,
		SourceJobRunID:        sourceJobRunID,
		TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
	}.WithSchema([]byte(`{"type": "object"}`))

	stagingID, err := stagingRepo.Insert(ctx, &stagingFile)
	require.NoError(t, err)

	uploadID, err := uploadsRepo.CreateWithStagingFiles(ctx, model.Upload{
		WorkspaceID:     workspaceID,
		Namespace:       namespace,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		Status:          model.Aborted,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}, []*model.StagingFile{{
		ID:              stagingID,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}})
	require.NoError(t, err)

	err = tableUploadsRepo.Insert(ctx, uploadID, []string{
		"test_table_1",
		"test_table_2",
		"test_table_3",
		"test_table_4",
		"test_table_5",

		"rudder_discards",
		"rudder_identity_mappings",
		"rudder_identity_merge_rules",
	})
	require.NoError(t, err)

	t.Run("invalid payload", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", bytes.NewReader([]byte(`"Invalid payload"`)))
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.InsertJobHandler(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Code)

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "invalid JSON in request body\n", string(b))
	})
	t.Run("invalid request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", bytes.NewReader([]byte(`{}`)))
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.InsertJobHandler(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Code)

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "invalid payload: source_id is required\n", string(b))
	})
	t.Run("success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "destination_id": "test_destination_id",
				  "job_run_id": "test_source_job_run_id",
				  "task_run_id": "test_source_task_run_id",
                  "async_job_type": "deletebyjobrunid"
				}
			`)))
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.InsertJobHandler(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)

		var insertResponse insertJobResponse
		err = json.NewDecoder(resp.Body).Decode(&insertResponse)
		require.NoError(t, err)
		require.Nil(t, insertResponse.Err)
		require.Len(t, insertResponse.JobIds, 5)
	})
	t.Run("exclude tables", func(t *testing.T) {
		// discards, merge rules and mapping tables should be excluded
	})
}

func TestManager_StatusJobHandler(t *testing.T) {
	const (
		workspaceID     = "test_workspace_id"
		sourceID        = "test_source_id"
		destinationID   = "test_destination_id"
		sourceJobID     = "test_source_job_id"
		sourceTaskRunID = "test_source_task_run_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	db, ctx := setupDB(t, pool), context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	sourceRepo := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	createSourceJob := func(jobRunID, taskRunID, tableName string) []int64 {
		metadata := fmt.Sprintf(`{"job_run_id":"%s","task_run_id":"%s","jobtype":"%s","start_time":"%s"}`,
			jobRunID,
			taskRunID,
			model.SourceJobTypeDeleteByJobRunID,
			now.Format(time.RFC3339),
		)

		ids, err := sourceRepo.Insert(ctx, []model.SourceJob{
			{
				SourceID:      sourceID,
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				TableName:     tableName,
				JobType:       model.SourceJobTypeDeleteByJobRunID,
				Metadata:      []byte(metadata),
			},
		})
		require.NoError(t, err)

		return ids
	}

	t.Run("invalid payload", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status", nil)
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.StatusJobHandler(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Code)

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "invalid request: source_id is required\n", string(b))
	})
	t.Run("status waiting", func(t *testing.T) {
		_ = createSourceJob(sourceJobID+"-1", sourceTaskRunID+"-1", "test_table-1")

		qp := url.Values{}
		qp.Add("task_run_id", sourceTaskRunID+"-1")
		qp.Add("job_run_id", sourceJobID+"-1")
		qp.Add("source_id", sourceID)
		qp.Add("destination_id", destinationID)
		qp.Add("workspace_id", workspaceID)

		req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status?"+qp.Encode(), nil)
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.StatusJobHandler(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)

		var statusResponse jobStatusResponse
		err = json.NewDecoder(resp.Body).Decode(&statusResponse)
		require.NoError(t, err)
		require.Equal(t, statusResponse.Status, model.SourceJobStatusWaiting.String())
		require.Empty(t, statusResponse.Err)
	})
	t.Run("status aborted", func(t *testing.T) {
		ids := createSourceJob(sourceJobID+"-2", sourceTaskRunID+"-2", "test_table-2")

		for _, id := range ids {
			err := sourceRepo.OnUpdateFailure(ctx, id, errors.New("test error"), -1)
			require.NoError(t, err)
		}

		qp := url.Values{}
		qp.Add("task_run_id", sourceTaskRunID+"-2")
		qp.Add("job_run_id", sourceJobID+"-2")
		qp.Add("source_id", sourceID)
		qp.Add("destination_id", destinationID)
		qp.Add("workspace_id", workspaceID)

		req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status?"+qp.Encode(), nil)
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.StatusJobHandler(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)

		var statusResponse jobStatusResponse
		err = json.NewDecoder(resp.Body).Decode(&statusResponse)
		require.NoError(t, err)
		require.Equal(t, statusResponse.Status, model.SourceJobStatusAborted.String())
		require.Equal(t, statusResponse.Err, errors.New("test error").Error())
	})
	t.Run("job not found", func(t *testing.T) {
		qp := url.Values{}
		qp.Add("task_run_id", sourceTaskRunID+"-unknown")
		qp.Add("job_run_id", sourceJobID+"-unknown")
		qp.Add("source_id", sourceID)
		qp.Add("destination_id", destinationID)
		qp.Add("workspace_id", workspaceID)

		req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status?"+qp.Encode(), nil)
		resp := httptest.NewRecorder()

		sourceManager := New(config.New(), logger.NOP, db, &mockPublisher{})
		sourceManager.StatusJobHandler(resp, req)
		require.Equal(t, http.StatusNotFound, resp.Code)

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "sources job not found\n", string(b))
	})
}
