package jobs

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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"
)

func TestAsyncJobHandlers(t *testing.T) {
	const (
		workspaceID         = "test_workspace_id"
		sourceID            = "test_source_id"
		destinationID       = "test_destination_id"
		workspaceIdentifier = "test_workspace-identifier"
		namespace           = "test_namespace"
		destinationType     = "test_destination_type"
		sourceTaskRunID     = "test_source_task_run_id"
		sourceJobID         = "test_source_job_id"
		sourceJobRunID      = "test_source_job_run_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	db := sqlmiddleware.New(pgResource.DB)

	ctx := context.Background()

	n := notifier.New(config.Default, logger.NOP, stats.Default, workspaceIdentifier)
	err = n.Setup(ctx, pgResource.DBDsn)
	require.NoError(t, err)

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

	t.Run("validate payload", func(t *testing.T) {
		testCases := []struct {
			name          string
			payload       StartJobReqPayload
			expectedError error
		}{
			{
				name: "invalid source",
				payload: StartJobReqPayload{
					JobRunID:      "job_run_id",
					TaskRunID:     "task_run_id",
					SourceID:      "",
					DestinationID: "destination_id",
					WorkspaceID:   "workspace_id",
				},
				expectedError: errors.New("source_id is required"),
			},
			{
				name: "invalid destination",
				payload: StartJobReqPayload{
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
				payload: StartJobReqPayload{
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
				payload: StartJobReqPayload{
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
				payload: StartJobReqPayload{
					JobRunID:      "job_run_id",
					TaskRunID:     "task_run_id",
					SourceID:      "source_id",
					DestinationID: "destination_id",
					WorkspaceID:   "workspace_id",
				},
			},
		}
		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.expectedError, validatePayload(&tc.payload))
			})
		}
	})

	t.Run("InsertJobHandler", func(t *testing.T) {
		t.Run("Not enabled", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", nil)
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  false,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.InsertJobHandler(resp, req)
			require.Equal(t, http.StatusInternalServerError, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "warehouse jobs api not initialized\n", string(b))
		})
		t.Run("invalid payload", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", bytes.NewReader([]byte(`"Invalid payload"`)))
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  true,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.InsertJobHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "invalid JSON in request body\n", string(b))
		})
		t.Run("invalid request", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/jobs", bytes.NewReader([]byte(`{}`)))
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  true,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.InsertJobHandler(resp, req)
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
				  "task_run_id": "test_source_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  true,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.InsertJobHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var insertResponse insertJobResponse
			err = json.NewDecoder(resp.Body).Decode(&insertResponse)
			require.NoError(t, err)
			require.Nil(t, insertResponse.Err)
			require.Len(t, insertResponse.JobIds, 5)
		})
	})

	t.Run("StatusJobHandler", func(t *testing.T) {
		t.Run("Not enabled", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status", nil)
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  false,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.StatusJobHandler(resp, req)
			require.Equal(t, http.StatusInternalServerError, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "warehouse jobs api not initialized\n", string(b))
		})
		t.Run("invalid payload", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status", nil)
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  true,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.StatusJobHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "invalid request: source_id is required\n", string(b))
		})
		t.Run("success", func(t *testing.T) {
			_, err := db.ExecContext(ctx, `
				INSERT INTO `+whutils.WarehouseAsyncJobTable+` (source_id, destination_id, status, created_at, updated_at, tablename, error, async_job_type, metadata, workspace_id)
				VALUES ('test_source_id', 'test_destination_id', 'aborted', NOW(), NOW(), 'test_table_name', 'test_error', 'deletebyjobrunid', '{"job_run_id": "test_source_job_run_id", "task_run_id": "test_source_task_run_id"}', 'test_workspace_id')
			`)
			require.NoError(t, err)

			qp := url.Values{}
			qp.Add("task_run_id", sourceTaskRunID)
			qp.Add("job_run_id", sourceJobRunID)
			qp.Add("source_id", sourceID)
			qp.Add("destination_id", destinationID)
			qp.Add("workspace_id", workspaceID)

			req := httptest.NewRequest(http.MethodGet, "/v1/warehouse/jobs/status?"+qp.Encode(), nil)
			resp := httptest.NewRecorder()

			jobsManager := AsyncJobWh{
				db:       db,
				enabled:  true,
				logger:   logger.NOP,
				context:  ctx,
				notifier: n,
			}
			jobsManager.StatusJobHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var statusResponse WhStatusResponse
			err = json.NewDecoder(resp.Body).Decode(&statusResponse)
			require.NoError(t, err)
			require.Equal(t, statusResponse.Status, "aborted")
			require.Equal(t, statusResponse.Err, "test_error")
		})
	})
}
