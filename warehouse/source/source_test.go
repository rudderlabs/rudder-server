package source

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func newMockPublisher(mockResponse <-chan *notifier.PublishResponse, mockError error) *mockPublisher {
	return &mockPublisher{
		mockResponse: mockResponse,
		mockError:    mockError,
	}
}

type mockPublisher struct {
	mockResponse <-chan *notifier.PublishResponse
	mockError    error
}

func (m *mockPublisher) Publish(context.Context, *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error) {
	if m.mockError != nil {
		return nil, m.mockError
	}
	return m.mockResponse, nil
}

func TestSource(t *testing.T) {
	const (
		workspaceID     = "test_workspace_id"
		sourceID        = "test_source_id"
		destinationID   = "test_destination_id"
		sourceTaskRunID = "test_source_task_run_id"
		sourceJobRunID  = "test_source_job_run_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	createSourceJob := func(sourceRepo *repo.Source, jobRunID, taskRunID, tableName string) []int64 {
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

	t.Run("no pending jobs", func(t *testing.T) {
		//m := New(config.Default, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(nil, nil))
		//
		//ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		//defer cancel()
		//require.NoError(t, m.Run(ctx))
	})
	t.Run("channel closed", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		_ = createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse)
		close(response)

		m := New(config.Default, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(response, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.EqualError(t, ErrReceivingChannelClosed, job.Error.Error())
	})
	t.Run("publisher response error", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		_ = createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse, 1)
		response <- &notifier.PublishResponse{
			Err: errors.New("test error"),
		}
		close(response)

		m := New(config.Default, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(response, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.Error(t, job.Error)
	})
	t.Run("some succeeded, some failed", func(t *testing.T) {})
	t.Run("failed then succeeded", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJob := createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse, 2)
		response <- &notifier.PublishResponse{
			Jobs: []notifier.Job{
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJob[0])),
					Status:  notifier.Failed,
					Error:   errors.New("test error"),
				},
			},
			Err: nil,
		}
		response <- &notifier.PublishResponse{
			Jobs: []notifier.Job{
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJob[0])),
					Status:  notifier.Succeeded,
					Error:   nil,
				},
			},
			Err: nil,
		}
		close(response)

		c := config.New()
		c.Set("Warehouse.jobs.processingSleepInterval", "1s")

		m := New(c, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(response, nil))

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return m.Run(gCtx)
		})
		g.Go(func() error {
			require.Eventually(t, func() bool {
				job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
				if err != nil {
					return false
				}
				return job.Status == model.SourceJobStatusSucceeded
			},
				10*time.Second,
				1*time.Second,
			):q:q:qqq
			return nil
		})
		require.Error(t, g.Wait())

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, model.SourceJobStatusSucceeded, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.NoError(t, job.Error)
	})
	t.Run("failed then aborted", func(t *testing.T) {})
	t.Run("timeout", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		_ = createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse)
		defer close(response)

		c := config.New()
		c.Set("Warehouse.jobs.processingTimeout", "1s")
		c.Set("Warehouse.jobs.processingSleepInterval", "1s")

		m := New(c, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(response, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.EqualError(t, ErrProcessingTimedOut, job.Error.Error())
	})
	t.Run("succeeded", func(t *testing.T) {})
}
