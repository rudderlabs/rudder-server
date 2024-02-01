package source

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

type jobRunTaskRun struct {
	jobRunID  string
	taskRunID string
}

func newMockPublisher(mockResponse <-chan chan *notifier.PublishResponse, mockError error) *mockPublisher {
	return &mockPublisher{
		mockResponse: mockResponse,
		mockError:    mockError,
	}
}

type mockPublisher struct {
	mockResponse <-chan chan *notifier.PublishResponse
	mockError    error
}

func (m *mockPublisher) Publish(context.Context, *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error) {
	if m.mockError != nil {
		return nil, m.mockError
	}
	return <-m.mockResponse, nil
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

	t.Run("channel closed", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs := createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse)
		close(response)
		publishResponse := make(chan chan *notifier.PublishResponse, 1)
		publishResponse <- response
		close(publishResponse)

		m := New(config.New(), logger.NOP, db, newMockPublisher(publishResponse, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, sourceJobs, []int64{job.ID})
		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.EqualError(t, ErrReceivingChannelClosed, job.Error.Error())
	})
	t.Run("publishing error", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs := createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		m := New(config.New(), logger.NOP, db, newMockPublisher(nil, errors.New("test error")))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, sourceJobs, []int64{job.ID})
		require.Equal(t, model.SourceJobStatusWaiting, job.Status)
		require.Zero(t, job.Attempts)
		require.NoError(t, job.Error)
	})
	t.Run("publishing response error", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs := createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse, 1)
		response <- &notifier.PublishResponse{
			Err: errors.New("test error"),
		}
		close(response)
		publishResponse := make(chan chan *notifier.PublishResponse, 1)
		publishResponse <- response
		close(publishResponse)

		m := New(config.New(), logger.NOP, db, newMockPublisher(publishResponse, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, sourceJobs, []int64{job.ID})
		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.Error(t, job.Error)
	})
	t.Run("timeout", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs := createSourceJob(sr, sourceJobRunID, sourceTaskRunID, "test_table")

		response := make(chan *notifier.PublishResponse)
		defer close(response)
		publishResponse := make(chan chan *notifier.PublishResponse, 1)
		publishResponse <- response
		close(publishResponse)

		c := config.New()
		c.Set("Warehouse.jobs.processingTimeout", "1s")
		c.Set("Warehouse.jobs.processingSleepInterval", "1s")

		m := New(c, logger.NOP, db, newMockPublisher(publishResponse, nil))
		require.Error(t, m.Run(ctx))

		job, err := m.sourceRepo.GetByJobRunTaskRun(ctx, sourceJobRunID, sourceTaskRunID)
		require.NoError(t, err)

		require.Equal(t, sourceJobs, []int64{job.ID})
		require.Equal(t, model.SourceJobStatusFailed, job.Status)
		require.Equal(t, int64(1), job.Attempts)
		require.EqualError(t, ErrProcessingTimedOut, job.Error.Error())
	})
	t.Run("some succeeded, some failed", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs1 := createSourceJob(sr, sourceJobRunID+"-1", sourceTaskRunID+"-1", "test_table-1")
		sourceJobs2 := createSourceJob(sr, sourceJobRunID+"-2", sourceTaskRunID+"-2", "test_table-1")
		sourceJobs3 := createSourceJob(sr, sourceJobRunID+"-3", sourceTaskRunID+"-3", "test_table-1")
		sourceJobs4 := createSourceJob(sr, sourceJobRunID+"-4", sourceTaskRunID+"-4", "test_table-1")

		response := make(chan *notifier.PublishResponse, 1)
		response <- &notifier.PublishResponse{
			Jobs: []notifier.Job{
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs1[0])),
					Status:  notifier.Succeeded,
				},
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs2[0])),
					Status:  notifier.Failed,
					Error:   errors.New("test error"),
				},
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs3[0])),
					Status:  notifier.Failed,
					Error:   errors.New("test error"),
				},
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs4[0])),
					Status:  notifier.Succeeded,
				},
			},
		}
		close(response)
		publishResponse := make(chan chan *notifier.PublishResponse, 1)
		publishResponse <- response
		close(publishResponse)

		c := config.New()
		c.Set("Warehouse.jobs.processingSleepInterval", "1ms")
		c.Set("Warehouse.jobs.maxAttemptsPerJob", -1)

		m := New(c, logger.NOP, db, newMockPublisher(publishResponse, nil))

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return m.Run(gCtx)
		})
		g.Go(func() error {
			runs := []jobRunTaskRun{
				{jobRunID: sourceJobRunID + "-1", taskRunID: sourceTaskRunID + "-1"},
				{jobRunID: sourceJobRunID + "-2", taskRunID: sourceTaskRunID + "-2"},
				{jobRunID: sourceJobRunID + "-3", taskRunID: sourceTaskRunID + "-3"},
				{jobRunID: sourceJobRunID + "-4", taskRunID: sourceTaskRunID + "-4"},
			}
			require.Eventually(t, func() bool {
				defer cancel()

				jobs := getAll(t, context.Background(), m.sourceRepo, runs...)
				filteredJobs := lo.Filter(jobs, func(j *model.SourceJob, index int) bool {
					return j.Status == model.SourceJobStatusAborted || j.Status == model.SourceJobStatusSucceeded
				})
				return len(filteredJobs) == len(runs)
			},
				60*time.Second,
				100*time.Millisecond,
			)
			return nil
		})
		require.NoError(t, g.Wait())

		job1, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-1", sourceTaskRunID+"-1")
		require.NoError(t, err)
		job2, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-2", sourceTaskRunID+"-2")
		require.NoError(t, err)
		job3, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-3", sourceTaskRunID+"-3")
		require.NoError(t, err)
		job4, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-4", sourceTaskRunID+"-4")
		require.NoError(t, err)

		require.Equal(t, sourceJobs1, []int64{job1.ID})
		require.Equal(t, model.SourceJobStatusSucceeded, job1.Status)
		require.Equal(t, int64(0), job1.Attempts)
		require.NoError(t, job1.Error)
		require.Equal(t, sourceJobs2, []int64{job2.ID})
		require.Equal(t, model.SourceJobStatusAborted, job2.Status)
		require.Equal(t, int64(1), job2.Attempts)
		require.Error(t, job2.Error)
		require.Equal(t, sourceJobs3, []int64{job3.ID})
		require.Equal(t, model.SourceJobStatusAborted, job3.Status)
		require.Equal(t, int64(1), job3.Attempts)
		require.Error(t, job3.Error)
		require.Equal(t, sourceJobs4, []int64{job4.ID})
		require.Equal(t, model.SourceJobStatusSucceeded, job4.Status)
		require.Equal(t, int64(0), job4.Attempts)
		require.NoError(t, job4.Error)
	})
	t.Run("failed and then aborted", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs1 := createSourceJob(sr, sourceJobRunID+"-1", sourceTaskRunID+"-1", "test_table-1")
		sourceJobs2 := createSourceJob(sr, sourceJobRunID+"-2", sourceTaskRunID+"-2", "test_table-2")

		publishResponse := make(chan chan *notifier.PublishResponse, 10)
		for i := 0; i < 10; i++ {
			response := make(chan *notifier.PublishResponse, 1)
			response <- &notifier.PublishResponse{
				Jobs: []notifier.Job{
					{
						Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs1[0])),
						Status:  notifier.Succeeded,
					},
					{
						Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs2[0])),
						Status:  notifier.Failed,
						Error:   errors.New("test error"),
					},
				},
				Err: nil,
			}
			close(response)
			publishResponse <- response
		}
		close(publishResponse)

		c := config.New()
		c.Set("Warehouse.jobs.processingSleepInterval", "1ms")
		c.Set("Warehouse.jobs.maxAttemptsPerJob", 5)

		m := New(c, logger.NOP, db, newMockPublisher(publishResponse, nil))

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return m.Run(gCtx)
		})
		g.Go(func() error {
			runs := []jobRunTaskRun{
				{jobRunID: sourceJobRunID + "-1", taskRunID: sourceTaskRunID + "-1"},
				{jobRunID: sourceJobRunID + "-2", taskRunID: sourceTaskRunID + "-2"},
			}
			require.Eventually(t, func() bool {
				defer cancel()

				jobs := getAll(t, context.Background(), m.sourceRepo, runs...)
				filteredJobs := lo.Filter(jobs, func(j *model.SourceJob, index int) bool {
					return j.Status == model.SourceJobStatusAborted || j.Status == model.SourceJobStatusSucceeded
				})
				return len(filteredJobs) == len(runs)
			},
				60*time.Second,
				100*time.Millisecond,
			)
			return nil
		})
		require.NoError(t, g.Wait())

		job1, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-1", sourceTaskRunID+"-1")
		require.NoError(t, err)
		job2, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-2", sourceTaskRunID+"-2")
		require.NoError(t, err)

		require.Equal(t, sourceJobs1, []int64{job1.ID})
		require.Equal(t, model.SourceJobStatusSucceeded, job1.Status)
		require.Equal(t, int64(0), job1.Attempts)
		require.NoError(t, job1.Error)
		require.Equal(t, sourceJobs2, []int64{job2.ID})
		require.Equal(t, model.SourceJobStatusAborted, job2.Status)
		require.Equal(t, int64(7), job2.Attempts)
		require.Error(t, job2.Error)
	})
	t.Run("failed then succeeded", func(t *testing.T) {
		db := setupDB(t, pool)

		sr := repo.NewSource(db, repo.WithNow(func() time.Time {
			return now
		}))

		sourceJobs1 := createSourceJob(sr, sourceJobRunID+"-1", sourceTaskRunID+"-1", "test_table-1")
		sourceJobs2 := createSourceJob(sr, sourceJobRunID+"-2", sourceTaskRunID+"-2", "test_table-2")

		failedResponse := &notifier.PublishResponse{
			Jobs: []notifier.Job{
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs1[0])),
					Status:  notifier.Failed,
					Error:   errors.New("test error"),
				},
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs2[0])),
					Status:  notifier.Failed,
					Error:   errors.New("test error"),
				},
			},
			Err: nil,
		}
		succeededResponse := &notifier.PublishResponse{
			Jobs: []notifier.Job{
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs1[0])),
					Status:  notifier.Succeeded,
				},
				{
					Payload: []byte(fmt.Sprintf(`{"id": %d}`, sourceJobs2[0])),
					Status:  notifier.Succeeded,
				},
			},
			Err: nil,
		}
		publishResponse := make(chan chan *notifier.PublishResponse, 10)
		for i := 0; i < 10; i++ {
			response := make(chan *notifier.PublishResponse, 1)
			if i < 5 {
				response <- failedResponse
			} else {
				response <- succeededResponse
			}
			close(response)
			publishResponse <- response
		}
		close(publishResponse)

		c := config.New()
		c.Set("Warehouse.jobs.processingSleepInterval", "1ms")
		c.Set("Warehouse.jobs.maxAttemptsPerJob", 7)

		m := New(c, logger.NOP, db, newMockPublisher(publishResponse, nil))

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return m.Run(gCtx)
		})
		g.Go(func() error {
			runs := []jobRunTaskRun{
				{jobRunID: sourceJobRunID + "-1", taskRunID: sourceTaskRunID + "-1"},
				{jobRunID: sourceJobRunID + "-2", taskRunID: sourceTaskRunID + "-2"},
			}
			require.Eventually(t, func() bool {
				defer cancel()

				jobs := getAll(t, context.Background(), m.sourceRepo, runs...)
				filteredJobs := lo.Filter(jobs, func(j *model.SourceJob, index int) bool {
					return j.Status == model.SourceJobStatusAborted || j.Status == model.SourceJobStatusSucceeded
				})
				return len(filteredJobs) == len(runs)
			},
				60*time.Second,
				100*time.Millisecond,
			)
			return nil
		})
		require.NoError(t, g.Wait())

		job1, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-1", sourceTaskRunID+"-1")
		require.NoError(t, err)
		job2, err := m.sourceRepo.GetByJobRunTaskRun(context.Background(), sourceJobRunID+"-2", sourceTaskRunID+"-2")
		require.NoError(t, err)

		require.Equal(t, sourceJobs1, []int64{job1.ID})
		require.Equal(t, model.SourceJobStatusSucceeded, job1.Status)
		require.Equal(t, int64(5), job1.Attempts)
		require.Error(t, job1.Error)
		require.Equal(t, sourceJobs2, []int64{job2.ID})
		require.Equal(t, model.SourceJobStatusSucceeded, job2.Status) // Failed
		require.Equal(t, int64(5), job2.Attempts)
		require.Error(t, job2.Error)
	})
}

func setupDB(t *testing.T, pool *dockertest.Pool) *sqlmiddleware.DB {
	t.Helper()

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	return sqlmiddleware.New(pgResource.DB)
}

func getAll(t testing.TB, ctx context.Context, sourceRepo sourceRepo, runs ...jobRunTaskRun) (jobs []*model.SourceJob) {
	t.Helper()

	for _, run := range runs {
		job, err := sourceRepo.GetByJobRunTaskRun(ctx, run.jobRunID, run.taskRunID)
		require.NoError(t, err)

		jobs = append(jobs, job)
	}
	return
}
