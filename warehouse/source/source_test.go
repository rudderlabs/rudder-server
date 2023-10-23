package source

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
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

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)
	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now().Truncate(time.Second).UTC()

	sourceJobs := []model.SourceJob{
		{
			SourceID:      sourceID,
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			TableName:     "test_table_name",
			JobType:       model.SourceJobTypeDeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id":"` + sourceJobRunID + `","task_run_id":"` + sourceTaskRunID + `","jobtype":"` + model.SourceJobTypeDeleteByJobRunID + `","start_time":"` + now.Format(time.RFC3339) + `"}`),
		},
	}

	m := New(config.Default, logger.NOP, sqlmiddleware.New(pgResource.DB), newMockPublisher(nil, nil))

	ids, err := m.sourceRepo.Insert(ctx, sourceJobs)
	require.NoError(t, err)
	require.Len(t, ids, len(sourceJobs))

	time.AfterFunc(3*time.Second, func() {
		cancel()
	})
	err = m.Run(ctx)
	require.NoError(t, err)

	t.Run("no pending jobs", func(t *testing.T) {})
	t.Run("channel closed", func(t *testing.T) {})
	t.Run("publisher response error", func(t *testing.T) {})
	t.Run("some succeeded, some failed", func(t *testing.T) {})
	t.Run("failed then succeeded", func(t *testing.T) {})
	t.Run("timeout", func(t *testing.T) {})
}
