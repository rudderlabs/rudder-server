package repo_test

import (
	"context"
	"encoding/json"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func sourcesJobs(
	sourceID, destinationID, workspaceID string,
	count int,
) []model.SourceJob {
	sourcesJobs := make([]model.SourceJob, 0, count)
	for i := 0; i < count; i++ {
		sourcesJobs = append(sourcesJobs, model.SourceJob{
			SourceID:      sourceID,
			DestinationID: destinationID,
			TableName:     "table" + strconv.Itoa(i),
			WorkspaceID:   workspaceID,
			Metadata:      json.RawMessage(`{"key": "value"}`),
		})
	}
	return sourcesJobs
}

func TestSource_Insert(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, 10),
		)
		require.NoError(t, err)
		require.Len(t, ids, 10)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		ids, err := repoSource.Insert(ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, 1),
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
	})
}

func TestSource_Reset(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, 10),
		)
		require.NoError(t, err)
		require.Len(t, ids, 10)

		for _, id := range ids[0:3] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusSucceeded, id)
			require.NoError(t, err)
		}
		for _, id := range ids[3:6] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusExecuting, id)
			require.NoError(t, err)
		}
		for _, id := range ids[6:10] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusFailed, id)
			require.NoError(t, err)
		}

		err = repoSource.Reset(ctx)
		require.NoError(t, err)

		for _, id := range ids[0:3] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusSucceeded, status)
		}

		for _, id := range ids[3:10] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusWaiting, status)
		}
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoSource.Reset(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}
