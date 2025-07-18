package repo_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestStagingFileSchemaSnapshots(t *testing.T) {
	schema := json.RawMessage(`{"foo":"bar"}`)

	ctx := context.Background()
	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("Insert and GetLatest success", func(t *testing.T) {
		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		id, err := db.Insert(ctx, "sourceID", "destinationID", "workspaceID", schema)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, id)

		snap, err := db.GetLatest(ctx, "sourceID", "destinationID")
		require.NoError(t, err)
		expectedSnapshot := &model.StagingFileSchemaSnapshot{
			ID:            id,
			SourceID:      "sourceID",
			DestinationID: "destinationID",
			WorkspaceID:   "workspaceID",
			Schema:        schema,
			CreatedAt:     now,
		}
		require.Equal(t, expectedSnapshot, snap)
	})

	t.Run("GetLatest returns ErrNoSchemaSnapshot for missing entry", func(t *testing.T) {
		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		_, err := db.GetLatest(ctx, "notfound", "notfound")
		require.ErrorIs(t, err, repo.ErrNoSchemaSnapshot)
	})

	t.Run("Multiple inserts, GetLatest returns most recent", func(t *testing.T) {
		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		schema1 := json.RawMessage(`{"version":1}`)
		schema2 := json.RawMessage(`{"version":2}`)

		id1, err := db.Insert(ctx, "multiSourceID", "multiDestinationID", "multiWorkspaceID", schema1)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, id1)

		db = repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now.Add(1 * time.Second)
		}))

		id2, err := db.Insert(ctx, "multiSourceID", "multiDestinationID", "multiWorkspaceID", schema2)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, id2)

		snap, err := db.GetLatest(ctx, "multiSourceID", "multiDestinationID")
		require.NoError(t, err)
		expectedSnapshot := &model.StagingFileSchemaSnapshot{
			ID:            id2,
			SourceID:      "multiSourceID",
			DestinationID: "multiDestinationID",
			WorkspaceID:   "multiWorkspaceID",
			Schema:        schema2,
			CreatedAt:     now.Add(1 * time.Second).UTC(),
		}
		require.Equal(t, expectedSnapshot, snap)
	})

	t.Run("Insert/GetLatest with different keys", func(t *testing.T) {
		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		idA, err := db.Insert(ctx, "sourceID1", "destinationID1", "workspaceID1", json.RawMessage(`{"version":1}`))
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, idA)

		idB, err := db.Insert(ctx, "sourceID2", "destinationID2", "workspaceID2", json.RawMessage(`{"version":2}`))
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, idB)

		snapA, err := db.GetLatest(ctx, "sourceID1", "destinationID1")
		require.NoError(t, err)
		expectedSnapshotA := &model.StagingFileSchemaSnapshot{
			ID:            idA,
			SourceID:      "sourceID1",
			DestinationID: "destinationID1",
			WorkspaceID:   "workspaceID1",
			Schema:        json.RawMessage(`{"version":1}`),
			CreatedAt:     now.UTC(),
		}
		require.Equal(t, expectedSnapshotA, snapA)

		snapB, err := db.GetLatest(ctx, "sourceID2", "destinationID2")
		require.NoError(t, err)
		expectedSnapshotB := &model.StagingFileSchemaSnapshot{
			ID:            idB,
			SourceID:      "sourceID2",
			DestinationID: "destinationID2",
			WorkspaceID:   "workspaceID2",
			Schema:        json.RawMessage(`{"version":2}`),
			CreatedAt:     now,
		}
		require.Equal(t, expectedSnapshotB, snapB)
	})

	t.Run("Insert with empty schema", func(t *testing.T) {
		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		emptySchema := json.RawMessage([]byte{})
		id, err := db.Insert(ctx, "emptySourceID", "emptyDestinationID", "emptyWorkspaceID", emptySchema)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, id)

		snap, err := db.GetLatest(ctx, "emptySourceID", "emptyDestinationID")
		require.NoError(t, err)
		expectedSnapshot := &model.StagingFileSchemaSnapshot{
			ID:            id,
			SourceID:      "emptySourceID",
			DestinationID: "emptyDestinationID",
			WorkspaceID:   "emptyWorkspaceID",
			Schema:        emptySchema,
			CreatedAt:     now,
		}
		require.Equal(t, expectedSnapshot, snap)
	})

	t.Run("Context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		db := repo.NewStagingFileSchemaSnapshots(setupDB(t), repo.WithNow(func() time.Time {
			return now
		}))

		id, err := db.Insert(cancelCtx, "cancel", "cancel", "cancel", schema)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, uuid.Nil, id)

		_, err = db.GetLatest(cancelCtx, "cancel", "cancel")
		require.ErrorIs(t, err, context.Canceled)
	})
}
