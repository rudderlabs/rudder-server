package repo_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestStatsEmission(t *testing.T) {
	t.Run("Query duration", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		repoLoadFiles := repo.NewLoadFiles(db, config.New(), repo.WithStats(statsStore))
		repoSchemas := repo.NewWHSchemas(db, config.New(), repo.WithStats(statsStore))
		repoStagingFiles := repo.NewStagingFiles(db, config.New(), repo.WithStats(statsStore))
		repoTableUploads := repo.NewTableUploads(db, config.New(), repo.WithStats(statsStore))
		repoSources := repo.NewSource(db, repo.WithStats(statsStore))
		repoUploads := repo.NewUploads(db, repo.WithStats(statsStore))
		repoStagingFileSchemaSnapshots := repo.NewStagingFileSchemaSnapshots(db, repo.WithStats(statsStore))

		require.NoError(t, repoLoadFiles.Insert(ctx, []model.LoadFile{
			{
				ID:              1,
				SourceID:        "source_id",
				DestinationID:   "destination_id",
				DestinationType: "destination_type",
			},
		}))
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":   "insert",
			"repoType": "wh_load_files",
			"sourceId": "source_id",
			"destId":   "destination_id",
			"destType": "destination_type",
		}).LastDuration(), time.Duration(0))

		err = repoSchemas.Insert(ctx, &model.WHSchema{
			ID:              1,
			SourceID:        "source_id",
			DestinationID:   "destination_id",
			DestinationType: "destination_type",
		})
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":   "insert",
			"repoType": "wh_schemas",
			"sourceId": "source_id",
			"destId":   "destination_id",
			"destType": "destination_type",
		}).LastDuration(), time.Duration(0))

		_, err = repoStagingFileSchemaSnapshots.Insert(ctx, "source_id", "destination_id", "workspace_id", json.RawMessage(`{}`))
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":      "insert",
			"repoType":    "wh_staging_file_schema_snapshots",
			"sourceId":    "source_id",
			"destId":      "destination_id",
			"workspaceId": "workspace_id",
		}).LastDuration(), time.Duration(0))

		_, err = repoStagingFiles.Insert(ctx, lo.ToPtr((model.StagingFile{
			SourceID:      "source_id",
			DestinationID: "destination_id",
			WorkspaceID:   "workspace_id",
		}).WithSchema(json.RawMessage(`{}`))))
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":      "insert",
			"repoType":    "wh_staging_files",
			"sourceId":    "source_id",
			"destId":      "destination_id",
			"workspaceId": "workspace_id",
		}).LastDuration(), time.Duration(0))

		err = repoTableUploads.Insert(ctx, 1, []string{"table1", "table2"})
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":   "insert",
			"repoType": "wh_table_uploads",
		}).LastDuration(), time.Duration(0))

		_, err = repoSources.Insert(ctx, []model.SourceJob{
			{
				SourceID:      "source_id",
				DestinationID: "destination_id",
				WorkspaceID:   "workspace_id",
				TableName:     "test_table",
				JobType:       model.SourceJobTypeDeleteByJobRunID,
				Metadata:      json.RawMessage(`{}`),
			},
		})
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":      "insert",
			"repoType":    "wh_async_jobs",
			"sourceId":    "source_id",
			"destId":      "destination_id",
			"workspaceId": "workspace_id",
		}).LastDuration(), time.Duration(0))

		_, err = repoUploads.CreateWithStagingFiles(ctx, model.Upload{
			ID:              1,
			SourceID:        "source_id",
			DestinationID:   "destination_id",
			DestinationType: "destination_type",
			WorkspaceID:     "workspace_id",
		},
			[]*model.StagingFile{
				{
					ID:            1,
					SourceID:      "source_id",
					DestinationID: "destination_id",
				},
			},
		)
		require.NoError(t, err)
		require.Greater(t, statsStore.Get("warehouse_repo_query_duration_seconds", stats.Tags{
			"action":      "create_with_staging_files",
			"repoType":    "wh_uploads",
			"sourceId":    "source_id",
			"destId":      "destination_id",
			"destType":    "destination_type",
			"workspaceId": "workspace_id",
		}).LastDuration(), time.Duration(0))
	})
}
