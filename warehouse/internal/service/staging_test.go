package service_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
)

func TestStageFileBatching(t *testing.T) {
	t.Run("batch based on size", func(t *testing.T) {
		batchSize := 10

		var stageFiles []*model.StagingFile
		for i := 1; i <= 81; i += 1 {
			stageFiles = append(stageFiles, &model.StagingFile{
				ID: int64(i),
			})
		}

		batches := service.StageFileBatching(stageFiles, batchSize)
		require.Equal(t, 1+(len(stageFiles)/batchSize), len(batches))

		var reconstruct []*model.StagingFile
		for i := range batches {
			require.LessOrEqual(t, len(batches[i]), batchSize)
			reconstruct = append(reconstruct, batches[i]...)
		}
		require.Equal(t, stageFiles, reconstruct)
	})

	t.Run("batch based on useRudderStorage", func(t *testing.T) {
		batchSize := 10

		var stageFiles []*model.StagingFile
		for i := 1; i <= 81; i += 1 {
			stageFiles = append(stageFiles, &model.StagingFile{
				ID:               int64(i),
				UseRudderStorage: i%3 == 0 || i%4 == 0, // 0 0 1 1 0 0 1 1
			})
		}

		batches := service.StageFileBatching(stageFiles, batchSize)

		var reconstruct []*model.StagingFile
		for i := range batches {
			require.LessOrEqual(t, len(batches[i]), batchSize)

			for j := 1; j < len(batches[i]); j += 1 {
				require.Equal(t, batches[i][j-1].UseRudderStorage, batches[i][j].UseRudderStorage)
			}

			reconstruct = append(reconstruct, batches[i]...)
		}
		require.Equal(t, stageFiles, reconstruct)
	})
}

func BenchmarkStageFileBatching(b *testing.B) {
	batchSize := 1000
	files := 50001

	b.StopTimer()
	var stageFiles []*model.StagingFile
	for i := 1; i <= files; i += 1 {
		stageFiles = append(stageFiles, &model.StagingFile{
			ID:               int64(i),
			UseRudderStorage: i%3 == 0 || i%4 == 0, // 0 0 1 1 0 0 1 1
			WorkspaceID:      uuid.New().String(),
			SourceID:         uuid.New().String(),
			DestinationID:    uuid.New().String(),
		})
	}

	var fileBatches [][]*model.StagingFile

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fileBatches = service.StageFileBatching(stageFiles, batchSize)
	}

	_ = fileBatches
}

func FuzzStageFileBatching(f *testing.F) {
	f.Add(1, 1000, 10)
	f.Add(32, 1000, 10)
	f.Add(10, 81, 0)
	f.Add(10, 81, 100)
	f.Add(10000, 1000, 10)
	f.Add(1, 1000, 0)
	f.Add(1, 1000, 1000)

	f.Fuzz(func(t *testing.T, batchSize, files, change int) {
		if batchSize <= 0 || batchSize > 1_000_000 {
			t.Skip()
		}
		if files <= 0 || files > 1_000_000 {
			t.Skip()
		}
		if change < 0 {
			t.Skip()
		}

		var stageFiles []*model.StagingFile
		for i := 1; i <= files; i += 1 {
			stageFiles = append(stageFiles, &model.StagingFile{
				ID:               int64(i),
				UseRudderStorage: i >= change,
				WorkspaceID:      uuid.New().String(),
				SourceID:         uuid.New().String(),
				DestinationID:    uuid.New().String(),
			})
		}

		batches := service.StageFileBatching(stageFiles, batchSize)

		var reconstruct []*model.StagingFile
		for i := range batches {
			require.LessOrEqual(t, len(batches[i]), batchSize)

			for j := 1; j < len(batches[i]); j += 1 {
				require.Equal(t, batches[i][j-1].UseRudderStorage, batches[i][j].UseRudderStorage)
			}

			reconstruct = append(reconstruct, batches[i]...)
		}

		require.Equal(t, stageFiles, reconstruct)
	})
}
