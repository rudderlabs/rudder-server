package stagingfiles

import (
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/stretchr/testify/require"
)

func getStagingFiles() []*model.StagingFile {
	var stagingFiles []*model.StagingFile
	for i := 0; i < 10; i++ {
		stagingFiles = append(stagingFiles, &model.StagingFile{
			ID: int64(i),
		})
	}
	return stagingFiles
}

func TestBatcher_SizeBasedGrouping(t *testing.T) {
	t.Run("size based grouping", func(t *testing.T) {
		testCases := []struct {
			name          string
			files         []*model.StagingFile
			batchSizes    []int
			skipSizeCheck bool
		}{
			{
				name:       "empty files",
				files:      []*model.StagingFile{},
				batchSizes: []int{},
			},
			{
				name: "single file under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
				},
				batchSizes: []int{1},
			},
			{
				name: "all files over limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 150 * 1024 * 1024, // 150MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 150 * 1024 * 1024, // 150MB
						},
					},
				},
				batchSizes:    []int{1, 1},
				skipSizeCheck: true,
			},
			{
				name: "multiple files under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024, // 40MB
						},
					},
				},
				batchSizes: []int{2},
			},
			{
				name: "optimal grouping case",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 100 * 1024 * 1024, // 100MB
						},
					},
					{
						ID: 3,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
				},
				batchSizes: []int{1, 2}, // [100MB], [50MB, 50MB]
			},
			{
				name: "sorting logic",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 20 * 1024 * 1024,
							"table2": 71 * 1024 * 1024,
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
					{
						ID: 3,
						BytesPerTable: map[string]int64{
							"table1": 70 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
					{
						ID: 4,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
				},
				batchSizes: []int{2, 2},
			},
			{
				name: "multiple tables different sizes",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 60 * 1024 * 1024, // 60MB
							"table2": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 5 * 1024 * 1024,  // 5MB
							"table2": 90 * 1024 * 1024, // 90MB
						},
					},
				},
				batchSizes: []int{1, 1}, // Split due to table2 exceeding limit when combined
			},
			{
				name: "multiple tables under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024, // 40MB
							"table2": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 30 * 1024 * 1024, // 30MB
							"table2": 20 * 1024 * 1024, // 20MB
						},
					},
				},
				batchSizes: []int{2},
			},
			{
				name: "one table exceeds limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 110 * 1024 * 1024,
							"table2": 3 * 1024 * 1024,
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table2": 20 * 1024 * 1024,
						},
					},
				},
				// Ideally we should have only 1 batch here
				// but we are not handling this case
				batchSizes:    []int{1, 1},
				skipSizeCheck: true,
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				batcher := NewBatcher(100, logger.NOP)
				batches := batcher.Batch(tc.files)
				require.Equal(t, len(tc.batchSizes), len(batches), "number of batches mismatch")

				actualBatchSizes := make([]int, len(batches))
				for i, batch := range batches {
					actualBatchSizes[i] = len(batch)
				}
				require.ElementsMatch(t, tc.batchSizes, actualBatchSizes, "batch sizes mismatch")

				if !tc.skipSizeCheck {
					maxSizeBytes := int64(100 * 1024 * 1024) // 100MB
					for _, batch := range batches {
						tableSizes := make(map[string]int64)
						for _, file := range batch {
							for table, size := range file.BytesPerTable {
								tableSizes[table] += size
							}
						}
						for _, size := range tableSizes {
							require.LessOrEqual(t, size, maxSizeBytes, "table size exceeds limit in a batch")
						}
					}
				}
			})
		}
	})
}

func TestBatcher_KeyBasedGrouping(t *testing.T) {
	t.Run("key based grouping", func(t *testing.T) {
		testCases := []struct {
			name       string
			files      []*model.StagingFile
			batchSizes []int
		}{
			{
				name: "group by UseRudderStorage",
				files: []*model.StagingFile{
					{
						ID:               1,
						UseRudderStorage: true,
					},
					{
						ID:               2,
						UseRudderStorage: false,
					},
					{
						ID:               3,
						UseRudderStorage: true,
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "group by DestinationRevisionID",
				files: []*model.StagingFile{
					{
						ID:                    1,
						DestinationRevisionID: "rev1",
					},
					{
						ID:                    2,
						DestinationRevisionID: "rev2",
					},
					{
						ID:                    3,
						DestinationRevisionID: "rev1",
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "group by TimeWindow",
				files: []*model.StagingFile{
					{
						ID:         1,
						TimeWindow: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:         2,
						TimeWindow: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:         3,
						TimeWindow: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "mixed keys",
				files: []*model.StagingFile{
					{
						ID:                    1,
						UseRudderStorage:      true,
						DestinationRevisionID: "rev1",
						TimeWindow:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:                    2,
						UseRudderStorage:      true,
						DestinationRevisionID: "rev1",
						TimeWindow:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:                    3,
						UseRudderStorage:      false,
						DestinationRevisionID: "rev2",
						TimeWindow:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				batchSizes: []int{1, 1, 1},
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				batcher := NewBatcher(100, logger.NOP)
				batches := batcher.Batch(tc.files)
				require.Equal(t, len(tc.batchSizes), len(batches), "number of batches mismatch")

				actualBatchSizes := make([]int, len(batches))
				for i, batch := range batches {
					actualBatchSizes[i] = len(batch)
				}
				require.ElementsMatch(t, tc.batchSizes, actualBatchSizes, "batch sizes mismatch")

				for _, batch := range batches {
					firstFile := batch[0]
					for _, file := range batch[1:] {
						require.Equal(t, firstFile.UseRudderStorage, file.UseRudderStorage, "UseRudderStorage mismatch in batch")
						require.Equal(t, firstFile.DestinationRevisionID, file.DestinationRevisionID, "DestinationRevisionID mismatch in batch")
						require.Equal(t, firstFile.TimeWindow, file.TimeWindow, "TimeWindow mismatch in batch")
					}
				}
			})
		}
	})
}
