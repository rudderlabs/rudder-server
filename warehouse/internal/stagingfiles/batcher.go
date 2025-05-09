package stagingfiles

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/samber/lo"
	"slices"
	"time"
)

type Batcher struct {
	maxLoadFileSizeInMB int
	logger              logger.Logger
}

type stagingFileGroupKey struct {
	UseRudderStorage             bool
	StagingUseRudderStorage      bool
	DestinationRevisionID        string
	StagingDestinationRevisionID string
	TimeWindow                   time.Time
}

func NewBatcher(maxLoadFileSizeInMB int, l logger.Logger) *Batcher {
	return &Batcher{
		maxLoadFileSizeInMB: maxLoadFileSizeInMB,
		logger:              l,
	}
}

func (b *Batcher) Batch(files []*model.StagingFile) [][]*model.StagingFile {
	groups := lo.GroupBy(files, func(file *model.StagingFile) stagingFileGroupKey {
		return stagingFileGroupKey{
			UseRudderStorage:             file.UseRudderStorage,
			StagingUseRudderStorage:      file.UseRudderStorage,
			DestinationRevisionID:        file.DestinationRevisionID,
			StagingDestinationRevisionID: file.DestinationRevisionID,
			TimeWindow:                   file.TimeWindow,
		}
	})

	result := make([][]*model.StagingFile, 0, len(groups))
	// For each group, apply size constraints
	for _, group := range groups {
		result = append(result, b.groupBySize(group)...)
	}
	return result

}

type tableSizeResult struct {
	sizes map[string]int64
	name  string
	size  int64
}

func (b *Batcher) groupBySize(files []*model.StagingFile) [][]*model.StagingFile {
	maxSizeMB := b.maxLoadFileSizeInMB
	maxSizeBytes := int64(maxSizeMB) * 1024 * 1024 // Convert MB to bytes

	// Find the table with the maximum total size
	maxTable := lo.Reduce(files, func(acc tableSizeResult, file *model.StagingFile, _ int) tableSizeResult {
		for tableName, size := range file.BytesPerTable {
			acc.sizes[tableName] += size
			if acc.sizes[tableName] > acc.size {
				acc.name = tableName
				acc.size = acc.sizes[tableName]
			}
		}
		return acc
	}, tableSizeResult{
		sizes: make(map[string]int64),
	})

	b.logger.Infof("maxTable: %s, maxTableSize: %d", maxTable.name, maxTable.size)

	// Sorting ensures that minimum batches are created
	slices.SortFunc(files, func(a, b *model.StagingFile) int {
		// Assuming that there won't be any overflows
		return int(b.BytesPerTable[maxTable.name] - a.BytesPerTable[maxTable.name])
	})

	var result [][]*model.StagingFile
	processed := make(map[int64]bool, len(files))

	for len(processed) < len(files) {
		// Start a new batch
		var currentBatch []*model.StagingFile
		batchTableSizes := make(map[string]int64)

		// Try to add files to the current batch
		for _, file := range files {
			if processed[file.ID] {
				continue
			}

			// Check if adding this file would exceed size limit for any table
			canAdd := true
			for tableName, size := range file.BytesPerTable {
				newSize := batchTableSizes[tableName] + size
				if newSize > maxSizeBytes {
					canAdd = false
					break
				}
			}

			if canAdd {
				// Add file to batch and update table sizes
				currentBatch = append(currentBatch, file)
				for tableName, size := range file.BytesPerTable {
					batchTableSizes[tableName] += size
				}
				processed[file.ID] = true
			} else {
				// If this is the first file in this iteration and it exceeds limits,
				// add it to its own batch
				if len(currentBatch) == 0 {
					result = append(result, []*model.StagingFile{file})
					processed[file.ID] = true
					break
				}
			}
		}
		// This condition will be false if a file was already added to the batch because it exceeded the size limit
		// In that case, it should not be added again to result
		if len(currentBatch) > 0 {
			result = append(result, currentBatch)
		}
	}

	return result
}
