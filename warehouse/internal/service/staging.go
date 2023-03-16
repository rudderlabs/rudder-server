package service

import (
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// StageFileBatching batches staging files.
func StageFileBatching(files []*model.StagingFile, batchSize int) [][]*model.StagingFile {
	fileBatches := make([][]*model.StagingFile, 0, len(files)/batchSize+1)
	for {
		if len(files) == 0 {
			break
		}

		cut := batchSize
		if len(files) < cut {
			cut = len(files)
		}

		for i := 1; i < cut; i += 1 {
			if files[i-1].UseRudderStorage != files[i].UseRudderStorage {
				cut = i
				break
			}
		}

		fileBatches = append(fileBatches, files[0:cut])
		files = files[cut:]
	}
	return fileBatches
}
