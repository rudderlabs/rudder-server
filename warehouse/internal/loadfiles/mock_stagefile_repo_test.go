package loadfiles_test

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockStageFilesRepo struct {
	store map[int64]model.StagingFile
}

func (m *mockStageFilesRepo) SetStatuses(_ context.Context, ids []int64, status string) (err error) {
	if m.store == nil {
		m.store = make(map[int64]model.StagingFile)
	}

	for _, id := range ids {
		m.store[id] = model.StagingFile{
			ID:     id,
			Status: status,
		}
	}

	return nil
}

func (m *mockStageFilesRepo) SetErrorStatus(_ context.Context, stagingFileID int64, stageFileErr error) error {
	m.store[stagingFileID] = model.StagingFile{
		ID:     stagingFileID,
		Status: warehouseutils.StagingFileFailedState,
		Error:  stageFileErr,
	}

	return nil
}
