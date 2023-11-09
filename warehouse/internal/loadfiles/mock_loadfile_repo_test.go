package loadfiles_test

import (
	"context"
	"slices"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type mockLoadFilesRepo struct {
	id    int64
	store []model.LoadFile
}

func (m *mockLoadFilesRepo) Insert(_ context.Context, loadFiles []model.LoadFile) error {
	for _, lf := range loadFiles {
		lf.ID = m.id + 1
		m.id += 1

		m.store = append(m.store, lf)
	}

	return nil
}

func (m *mockLoadFilesRepo) DeleteByStagingFiles(_ context.Context, stagingFileIDs []int64) error {
	store := make([]model.LoadFile, 0)
	for _, loadFile := range m.store {
		if !slices.Contains(stagingFileIDs, loadFile.StagingFileID) {
			store = append(store, loadFile)
		}
	}
	m.store = store

	return nil
}

func (m *mockLoadFilesRepo) GetByStagingFiles(_ context.Context, stagingFileIDs []int64) ([]model.LoadFile, error) {
	var loadFiles []model.LoadFile
	for _, loadFile := range m.store {
		if slices.Contains(stagingFileIDs, loadFile.StagingFileID) {
			loadFiles = append(loadFiles, loadFile)
		}
	}
	return loadFiles, nil
}
