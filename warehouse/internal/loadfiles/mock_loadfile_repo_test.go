package loadfiles_test

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"golang.org/x/exp/slices"
)

type mockLoadFilesRepo struct {
	store []model.LoadFile
}

func (m *mockLoadFilesRepo) Insert(_ context.Context, loadFiles []model.LoadFile) error {
	m.store = append(m.store, loadFiles...)
	return nil
}

func (m *mockLoadFilesRepo) DeleteByStagingFiles(ctx context.Context, stagingFileIDs []int64) error {
	store := make([]model.LoadFile, 0)
	for _, loadFile := range m.store {
		if !slices.Contains(stagingFileIDs, loadFile.StagingFileID) {
			store = append(store, loadFile)
		}
	}
	m.store = store

	return nil
}

func (m *mockLoadFilesRepo) GetByStagingFiles(ctx context.Context, stagingFileIDs []int64) ([]model.LoadFile, error) {
	var loadFiles []model.LoadFile
	for i, loadFile := range m.store {
		if slices.Contains(stagingFileIDs, loadFile.StagingFileID) {
			loadFile.ID = int64(i + 1)
			loadFiles = append(loadFiles, loadFile)
		}
	}
	return loadFiles, nil
}
