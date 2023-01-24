package storage

import (
	"context"
	"fmt"
	"os"
	"path"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Storage struct {
	FileManagerFactory filemanager.FileManagerFactory
	Destination        backendconfig.DestinationT
	ControlPlaneClient loadfiles.ControlPlaneClient
}

func (s *Storage) OpenStagingFile(ctx context.Context, stagingFile model.StagingFile) (*os.File, error) {
	fileManager, err := s.fileManager(stagingFile)
	if err != nil {
		return nil, err
	}

	file, err := s.TmpFile(path.Base(stagingFile.Location))
	if err != nil {
		return nil, fmt.Errorf("tmp file: %w", err)
	}

	err = fileManager.Download(ctx, file, stagingFile.Location)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}

	return os.Open(file.Name())
}

func (s *Storage) UploadStagingFile(ctx context.Context, stagingFile *model.StagingFile, file *os.File) error {
	fileManager, err := s.fileManager(*stagingFile)
	if err != nil {
		return err
	}

	o, err := fileManager.Upload(ctx, file, stagingFile.Location)
	if err != nil {
		return err
	}

	stagingFile.Location, err = fileManager.GetObjectNameFromLocation(o.Location)
	if err != nil {
		return err
	}

	return err
}

func (s *Storage) TmpFile(name string) (*os.File, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, err
	}

	file, err := os.CreateTemp(tmpDirPath, misc.RudderWarehouseJsonUploadsTmp+name)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (s *Storage) fileManager(stagingFile model.StagingFile) (filemanager.FileManager, error) {
	storageProvider := warehouseutils.ObjectStorageType(
		s.Destination.DestinationDefinition.Name,
		s.Destination.Config,
		stagingFile.UseRudderStorage,
	)

	fileManager, err := s.FileManagerFactory.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:                    storageProvider,
			Config:                      s.Destination.Config,
			UseRudderStorage:            stagingFile.UseRudderStorage,
			RudderStoragePrefixOverride: misc.GetRudderObjectStoragePrefix(),
			WorkspaceID:                 stagingFile.WorkspaceID,
		}),
	})
	return fileManager, err
}
