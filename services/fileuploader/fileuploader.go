package fileuploader

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

type storageSettings struct {
	bucket      backendconfig.StorageBucket
	preferences backendconfig.StoragePreferences
}

type FileUploader interface {
	GetFileUploader(workspaceID string) (filemanager.FileManager, error)
	GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error)
}

func NewService(ctx context.Context, config backendconfig.BackendConfig) FileUploader {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: make(map[string]storageSettings),
	}
	go s.updateLoop(ctx, config)
	return s
}

func NewDefaultService() FileUploader {
	d := &defaultservice{}
	return d
}

func NewStaticService(storageSettings map[string]storageSettings) FileUploader {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: storageSettings,
	}
	close(s.init)
	return s
}

type service struct {
	onceInit        sync.Once
	init            chan struct{}
	storageSettings map[string]storageSettings
}

type defaultservice struct{}

func (_ *defaultservice) GetFileUploader(_ string) (filemanager.FileManager, error) {
	defaultConfig := getDefaultConfig(context.Background())
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: defaultConfig.Type,
		Config:   defaultConfig.Config,
	})
}

func (_ *defaultservice) GetStoragePreferences(_ string) (backendconfig.StoragePreferences, error) {
	return backendconfig.StoragePreferences{
		ProcErrors:   true,
		GatewayDumps: true,
	}, nil
}

func (s *service) GetFileUploader(workspaceID string) (filemanager.FileManager, error) {
	<-s.init
	settings := s.storageSettings[workspaceID]
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: settings.bucket.Type,
		Config:   settings.bucket.Config,
	})
}

func (s *service) GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error) {
	<-s.init
	return s.storageSettings[workspaceID].preferences, nil
}

func (s *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {
	ch := config.Subscribe(ctx, backendconfig.TopicBackendConfig)

	settings := make(map[string]storageSettings)

	var bucket backendconfig.StorageBucket
	var preferences backendconfig.StoragePreferences

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		for _, c := range configs {
			if c.Settings.DataRetention.UseSelfStorage {
				bucket = c.Settings.DataRetention.StorageBucket
			} else {
				bucket = getDefaultConfig(ctx)
			}
			preferences = c.Settings.DataRetention.StoragePreferences
			settings[c.WorkspaceID] = storageSettings{
				bucket:      bucket,
				preferences: preferences,
			}
		}

		s.onceInit.Do(func() {
			close(s.init)
		})
	}

	s.onceInit.Do(func() {
		close(s.init)
	})
}

func getDefaultConfig(ctx context.Context) backendconfig.StorageBucket {
	return backendconfig.StorageBucket{
		Type:   config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config: filemanager.GetProviderConfigForBackupsFromEnv(ctx),
	}
}
