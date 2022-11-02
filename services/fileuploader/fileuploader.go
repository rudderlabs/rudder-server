package fileuploader

import (
	"context"
	"fmt"
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

func NewStaticService(storageSettings map[string]storageSettings) FileUploader {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: storageSettings,
	}
	close(s.init)
	return s
}

func NewDefaultService() FileUploader {
	d := &defaultservice{}
	return d
}

type service struct {
	onceInit        sync.Once
	init            chan struct{}
	storageSettings map[string]storageSettings
}

func (s *service) GetFileUploader(workspaceID string) (filemanager.FileManager, error) {
	<-s.init
	settings, ok := s.storageSettings[workspaceID]
	if !ok {
		return nil, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: settings.bucket.Type,
		Config:   settings.bucket.Config,
	})
}

func (s *service) GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error) {
	<-s.init
	var prefs backendconfig.StoragePreferences
	settings, ok := s.storageSettings[workspaceID]
	if !ok {
		return prefs, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return settings.preferences, nil
}

func (s *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {
	ch := config.Subscribe(ctx, backendconfig.TopicBackendConfig)

	settings := make(map[string]storageSettings)

	var bucket backendconfig.StorageBucket
	var preferences backendconfig.StoragePreferences

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		defaultBucket := getDefaultBucket(ctx)
		for _, c := range configs {
			if c.Settings.DataRetention.UseSelfStorage {
				bucket = overrideWithSettings(defaultBucket.Config, c.Settings.DataRetention.StorageBucket, c.WorkspaceID)
			} else {
				bucket = defaultBucket
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
		s.storageSettings = settings
	}

	s.onceInit.Do(func() {
		close(s.init)
	})
}

type defaultservice struct{}

func (*defaultservice) GetFileUploader(_ string) (filemanager.FileManager, error) {
	defaultConfig := getDefaultBucket(context.Background())
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: defaultConfig.Type,
		Config:   defaultConfig.Config,
	})
}

func (*defaultservice) GetStoragePreferences(_ string) (backendconfig.StoragePreferences, error) {
	return backendconfig.StoragePreferences{
		ProcErrors:       true,
		GatewayDumps:     true,
		ProcErrorDumps:   true,
		RouterDumps:      true,
		BatchRouterDumps: true,
	}, nil
}

func getDefaultBucket(ctx context.Context) backendconfig.StorageBucket {
	return backendconfig.StorageBucket{
		Type:   config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config: filemanager.GetProviderConfigForBackupsFromEnv(ctx),
	}
}

func overrideWithSettings(defaultConfig map[string]interface{}, settings backendconfig.StorageBucket, workspaceID string) backendconfig.StorageBucket {
	config := make(map[string]interface{})
	for k, v := range defaultConfig {
		config[k] = v
	}
	for k, v := range settings.Config {
		config[k] = v
	}
	if _, ok := config["externalId"]; ok && settings.Type == "S3" {
		config["externalId"] = workspaceID
	}
	return backendconfig.StorageBucket{
		Type:   settings.Type,
		Config: config,
	}
}
