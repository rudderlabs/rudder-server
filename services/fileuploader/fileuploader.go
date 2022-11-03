package fileuploader

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

type StorageSettings struct {
	Bucket      backendconfig.StorageBucket
	Preferences backendconfig.StoragePreferences
}

type FileUploader interface {
	// Gets a file manager for the given workspace.
	GetFileUploader(workspaceID string) (filemanager.FileManager, error)
	// Gets the storage preferences for the given workspace.
	GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error)
}

// NewService creates a new service that updates its storage settings while backend configuration gets updated.
func NewService(ctx context.Context, config backendconfig.BackendConfig) FileUploader {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: make(map[string]StorageSettings),
	}
	go s.updateLoop(ctx, config)
	return s
}

// NewStaticService creates a new service that operates against a predefined storage settings.
// Useful for tests.
func NewStaticService(storageSettings map[string]StorageSettings) FileUploader {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: storageSettings,
	}
	close(s.init)
	return s
}

// NewDefaultService creates a new service that operates against the default storage settings populated from the env.
// Useful for tests that populate settings from env.
func NewDefaultService() FileUploader {
	d := &defaultservice{}
	return d
}

type service struct {
	onceInit        sync.Once
	init            chan struct{}
	storageSettings map[string]StorageSettings
}

func (s *service) GetFileUploader(workspaceID string) (filemanager.FileManager, error) {
	<-s.init
	settings, ok := s.storageSettings[workspaceID]
	if !ok {
		return nil, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: settings.Bucket.Type,
		Config:   settings.Bucket.Config,
	})
}

func (s *service) GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error) {
	<-s.init
	var prefs backendconfig.StoragePreferences
	settings, ok := s.storageSettings[workspaceID]
	if !ok {
		return prefs, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return settings.Preferences, nil
}

// updateLoop uses backend config to retrieve & keep up-to-date the storage settings of all workspaces.
func (s *service) updateLoop(ctx context.Context, backendConfig backendconfig.BackendConfig) {
	ch := backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	settings := make(map[string]StorageSettings)

	var bucket backendconfig.StorageBucket
	var preferences backendconfig.StoragePreferences

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		for _, c := range configs {
			if c.Settings.DataRetention.UseSelfStorage {
				settings := c.Settings.DataRetention.StorageBucket
				defaultBucket := getDefaultBucket(ctx, settings.Type)
				bucket = overrideWithSettings(defaultBucket.Config, settings, c.WorkspaceID)
			} else {
				bucket = getDefaultBucket(ctx, config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
			}
			preferences = c.Settings.DataRetention.StoragePreferences
			settings[c.WorkspaceID] = StorageSettings{
				Bucket:      bucket,
				Preferences: preferences,
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
	defaultConfig := getDefaultBucket(context.Background(), config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
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

func getDefaultBucket(ctx context.Context, provider string) backendconfig.StorageBucket {
	return backendconfig.StorageBucket{
		Type:   provider,
		Config: filemanager.GetProviderConfigFromEnv(ctx, provider),
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
