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

// Provider is an interface that provides file managers and storage preferences for a given workspace.
type Provider interface {
	// Gets a file manager for the given workspace.
	GetFileManager(workspaceID string) (filemanager.FileManager, error)
	// Gets the storage preferences for the given workspace.
	GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error)
}

// NewProvider creates a new provider that updates its storage settings while backend configuration gets updated.
func NewProvider(ctx context.Context, config backendconfig.BackendConfig) Provider {
	s := &provider{
		init:            make(chan struct{}),
		storageSettings: make(map[string]StorageSettings),
	}
	go s.updateLoop(ctx, config)
	return s
}

// NewStaticProvider creates a new provider that operates against a predefined storage settings.
// Useful for tests.
func NewStaticProvider(storageSettings map[string]StorageSettings) Provider {
	s := &provider{
		init:            make(chan struct{}),
		storageSettings: storageSettings,
	}
	close(s.init)
	return s
}

// NewDefaultProvider creates a new provider that operates against the default storage settings populated from the env.
// Useful for tests that populate settings from env.
func NewDefaultProvider() Provider {
	d := &defaultProvider{}
	return d
}

type provider struct {
	onceInit        sync.Once
	init            chan struct{}
	storageSettings map[string]StorageSettings
}

func (p *provider) GetFileManager(workspaceID string) (filemanager.FileManager, error) {
	<-p.init
	settings, ok := p.storageSettings[workspaceID]
	if !ok {
		return nil, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: settings.Bucket.Type,
		Config:   settings.Bucket.Config,
	})
}

func (p *provider) GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error) {
	<-p.init
	var prefs backendconfig.StoragePreferences
	settings, ok := p.storageSettings[workspaceID]
	if !ok {
		return prefs, fmt.Errorf("no storage settings found for workspace: %s", workspaceID)
	}
	return settings.Preferences, nil
}

// updateLoop uses backend config to retrieve & keep up-to-date the storage settings of all workspaces.
func (p *provider) updateLoop(ctx context.Context, backendConfig backendconfig.BackendConfig) {
	ch := backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	settings := make(map[string]StorageSettings)

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		for workspaceId, c := range configs {

			var bucket backendconfig.StorageBucket
			var preferences backendconfig.StoragePreferences

			if c.Settings.DataRetention.UseSelfStorage {
				settings := c.Settings.DataRetention.StorageBucket
				defaultBucket := getDefaultBucket(ctx, settings.Type)
				bucket = overrideWithSettings(defaultBucket.Config, settings, workspaceId)
			} else {
				bucket = getDefaultBucket(ctx, config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
			}
			// bucket type and configuration must not be empty
			if bucket.Type != "" && len(bucket.Config) > 0 {
				preferences = c.Settings.DataRetention.StoragePreferences
			}
			settings[workspaceId] = StorageSettings{
				Bucket:      bucket,
				Preferences: preferences,
			}
		}
		p.storageSettings = settings
		p.onceInit.Do(func() {
			close(p.init)
		})
	}

	p.onceInit.Do(func() {
		close(p.init)
	})
}

type defaultProvider struct{}

func (*defaultProvider) GetFileManager(_ string) (filemanager.FileManager, error) {
	defaultConfig := getDefaultBucket(context.Background(), config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
	return filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: defaultConfig.Type,
		Config:   defaultConfig.Config,
	})
}

func (*defaultProvider) GetStoragePreferences(_ string) (backendconfig.StoragePreferences, error) {
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
