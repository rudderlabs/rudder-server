package fileuploader

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
)

type StorageSettings struct {
	Bucket      backendconfig.StorageBucket
	Preferences backendconfig.StoragePreferences
	updatedAt   time.Time
}

var (
	ErrNoStorageForWorkspace = fmt.Errorf("no storage settings found for workspace")
	ErrNotSubscribed         = fmt.Errorf("provider not subscribed to backend config")
)

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
		storageSettings: make(map[string]StorageSettings),
	}
	go s.updateLoop(ctx, config)
	return s
}

// NewStaticProvider creates a new provider that operates against a predefined storage settings.
// Useful for tests.
func NewStaticProvider(storageSettings map[string]StorageSettings) Provider {
	s := &provider{
		storageSettings: storageSettings,
	}
	s.fileManagerMap = make(map[string]func() (filemanager.FileManager, error))
	for workspaceID, settings := range storageSettings {
		if settings.Bucket.Type != "" {
			s.fileManagerMap[workspaceID] = sync.OnceValues(func() (filemanager.FileManager, error) {
				return filemanager.New(&filemanager.Settings{
					Provider: settings.Bucket.Type,
					Config:   settings.Bucket.Config,
				})
			})
		}
	}
	s.subscribed.Store(true)
	return s
}

// NewDefaultProvider creates a new provider that operates against the default storage settings populated from the env.
// Useful for tests that populate settings from env.
func NewDefaultProvider() Provider {
	d := &defaultProvider{}
	return d
}

type provider struct {
	subscribed      atomic.Bool
	mu              sync.RWMutex
	storageSettings map[string]StorageSettings
	fileManagerMap  map[string]func() (filemanager.FileManager, error)
}

func (p *provider) GetFileManager(workspaceID string) (filemanager.FileManager, error) {
	if !p.subscribed.Load() {
		return nil, ErrNotSubscribed
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	fileManager, ok := p.fileManagerMap[workspaceID]
	if !ok {
		return nil, ErrNoStorageForWorkspace
	}
	return fileManager()
}

func (p *provider) GetStoragePreferences(workspaceID string) (backendconfig.StoragePreferences, error) {
	var prefs backendconfig.StoragePreferences
	if !p.subscribed.Load() {
		return prefs, ErrNotSubscribed
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	settings, ok := p.storageSettings[workspaceID]
	if !ok {
		return prefs, ErrNoStorageForWorkspace
	}
	return settings.Preferences, nil
}

// updateLoop uses backend config to retrieve & keep up-to-date the storage settings of all workspaces.
func (p *provider) updateLoop(ctx context.Context, backendConfig backendconfig.BackendConfig) {
	ch := backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	for ev := range ch {
		p.mu.RLock()
		currentSettingsMap := lo.Assign(p.storageSettings)
		currentFileManagerMap := lo.Assign(p.fileManagerMap)
		p.mu.RUnlock()
		settingsMap := make(map[string]StorageSettings)
		filemanagerMap := make(map[string]func() (filemanager.FileManager, error))
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		for workspaceId, c := range configs {
			currentWorkspaceConfig, ok := currentSettingsMap[workspaceId]
			// no change in workspace config, don't process the same config again
			if ok && !c.UpdatedAt.After(currentWorkspaceConfig.updatedAt) {
				settingsMap[workspaceId] = currentWorkspaceConfig
				filemanagerMap[workspaceId] = currentFileManagerMap[workspaceId]
				continue
			}

			var bucket backendconfig.StorageBucket
			var preferences backendconfig.StoragePreferences

			if c.Settings.DataRetention.UseSelfStorage {
				settings := c.Settings.DataRetention.StorageBucket
				defaultBucket := getDefaultBucket(ctx, settings.Type)
				bucket = overrideWithSettings(defaultBucket.Config, settings, workspaceId)
				if bucket.Type == "" {
					delete(settingsMap, workspaceId)
					delete(filemanagerMap, workspaceId)
					continue
				}
			} else {
				bucket = getDefaultBucket(ctx, config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
				switch c.Settings.DataRetention.RetentionPeriod {
				case "default":
					bucket.Config["prefix"] = config.GetString("JOBS_BACKUP_DEFAULT_PREFIX", "7dayretention")
				case "full":
				default:
				}
			}
			// bucket type and configuration must not be empty
			if bucket.Type != "" && len(bucket.Config) > 0 {
				preferences = c.Settings.DataRetention.StoragePreferences
			}

			// if no change in storage configuration, don't create new Filemanager
			if ok && reflect.DeepEqual(currentWorkspaceConfig.Bucket, bucket) && reflect.DeepEqual(currentWorkspaceConfig.Preferences, preferences) {
				settingsMap[workspaceId] = currentWorkspaceConfig
				filemanagerMap[workspaceId] = currentFileManagerMap[workspaceId]
				continue
			}

			// either newly polled workspace settings or updated storage config - update object storage Filemanager
			settingsMap[workspaceId] = StorageSettings{
				Bucket:      bucket,
				Preferences: preferences,
				updatedAt:   time.Now(),
			}
			filemanagerMap[workspaceId] = sync.OnceValues(func() (filemanager.FileManager, error) {
				return filemanager.New(&filemanager.Settings{
					Provider: bucket.Type,
					Config:   bucket.Config,
				})
			})
		}
		p.mu.Lock()
		p.storageSettings = settingsMap
		p.fileManagerMap = filemanagerMap
		p.mu.Unlock()
		p.subscribed.Store(true)
	}
	p.subscribed.Store(false)
}

type defaultProvider struct{}

func (*defaultProvider) GetFileManager(string) (filemanager.FileManager, error) {
	defaultConfig := getDefaultBucket(context.Background(), config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"))
	return filemanager.New(&filemanager.Settings{
		Provider: defaultConfig.Type,
		Config:   defaultConfig.Config,
	})
}

func (*defaultProvider) GetStoragePreferences(string) (backendconfig.StoragePreferences, error) {
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
		Config: filemanager.GetProviderConfigFromEnv(filemanagerutil.ProviderConfigOpts(ctx, provider, config.Default)),
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
	if settings.Type == "S3" && config["iamRoleArn"] != nil {
		config["externalID"] = workspaceID
	}
	return backendconfig.StorageBucket{
		Type:   settings.Type,
		Config: config,
	}
}
