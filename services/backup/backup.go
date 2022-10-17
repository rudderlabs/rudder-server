package transientsource

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

type StorageSettings map[string]backendconfig.SettingsT

type StorageService interface {
	StorageSupplier() func() StorageSettings
}

func NewService(ctx context.Context, config backendconfig.BackendConfig) StorageService {
	s := &service{
		init: make(chan struct{}),
	}
	go s.updateLoop(ctx, config)
	return s
}

func NewEmptyService() StorageService {
	return NewStaticService(StorageSettings{})
}

func NewStaticService(storage StorageSettings) StorageService {
	s := &service{
		init:    make(chan struct{}),
		storage: storage,
	}
	close(s.init)
	return s
}

type service struct {
	onceInit sync.Once
	init     chan struct{}
	storage  StorageSettings
}

func (s *service) StorageSupplier() func() StorageSettings {
	<-s.init
	return func() StorageSettings {
		return s.storage
	}
}

func (s *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {
	ch := config.Subscribe(ctx, backendconfig.TopicBackendConfig)

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)

		for _, c := range configs {
			s.storage[c.WorkspaceID] = getStorage(ctx, c.Settings)
		}

		s.onceInit.Do(func() {
			close(s.init)
		})
	}

	s.onceInit.Do(func() {
		close(s.init)
	})
}

// TODO: get default values from env if useSelfStorage is false
func getStorage(ctx context.Context, settings backendconfig.SettingsT) backendconfig.SettingsT {
	if settings.DataRetention.UseSelfStorage {
		return settings
	}

	return backendconfig.SettingsT{
		DataRetention: backendconfig.DataRetentionT{
			UseSelfStorage: false,
			StorageBucket: backendconfig.StorageBucketT{
				Type:   config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
				Config: filemanager.GetProviderConfigForBackupsFromEnv(ctx),
			},
			StoragePreferences: backendconfig.StoragePreferencesT{
				ProcErrors:   true,
				GatewayDumps: true,
			},
		},
	}
}
