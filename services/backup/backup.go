package backup

import (
	"context"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

type StorageBucket map[string]backendconfig.StorageBucketT

type StoragePreferences map[string]backendconfig.StoragePreferencesT

type StorageSettings struct {
	StorageBucket      StorageBucket
	StoragePreferences StoragePreferences
}

type StorageService interface {
	StorageSettings() StorageSettings
}

func NewService(ctx context.Context, config backendconfig.BackendConfig) StorageService {
	s := &service{
		init:            make(chan struct{}),
		storageSettings: StorageSettings{},
	}
	go s.updateLoop(ctx, config)
	return s
}

func NewEmptyService() StorageService {
	return NewStaticService(StorageSettings{})
}

func NewStaticService(storageSettings StorageSettings) StorageService {
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
	storageSettings StorageSettings
}

func (s *service) StorageSettings() StorageSettings {
	<-s.init
	return s.storageSettings
}

func (s *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {
	ch := config.Subscribe(ctx, backendconfig.TopicBackendConfig)

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		storageBucket := make(StorageBucket)
		storagePreferences := make(StoragePreferences)
		for _, c := range configs {
			if c.Settings.DataRetention.UseSelfStorage {
				storageBucket[c.WorkspaceID] = c.Settings.DataRetention.StorageBucket
			}
			storagePreferences[c.WorkspaceID] = c.Settings.DataRetention.StoragePreferences
		}
		s.storageSettings = StorageSettings{
			StorageBucket:      storageBucket,
			StoragePreferences: storagePreferences,
		}

		s.onceInit.Do(func() {
			close(s.init)
		})
	}

	s.onceInit.Do(func() {
		close(s.init)
	})
}
