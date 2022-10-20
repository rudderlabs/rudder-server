package backup

import (
	"context"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

type StorageSettings map[string]backendconfig.SettingsT

type StorageService interface {
	StorageOverwrites() StorageSettings
}

func NewService(ctx context.Context, config backendconfig.BackendConfig) StorageService {
	s := &service{
		init:    make(chan struct{}),
		storage: make(StorageSettings),
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

func (s *service) StorageOverwrites() StorageSettings {
	<-s.init
	return s.storage
}

func (s *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {
	ch := config.Subscribe(ctx, backendconfig.TopicBackendConfig)

	for ev := range ch {
		configs := ev.Data.(map[string]backendconfig.ConfigT)
		storage := make(StorageSettings)
		for _, c := range configs {
			if c.Settings.DataRetention.UseSelfStorage {
				storage[c.WorkspaceID] = c.Settings
			}
		}
		s.storage = storage

		s.onceInit.Do(func() {
			close(s.init)
		})
	}

	s.onceInit.Do(func() {
		close(s.init)
	})
}
