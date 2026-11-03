package multitenant

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

const degradedWorkspaceIDsKey = "Warehouse.degradedWorkspaceIDs"

type Manager struct {
	conf                 *config.Config
	backendConfig        backendconfig.BackendConfig
	degradedWorkspaceIDs *config.Reloadable[[]string]

	sourceIDToWorkspaceID map[string]string
	// fullConfig is the unfiltered snapshot. Run is its only writer, and
	// WatchConfig subscribers filter it, so it is stored before any filtered snapshot.
	fullConfig        map[string]backendconfig.ConfigT
	fullConfigVersion uint64
	sourceMu          sync.RWMutex

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]struct{}

	ready     chan struct{}
	readyOnce sync.Once
	initOnce  sync.Once
}

func New(conf *config.Config, bcConfig backendconfig.BackendConfig) *Manager {
	return &Manager{
		conf:                 conf,
		backendConfig:        bcConfig,
		degradedWorkspaceIDs: conf.GetReloadableStringSliceVar(nil, degradedWorkspaceIDsKey),
	}
}

func (m *Manager) init() {
	m.initOnce.Do(func() {
		m.sourceIDToWorkspaceID = make(map[string]string)
		m.ready = make(chan struct{})
		m.subscribers = make(map[chan struct{}]struct{})
		m.conf.OnReloadableConfigChange(func(key string, _, _ any) {
			if key == degradedWorkspaceIDsKey {
				m.notifySubscribers()
			}
		})
	})
}

func (m *Manager) notifySubscribers() {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()
	for ch := range m.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Run is a blocking function that executes manager background logic.
func (m *Manager) Run(ctx context.Context) {
	m.init()

	chIn := m.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range chIn {
		m.sourceMu.Lock()
		wConfig := data.Data.(map[string]backendconfig.ConfigT)
		m.fullConfig = wConfig
		m.fullConfigVersion++
		for workspaceID := range wConfig {
			for _, source := range wConfig[workspaceID].Sources {
				m.sourceIDToWorkspaceID[source.ID] = workspaceID
			}
		}
		m.sourceMu.Unlock()
		m.readyOnce.Do(func() {
			close(m.ready)
		})
		m.notifySubscribers()
	}
}

// DegradedWorkspace returns true if the workspaceID is degraded.
func (m *Manager) DegradedWorkspace(workspaceID string) bool {
	return slices.Contains(m.degradedWorkspaceIDs.Load(), workspaceID)
}

// DegradedWorkspaces returns a list of degraded workspaceIDs.
func (m *Manager) DegradedWorkspaces() []string {
	return m.degradedWorkspaceIDs.Load()
}

// ConnectionRemoved reports whether the full snapshot lacks the connection for destType.
// A missing snapshot is not evidence that a connection was removed.
// Empty workspace IDs on legacy uploads are resolved by source and destination IDs.
func (m *Manager) ConnectionRemoved(workspaceID, sourceID, destinationID, destType string) bool {
	m.sourceMu.RLock()
	defer m.sourceMu.RUnlock()

	if m.fullConfig == nil {
		return false
	}
	if workspaceID != "" {
		return !connectionInWorkspace(m.fullConfig[workspaceID], sourceID, destinationID, destType)
	}
	for _, config := range m.fullConfig {
		if connectionInWorkspace(config, sourceID, destinationID, destType) {
			return false
		}
	}
	return true
}

func connectionInWorkspace(config backendconfig.ConfigT, sourceID, destinationID, destType string) bool {
	for _, source := range config.Sources {
		if source.ID != sourceID {
			continue
		}
		for _, destination := range source.Destinations {
			if destination.ID == destinationID && destination.DestinationDefinition.Name == destType {
				return true
			}
		}
	}
	return false
}

// SourceToWorkspace returns the workspaceID for a given sourceID, even if workspaceID is degraded.
// An error is returned if the sourceID is not found, or context is canceled.
//
//	NOTE: This function blocks until the backend config is loaded.
func (m *Manager) SourceToWorkspace(ctx context.Context, sourceID string) (string, error) {
	m.init()

	select {
	case <-m.ready:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	m.sourceMu.Lock()
	defer m.sourceMu.Unlock()

	workspaceID, ok := m.sourceIDToWorkspaceID[sourceID]
	if !ok {
		return "", fmt.Errorf("sourceID: %s not found", sourceID)
	}

	return workspaceID, nil
}

// WatchConfig returns a backend config map that excludes degraded workspaces.
//
// NOTE: WatchConfig is responsible for closing the channel when context gets cancel.
func (m *Manager) WatchConfig(ctx context.Context) <-chan map[string]backendconfig.ConfigT {
	m.init()

	chOut := make(chan map[string]backendconfig.ConfigT)
	signal := make(chan struct{}, 1)
	signal <- struct{}{} // emit Run's current snapshot, if any
	m.subscribersMu.Lock()
	m.subscribers[signal] = struct{}{}
	m.subscribersMu.Unlock()

	go func() {
		defer close(chOut)
		defer func() {
			m.subscribersMu.Lock()
			delete(m.subscribers, signal)
			m.subscribersMu.Unlock()
		}()

		var sentVersion uint64
		var sentDegraded []string
		for {
			select {
			case <-ctx.Done():
				return
			case <-signal:
			}
			m.sourceMu.RLock()
			input, version := m.fullConfig, m.fullConfigVersion
			m.sourceMu.RUnlock()
			degraded := m.degradedWorkspaceIDs.Load()
			// The startup signal and Run's notification can both observe the same snapshot.
			if input == nil || (version == sentVersion && slices.Equal(degraded, sentDegraded)) {
				continue
			}

			filteredConfig := lo.OmitByKeys(input, degraded)
			select {
			case chOut <- filteredConfig:
				sentVersion, sentDegraded = version, degraded
			case <-ctx.Done():
				return
			}
		}
	}()

	return chOut
}
