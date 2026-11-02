package multitenant

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const degradedWorkspaceIDsKey = "Warehouse.degradedWorkspaceIDs"

type Manager struct {
	conf                 *config.Config
	backendConfig        backendconfig.BackendConfig
	degradedWorkspaceIDs *config.Reloadable[[]string]

	sourceIDToWorkspaceID map[string]string
	// fullConfig is updated by each WatchConfig subscription before publishing its
	// filtered snapshot. Run also updates it for SourceToWorkspace callers.
	fullConfig map[string]backendconfig.ConfigT
	sourceMu   sync.RWMutex

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
			if key != degradedWorkspaceIDsKey {
				return
			}
			m.subscribersMu.Lock()
			defer m.subscribersMu.Unlock()
			for ch := range m.subscribers {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		})
	})
}

// Run is a blocking function that executes manager background logic.
func (m *Manager) Run(ctx context.Context) {
	m.init()

	chIn := m.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range chIn {
		m.sourceMu.Lock()
		wConfig := data.Data.(map[string]backendconfig.ConfigT)
		m.fullConfig = wConfig
		for workspaceID := range wConfig {
			for _, source := range wConfig[workspaceID].Sources {
				m.sourceIDToWorkspaceID[source.ID] = workspaceID
			}
		}
		m.sourceMu.Unlock()
		m.readyOnce.Do(func() {
			close(m.ready)
		})
	}
}

// DegradedWorkspace returns true if the workspaceID is degraded.
func (m *Manager) DegradedWorkspace(workspaceID string) bool {
	return slices.Contains(m.degradedWorkspaceIDs.Load(), workspaceID)
}

// DegradedWorkspaces returns a list of degraded workspaceIDs.
func (m *Manager) DegradedWorkspaces() []string {
	return slices.Clone(m.degradedWorkspaceIDs.Load())
}

// HasConnection reports whether the full snapshot contains a connection eligible
// for destType. A missing snapshot is not evidence that a connection was removed.
// Empty workspace IDs on legacy uploads are resolved by source and destination IDs.
func (m *Manager) HasConnection(workspaceID, sourceID, destinationID, destType string) (exists, known bool) {
	m.sourceMu.RLock()
	defer m.sourceMu.RUnlock()

	if m.fullConfig == nil {
		return false, false
	}
	if _, eligible := warehouseutils.PseudoWarehouseDestinationMap[destType]; !eligible {
		return false, true
	}
	if workspaceID != "" {
		return connectionInWorkspace(m.fullConfig[workspaceID], sourceID, destinationID, destType), true
	}
	for _, config := range m.fullConfig {
		if connectionInWorkspace(config, sourceID, destinationID, destType) {
			return true, true
		}
	}
	return false, true
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

	chIn := m.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	chOut := make(chan map[string]backendconfig.ConfigT)
	signal := make(chan struct{}, 1)
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

		var input map[string]backendconfig.ConfigT
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-chIn:
				if !ok {
					chIn = nil // Retain the snapshot for future reloads until the context is canceled.
					continue
				}
				input = data.Data.(map[string]backendconfig.ConfigT)
				// The filtered consumer cannot act on this snapshot until chOut receives it.
				// Publish the full snapshot first, even if Run's subscription is delayed.
				m.sourceMu.Lock()
				m.fullConfig = input
				m.sourceMu.Unlock()
			case <-signal:
				if input == nil {
					continue
				}
			}

			filteredConfig := make(map[string]backendconfig.ConfigT, len(input))
			degraded := m.degradedWorkspaceIDs.Load()
			for workspaceID, cfg := range input {
				if !slices.Contains(degraded, workspaceID) {
					filteredConfig[workspaceID] = cfg
				}
			}
			select {
			case chOut <- filteredConfig:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chOut
}
