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
	backendConfig        backendconfig.BackendConfig
	degradedWorkspaceIDs *config.Reloadable[[]string]

	sourceIDToWorkspaceID map[string]string
	// fullConfig is the unfiltered snapshot, written only by Run. configVersion changes
	// with every snapshot and degraded-list reload, so subscribers can skip unchanged sends.
	fullConfig    map[string]backendconfig.ConfigT
	configVersion uint64
	configMu      sync.RWMutex

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]struct{}

	ready     chan struct{}
	readyOnce sync.Once
}

func New(conf *config.Config, bcConfig backendconfig.BackendConfig) *Manager {
	m := &Manager{
		backendConfig:         bcConfig,
		degradedWorkspaceIDs:  conf.GetReloadableStringSliceVar(nil, degradedWorkspaceIDsKey),
		sourceIDToWorkspaceID: make(map[string]string),
		subscribers:           make(map[chan struct{}]struct{}),
		ready:                 make(chan struct{}),
	}
	conf.OnReloadableConfigChange(func(key string, _, _ any) {
		if key == degradedWorkspaceIDsKey {
			m.configMu.Lock()
			m.configVersion++
			m.configMu.Unlock()
			m.notifySubscribers()
		}
	})
	return m
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
	chIn := m.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range chIn {
		m.configMu.Lock()
		wConfig := data.Data.(map[string]backendconfig.ConfigT)
		m.fullConfig = wConfig
		m.configVersion++
		for workspaceID := range wConfig {
			for _, source := range wConfig[workspaceID].Sources {
				m.sourceIDToWorkspaceID[source.ID] = workspaceID
			}
		}
		m.configMu.Unlock()
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
func (m *Manager) ConnectionRemoved(workspaceID, sourceID, destinationID, destType string) bool {
	m.configMu.RLock()
	defer m.configMu.RUnlock()

	if m.fullConfig == nil {
		return false
	}
	return !connectionInWorkspace(m.fullConfig[workspaceID], sourceID, destinationID, destType)
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
	select {
	case <-m.ready:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	m.configMu.RLock()
	defer m.configMu.RUnlock()

	workspaceID, ok := m.sourceIDToWorkspaceID[sourceID]
	if !ok {
		return "", fmt.Errorf("sourceID: %s not found", sourceID)
	}

	return workspaceID, nil
}

// WatchConfig returns a channel of backend config maps that exclude degraded workspaces.
// It emits again when the backend config or the degraded list changes, and is closed when ctx is canceled.
func (m *Manager) WatchConfig(ctx context.Context) <-chan map[string]backendconfig.ConfigT {
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
		for {
			select {
			case <-ctx.Done():
				return
			case <-signal:
			}
			m.configMu.RLock()
			input, version := m.fullConfig, m.configVersion
			m.configMu.RUnlock()
			// The startup signal and Run's notification can both observe the same snapshot.
			if input == nil || version == sentVersion {
				continue
			}

			filteredConfig := lo.OmitByKeys(input, m.degradedWorkspaceIDs.Load())
			select {
			case chOut <- filteredConfig:
				sentVersion = version
			case <-ctx.Done():
				return
			}
		}
	}()

	return chOut
}
