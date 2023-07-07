package multitenant

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

var degradedWorkspaceIDs []string

func init() {
	config.RegisterStringSliceConfigVariable(nil, &degradedWorkspaceIDs, false, "Warehouse.degradedWorkspaceIDs")
}

type Manager struct {
	BackendConfig        backendconfig.BackendConfig
	DegradedWorkspaceIDs []string

	sourceIDToWorkspaceID map[string]string
	excludeWorkspaceIDMap map[string]struct{}

	ready     chan struct{}
	sourceMu  sync.Mutex
	readyOnce sync.Once
	initOnce  sync.Once
}

func (m *Manager) init() {
	m.initOnce.Do(func() {
		if m.DegradedWorkspaceIDs == nil {
			m.DegradedWorkspaceIDs = degradedWorkspaceIDs
		}

		m.sourceIDToWorkspaceID = make(map[string]string)
		m.excludeWorkspaceIDMap = make(map[string]struct{})

		for _, workspaceID := range m.DegradedWorkspaceIDs {
			m.excludeWorkspaceIDMap[workspaceID] = struct{}{}
		}
		m.ready = make(chan struct{})
	})
}

// Run is a blocking function that executes manager background logic.
func (m *Manager) Run(ctx context.Context) {
	m.init()

	chIn := m.BackendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range chIn {
		m.sourceMu.Lock()
		config := data.Data.(map[string]backendconfig.ConfigT)
		for workspaceID := range config {
			for _, source := range config[workspaceID].Sources {
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
	m.init()

	_, ok := m.excludeWorkspaceIDMap[workspaceID]
	return ok
}

// DegradedWorkspaces returns a list of degraded workspaceIDs.
func (m *Manager) DegradedWorkspaces() []string {
	m.init()

	return m.DegradedWorkspaceIDs
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

	chIn := m.BackendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)

	chOut := make(chan map[string]backendconfig.ConfigT)

	go func() {
		for data := range chIn {
			input := data.Data.(map[string]backendconfig.ConfigT)
			filteredConfig := make(map[string]backendconfig.ConfigT, len(input))

			for workspaceID := range input {
				if !m.DegradedWorkspace(workspaceID) {
					filteredConfig[workspaceID] = input[workspaceID]
				}
			}

			chOut <- filteredConfig
		}
		close(chOut)
	}()

	return chOut
}
