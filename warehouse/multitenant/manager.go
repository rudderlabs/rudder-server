package multitenant

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
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

	sourceMu sync.Mutex
	once     sync.Once
}

func (m *Manager) init() {
	m.once.Do(func() {
		if m.DegradedWorkspaceIDs == nil {
			m.DegradedWorkspaceIDs = degradedWorkspaceIDs
		}

		m.sourceIDToWorkspaceID = make(map[string]string)
		m.excludeWorkspaceIDMap = make(map[string]struct{})

		for _, workspaceID := range m.DegradedWorkspaceIDs {
			m.excludeWorkspaceIDMap[workspaceID] = struct{}{}
		}
	})
}

func (m *Manager) Run(ctx context.Context) error {
	m.init()

	chIn := m.BackendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range chIn {
		m.sourceMu.Lock()
		config := data.Data.(map[string]backendconfig.ConfigT)
		for workspaceID := range config {
			for _, source := range config[workspaceID].Sources {
				m.sourceIDToWorkspaceID[source.SourceDefinition.ID] = workspaceID
			}
		}
		m.sourceMu.Unlock()
	}

	return nil
}

func (m *Manager) DegradedWorkspace(workspaceID string) bool {
	m.init()

	for _, degradedWorkspaceID := range m.DegradedWorkspaceIDs {
		if workspaceID == degradedWorkspaceID {
			return true
		}
	}
	return false
}

func (m *Manager) DegradedWorkspaces() []string {
	m.init()

	return m.DegradedWorkspaceIDs
}

func (m *Manager) SourceToWorkspace(sourceID string) (string, error) {
	m.init()

	m.sourceMu.Lock()
	defer m.sourceMu.Unlock()

	workspaceID, ok := m.sourceIDToWorkspaceID[sourceID]
	if !ok {
		return "", fmt.Errorf("sourceID: %s not found", sourceID)
	}

	return workspaceID, nil
}

func (m *Manager) WatchConfig(ctx context.Context) chan map[string]backendconfig.ConfigT {
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
