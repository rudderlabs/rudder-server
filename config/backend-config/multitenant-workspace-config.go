package backendconfig

import (
	"encoding/json"
	"sync"
)

type MultiTenantWorkspaceConfig struct {
	CommonBackendConfig
	workspaceID               string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (workspaceConfig *MultiTenantWorkspaceConfig) SetUp() {
}

func (workspaceConfig *MultiTenantWorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

//GetWorkspaceLibrariesFromWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *MultiTenantWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()
	if workspaceConfig.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return workspaceConfig.workspaceIDToLibrariesMap[workspaceID]
}

//TODO fix this
//Get returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspaceConfig) Get() (ConfigT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromFile()
	}
}

//TODO fix this
//GetRegulations returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspaceConfig) GetRegulations() (RegulationsT, bool) {
	if configFromFile {
		return workspaceConfig.getRegulationsFromFile()
	} else {
		return workspaceConfig.getRegulationsFromFile()
	}
}

// getFromFile reads the workspace config from JSON file
func (workspaceConfig *MultiTenantWorkspaceConfig) getFromFile() (ConfigT, bool) {
	pkgLogger.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(configJSONPath)
	if err != nil {
		pkgLogger.Errorf("Unable to read backend config from file: %s with error : %s", configJSONPath, err.Error())
		return ConfigT{}, false
	}
	var configJSON ConfigT
	error := json.Unmarshal(data, &configJSON)
	if error != nil {
		pkgLogger.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return ConfigT{}, false
	}
	return configJSON, true
}

func (workspaceConfig *MultiTenantWorkspaceConfig) getRegulationsFromFile() (RegulationsT, bool) {
	regulations := RegulationsT{}
	regulations.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	regulations.SourceRegulations = make([]SourceRegulationT, 0)
	return regulations, true
}
