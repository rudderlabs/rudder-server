package backendconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
)

//MultiWorkspaceConfig is a struct to hold variables necessary for supporting multiple workspaces.
type MultiWorkspaceConfig struct {
	CommonBackendConfig
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceWriteKeysMapLock sync.RWMutex
}

//WorkspacesT holds sources of workspaces
type WorkspacesT struct {
	WorkspaceSourcesMap map[string][]SourceT `json:"-"`
}

//WorkspaceRegulationsT holds regulations of workspaces
type WorkspaceRegulationsT struct {
	WorkspaceRegulationsMap map[string]RegulationsT `json:"-"`
}

//SetUp sets up MultiWorkspaceConfig
func (multiWorkspaceConfig *MultiWorkspaceConfig) SetUp() {
	multiWorkspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)
}

//GetWorkspaceIDForWriteKey returns workspaceID for the given writeKey
func (multiWorkspaceConfig *MultiWorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	multiWorkspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer multiWorkspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := multiWorkspaceConfig.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

//Get returns sources from all hosted workspaces
func (multiWorkspaceConfig *MultiWorkspaceConfig) Get() (SourcesT, bool) {
	url := fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL)
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		log.Error("Error when creating request to the server", err)
		return SourcesT{}, false
	}

	req.SetBasicAuth(multiWorkspaceSecret, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Error when sending request to the server", err)
		return SourcesT{}, false
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}

	writeKeyToWorkspaceIDMap := make(map[string]string)
	sourcesJSON := SourcesT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, sourceArr := range workspaces.WorkspaceSourcesMap {
		for _, source := range sourceArr {
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, sourceArr...)
	}

	multiWorkspaceConfig.workspaceWriteKeysMapLock.Lock()
	multiWorkspaceConfig.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	multiWorkspaceConfig.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, true
}

//GetRegulations returns regulations from all hosted workspaces
func (multiWorkspaceConfig *MultiWorkspaceConfig) GetRegulations() (RegulationsT, bool) {
	url := fmt.Sprintf("%s/hostedRegulations", configBackendURL)
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		log.Error("Error when creating request to the server", err)
		return RegulationsT{}, false
	}

	req.SetBasicAuth(multiWorkspaceSecret, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Error when sending request to the server", err)
		return RegulationsT{}, false
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	var workspaceRegulations WorkspaceRegulationsT
	err = json.Unmarshal(respBody, &workspaceRegulations.WorkspaceRegulationsMap)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
		return RegulationsT{}, false
	}

	regulationsJSON := RegulationsT{}
	regulationsJSON.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	regulationsJSON.SourceRegulations = make([]SourceRegulationT, 0)
	for _, regulationsArr := range workspaceRegulations.WorkspaceRegulationsMap {
		regulationsJSON.WorkspaceRegulations = append(regulationsJSON.WorkspaceRegulations, regulationsArr.WorkspaceRegulations...)
		regulationsJSON.SourceRegulations = append(regulationsJSON.SourceRegulations, regulationsArr.SourceRegulations...)
	}

	return regulationsJSON, true
}
