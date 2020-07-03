package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
type WorkspaceT struct {
	WorkspaceID string `json:"id"`
}

type HostedWorkspacesT struct {
	HostedWorkspaces []WorkspaceT `json:"workspaces"`
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

	respBody, statusCode, err := multiWorkspaceConfig.makeHTTPRequest(url)
	if err != nil {
		log.Error("Error sending request to the server", err)
		return SourcesT{}, false
	}
	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), statusCode)
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
	url := fmt.Sprintf("%s/hostedWorkspaces", configBackendURL)
	respBody, statusCode, err := multiWorkspaceConfig.makeHTTPRequest(url)
	if err != nil {
		log.Error("Error sending request to the server", err)
		return RegulationsT{}, false
	}

	var hostedWorkspaces HostedWorkspacesT
	err = json.Unmarshal(respBody, &hostedWorkspaces)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), statusCode)
		return RegulationsT{}, false
	}

	regulationsJSON := RegulationsT{}
	regulationsJSON.SourceRegulations = make([]SourceRegulationT, 0)
	regulationsJSON.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	for _, workspace := range hostedWorkspaces.HostedWorkspaces {
		wregulations, status := multiWorkspaceConfig.getWorkspaceRegulations(workspace.WorkspaceID)
		if !status {
			return RegulationsT{}, false
		}
		regulationsJSON.WorkspaceRegulations = append(regulationsJSON.WorkspaceRegulations, wregulations...)

		var sregulations []SourceRegulationT
		sregulations, status = multiWorkspaceConfig.getSourceRegulations(workspace.WorkspaceID)
		if !status {
			return RegulationsT{}, false
		}
		regulationsJSON.SourceRegulations = append(regulationsJSON.SourceRegulations, sregulations...)
	}

	return regulationsJSON, true
}

func (multiWorkspaceConfig *MultiWorkspaceConfig) getWorkspaceRegulations(workspaceID string) ([]WorkspaceRegulationT, bool) {
	offset := 0

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/hostedWorkspaceRegulations?workspaceId=%s&offset=%d&limit=%d", configBackendURL, workspaceID, offset, maxRegulationsPerRequest)
		respBody, statusCode, err := multiWorkspaceConfig.makeHTTPRequest(url)
		if err != nil {
			log.Error("Error sending request to the server", err)
			return []WorkspaceRegulationT{}, false
		}

		var workspaceRegulationsJSON WRegulationsT
		err = json.Unmarshal(respBody, &workspaceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), statusCode)
			return []WorkspaceRegulationT{}, false
		}

		totalWorkspaceRegulations = append(totalWorkspaceRegulations, workspaceRegulationsJSON.WorkspaceRegulations...)

		if workspaceRegulationsJSON.End {
			break
		}

		if value, err := strconv.Atoi(workspaceRegulationsJSON.Next); err == nil {
			offset = value
		} else {
			return []WorkspaceRegulationT{}, false
		}
	}

	return totalWorkspaceRegulations, true
}

func (multiWorkspaceConfig *MultiWorkspaceConfig) getSourceRegulations(workspaceID string) ([]SourceRegulationT, bool) {
	offset := 0

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/hostedSourceRegulations?workspaceId=%s&offset=%d&limit=%d", configBackendURL, workspaceID, offset, maxRegulationsPerRequest)
		respBody, statusCode, err := multiWorkspaceConfig.makeHTTPRequest(url)
		if err != nil {
			log.Error("Error sending request to the server", err)
			return []SourceRegulationT{}, false
		}

		var sourceRegulationsJSON SRegulationsT
		err = json.Unmarshal(respBody, &sourceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), statusCode)
			return []SourceRegulationT{}, false
		}

		totalSourceRegulations = append(totalSourceRegulations, sourceRegulationsJSON.SourceRegulations...)

		if sourceRegulationsJSON.End {
			break
		}

		if value, err := strconv.Atoi(sourceRegulationsJSON.Next); err == nil {
			offset = value
		} else {
			return []SourceRegulationT{}, false
		}
	}

	return totalSourceRegulations, true
}

func (multiWorkspaceConfig *MultiWorkspaceConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(multiWorkspaceSecret, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = IoUtil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	return respBody, resp.StatusCode, nil
}
