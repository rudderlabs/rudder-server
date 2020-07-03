package backendconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	url := fmt.Sprintf("%s/hostedWorkspaces", configBackendURL)
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

	var hostedWorkspaces HostedWorkspacesT
	err = json.Unmarshal(respBody, &hostedWorkspaces)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
		return RegulationsT{}, false
	}

	regulationsJSON := RegulationsT{}
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
	limit := 10

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/hostedWorkspaceRegulations?workspaceId=%s&offset=%d&limit=%d", configBackendURL, workspaceID, offset, limit)
		req, err := Http.NewRequest("GET", url, nil)
		if err != nil {
			log.Error("Error when creating request", err)
			return []WorkspaceRegulationT{}, false
		}

		req.SetBasicAuth(multiWorkspaceSecret, "")
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Error("Error when sending request to the server", err)
			return []WorkspaceRegulationT{}, false
		}

		var respBody []byte
		if resp != nil && resp.Body != nil {
			respBody, _ = IoUtil.ReadAll(resp.Body)
			defer resp.Body.Close()
		}

		var workspaceRegulationsJSON WRegulationsT
		err = json.Unmarshal(respBody, &workspaceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
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
	limit := 10

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/hostedSourceRegulations?workspaceId=%s&offset=%d&limit=%d", configBackendURL, workspaceID, offset, limit)
		req, err := Http.NewRequest("GET", url, nil)
		if err != nil {
			log.Error("Error when creating request", err)
			return []SourceRegulationT{}, false
		}

		req.SetBasicAuth(multiWorkspaceSecret, "")
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Error("Error when sending request to the server", err)
			return []SourceRegulationT{}, false
		}

		var respBody []byte
		if resp != nil && resp.Body != nil {
			respBody, _ = IoUtil.ReadAll(resp.Body)
			defer resp.Body.Close()
		}

		var sourceRegulationsJSON SRegulationsT
		err = json.Unmarshal(respBody, &sourceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
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
