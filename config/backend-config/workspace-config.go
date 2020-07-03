package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
)

type WorkspaceConfig struct {
	CommonBackendConfig
	workspaceID     string
	workspaceIDLock sync.RWMutex
}

func (workspaceConfig *WorkspaceConfig) SetUp() {
}

func (workspaceConfig *WorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

//Get returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) Get() (SourcesT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI()
	}
}

//GetRegulations returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) GetRegulations() (RegulationsT, bool) {
	if configFromFile {
		return workspaceConfig.getRegulationsFromFile()
	} else {
		return workspaceConfig.getRegulationsFromAPI()
	}
}

// getFromApi gets the workspace config from api
func (workspaceConfig *WorkspaceConfig) getFromAPI() (SourcesT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		log.Error("Error when creating request", err)
		return SourcesT{}, false
	}

	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Error when sending request to the server", err)
		return SourcesT{}, false
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = IoUtil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	var sourcesJSON SourcesT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}

	workspaceConfig.workspaceIDLock.Lock()
	workspaceConfig.workspaceID = sourcesJSON.WorkspaceID
	workspaceConfig.workspaceIDLock.Unlock()

	return sourcesJSON, true
}

// getFromFile reads the workspace config from JSON file
func (workspaceConfig *WorkspaceConfig) getFromFile() (SourcesT, bool) {
	log.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(configJSONPath)
	if err != nil {
		log.Errorf("Unable to read backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	var configJSON SourcesT
	error := json.Unmarshal(data, &configJSON)
	if error != nil {
		log.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	return configJSON, true
}

func (workspaceConfig *WorkspaceConfig) getRegulationsFromAPI() (RegulationsT, bool) {
	regulationsJSON := RegulationsT{}
	wregulations, status := workspaceConfig.getWorkspaceRegulationsFromAPI()
	if !status {
		return RegulationsT{}, false
	}
	regulationsJSON.WorkspaceRegulations = wregulations

	var sregulations []SourceRegulationT
	sregulations, status = workspaceConfig.getSourceRegulationsFromAPI()
	if !status {
		return RegulationsT{}, false
	}
	regulationsJSON.SourceRegulations = sregulations

	return regulationsJSON, true
}

func (workspaceConfig *WorkspaceConfig) getWorkspaceRegulationsFromAPI() ([]WorkspaceRegulationT, bool) {
	offset := 0
	limit := 10

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaceRegulations?offset=%d&limit=%d", configBackendURL, offset, limit)
		req, err := Http.NewRequest("GET", url, nil)
		if err != nil {
			log.Error("Error when creating request", err)
			return []WorkspaceRegulationT{}, false
		}

		req.SetBasicAuth(workspaceToken, "")
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

func (workspaceConfig *WorkspaceConfig) getSourceRegulationsFromAPI() ([]SourceRegulationT, bool) {
	offset := 0
	limit := 10

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/sourceRegulations?offset=%d&limit=%d", configBackendURL, offset, limit)
		req, err := Http.NewRequest("GET", url, nil)
		if err != nil {
			log.Error("Error when creating request", err)
			return []SourceRegulationT{}, false
		}

		req.SetBasicAuth(workspaceToken, "")
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

func (workspaceConfig *WorkspaceConfig) getRegulationsFromFile() (RegulationsT, bool) {
	return RegulationsT{}, false
}
