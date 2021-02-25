package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/tidwall/gjson"
)

type WorkspaceConfig struct {
	CommonBackendConfig
	workspaceID               string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (workspaceConfig *WorkspaceConfig) SetUp() {
}

func (workspaceConfig *WorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

//GetWorkspaceLibrariesFromWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *WorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()
	if workspaceConfig.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return workspaceConfig.workspaceIDToLibrariesMap[workspaceID]
}

//Get returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) Get() (ConfigT, bool) {
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
func (workspaceConfig *WorkspaceConfig) getFromAPI() (ConfigT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)

	var respBody []byte
	var statusCode int

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})

	if err != nil {
		pkgLogger.Error("Error sending request to the server", err)
		return ConfigT{}, false
	}

	configEnvHandler := workspaceConfig.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var sourcesJSON ConfigT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		pkgLogger.Error("Error while parsing request", err, string(respBody), statusCode)
		return ConfigT{}, false
	}

	workspaceConfig.workspaceIDLock.Lock()
	workspaceConfig.workspaceID = sourcesJSON.WorkspaceID
	workspaceConfig.workspaceIDToLibrariesMap = make(map[string]LibrariesT)
	workspaceConfig.workspaceIDToLibrariesMap[sourcesJSON.WorkspaceID] = sourcesJSON.Libraries
	workspaceConfig.workspaceIDLock.Unlock()

	return sourcesJSON, true
}

// getFromFile reads the workspace config from JSON file
func (workspaceConfig *WorkspaceConfig) getFromFile() (ConfigT, bool) {
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
	start := 0

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
			return fetchError
		}

		backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
		err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch workspace regulations from API with error: %v, retrying after %v", err, t)
		})

		if err != nil {
			pkgLogger.Error("Error sending request to the server", err)
			return []WorkspaceRegulationT{}, false
		}

		//If statusCode is not 2xx, then returning empty regulations
		if statusCode < 200 || statusCode >= 300 {
			pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch workspace regulations. statusCode: %v, error: %v", statusCode, err)
			return []WorkspaceRegulationT{}, false
		}

		var workspaceRegulationsJSON WRegulationsT
		err = json.Unmarshal(respBody, &workspaceRegulationsJSON)
		if err != nil {
			pkgLogger.Error("Error while parsing request", err, string(respBody), statusCode)
			return []WorkspaceRegulationT{}, false
		}

		endExists := gjson.GetBytes(respBody, "end").Exists()
		if !endExists {
			pkgLogger.Errorf("[[ Workspace-config ]] No end key found in the workspace regulations response. Breaking the regulations fetch loop. Response: %v", string(respBody))
		}
		totalWorkspaceRegulations = append(totalWorkspaceRegulations, workspaceRegulationsJSON.WorkspaceRegulations...)

		if workspaceRegulationsJSON.End || !endExists {
			break
		}

		start = workspaceRegulationsJSON.Next
	}

	return totalWorkspaceRegulations, true
}

func (workspaceConfig *WorkspaceConfig) getSourceRegulationsFromAPI() ([]SourceRegulationT, bool) {
	start := 0

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/sources/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
			return fetchError
		}

		backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
		err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch source regulations from API with error: %v, retrying after %v", err, t)
		})
		if err != nil {
			pkgLogger.Error("Error sending request to the server", err)
			return []SourceRegulationT{}, false
		}

		//If statusCode is not 2xx, then returning empty regulations
		if statusCode < 200 || statusCode >= 300 {
			pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch source regulations. statusCode: %v, error: %v", statusCode, err)
			return []SourceRegulationT{}, false
		}

		var sourceRegulationsJSON SRegulationsT
		err = json.Unmarshal(respBody, &sourceRegulationsJSON)
		if err != nil {
			pkgLogger.Error("Error while parsing request", err, string(respBody), statusCode)
			return []SourceRegulationT{}, false
		}

		endExists := gjson.GetBytes(respBody, "end").Exists()
		if !endExists {
			pkgLogger.Errorf("[[ Workspace-config ]] No end key found in the source regulations response. Breaking the regulations fetch loop. Response: %v", string(respBody))
		}
		totalSourceRegulations = append(totalSourceRegulations, sourceRegulationsJSON.SourceRegulations...)

		if sourceRegulationsJSON.End || !endExists {
			break
		}

		start = sourceRegulationsJSON.Next
	}

	return totalSourceRegulations, true
}

func (workspaceConfig *WorkspaceConfig) getRegulationsFromFile() (RegulationsT, bool) {
	regulations := RegulationsT{}
	regulations.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	regulations.SourceRegulations = make([]SourceRegulationT, 0)
	return regulations, true
}

func (workspaceConfig *WorkspaceConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceToken, "")
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
