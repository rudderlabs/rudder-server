package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/distributed"
	"github.com/tidwall/gjson"
)

type MultiTenantWorkspaceConfig struct {
	CommonBackendConfig
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

func (workspaceConfig *MultiTenantWorkspaceConfig) SetUp() {
	workspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)
}

func (workspaceConfig *MultiTenantWorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

//GetWorkspaceLibrariesFromWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *MultiTenantWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := workspaceConfig.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

//Get returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspaceConfig) Get() (ConfigT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI()
	}
}

//GetRegulations returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspaceConfig) GetRegulations() (RegulationsT, bool) {
	if configFromFile {
		return workspaceConfig.getRegulationsFromFile()
	} else {
		return workspaceConfig.getRegulationsFromAPI()
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

// getFromApi gets the workspace config from api
func (workspaceConfig *MultiTenantWorkspaceConfig) getFromAPI() (ConfigT, bool) {
	url := fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=", configBackendURL)
	workspaceIDs := distributed.GetWorkspaceIDs()
	workspacesString := strings.Join(workspaceIDs[:], ",")
	url = url + workspacesString
	// fmt.Println(url)
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
	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Error("Error while parsing request", err, statusCode)
		return ConfigT{}, false
	}
	writeKeyToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, workspaceConfig := range workspaces.WorkspaceSourcesMap {
		for _, source := range workspaceConfig.Sources {
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = workspaceConfig.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, workspaceConfig.Sources...)
	}
	workspaceConfig.workspaceWriteKeysMapLock.Lock()
	workspaceConfig.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	workspaceConfig.workspaceIDToLibrariesMap = make(map[string]LibrariesT)
	workspaceConfig.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	workspaceConfig.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, true
}

func (workspaceConfig *MultiTenantWorkspaceConfig) getRegulationsFromFile() (RegulationsT, bool) {
	regulations := RegulationsT{}
	regulations.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	regulations.SourceRegulations = make([]SourceRegulationT, 0)
	return regulations, true
}

func (workspaceConfig *MultiTenantWorkspaceConfig) getRegulationsFromAPI() (RegulationsT, bool) {
	regulationsJSON := RegulationsT{}
	regulationsJSON.SourceRegulations = make([]SourceRegulationT, 0)
	regulationsJSON.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	for _, workspace := range distributed.GetWorkspaceIDs()[0:1] {
		wregulations, status := workspaceConfig.getWorkspaceRegulationsFromAPI(workspace)
		if !status {
			return RegulationsT{}, false
		}
		regulationsJSON.WorkspaceRegulations = append(regulationsJSON.WorkspaceRegulations, wregulations...)

		var sregulations []SourceRegulationT
		sregulations, status = workspaceConfig.getSourceRegulationsFromAPI(workspace)
		if !status {
			return RegulationsT{}, false
		}
		regulationsJSON.SourceRegulations = append(regulationsJSON.SourceRegulations, sregulations...)
	}

	return regulationsJSON, true
}

func (workspaceConfig *MultiTenantWorkspaceConfig) getWorkspaceRegulationsFromAPI(workspace string) ([]WorkspaceRegulationT, bool) {
	start := 0

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequestWithWorkspaceID(url, workspace)
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
			pkgLogger.Error("Error while parsing request", err, statusCode)
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

func (workspaceConfig *MultiTenantWorkspaceConfig) getSourceRegulationsFromAPI(workspace string) ([]SourceRegulationT, bool) {
	start := 0

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/sources/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequestWithWorkspaceID(url, workspace)
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
			pkgLogger.Error("Error while parsing request", err, statusCode)
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

func (workspaceConfig *MultiTenantWorkspaceConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(multitenantWorkspaceSecret, "")
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

func (workspaceConfig *MultiTenantWorkspaceConfig) makeHTTPRequestWithWorkspaceID(url string, workspaceID string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceID, "")
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
