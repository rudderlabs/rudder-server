package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
)

type MultiTenantWorkspaceConfig struct {
	CommonBackendConfig
	writeKeyToWorkspaceIDMap  map[string]string
	sourceToWorkspaceIDMap    map[string]string
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

func (workspaceConfig *MultiTenantWorkspaceConfig) GetWorkspaceIDForSourceID(source string) string {
	//TODO use another map later
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.sourceToWorkspaceIDMap[source]; ok {
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
func (workspaceConfig *MultiTenantWorkspaceConfig) Get(workspaces string) (ConfigT, bool) {
	return workspaceConfig.getFromAPI(workspaces)
}

// getFromApi gets the workspace config from api
func (workspaceConfig *MultiTenantWorkspaceConfig) getFromAPI(workspaceArr string) (ConfigT, bool) {
	url := fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL)
	//TODO : Uncomment it and add tests once we have cluster managers ready
	//To support existing multitenant behaviour
	// url := fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=[%s]", configBackendURL, workspaceArr)
	// workspacesString := ""
	// url = url + workspacesString
	// url = url + "&fetchAll=true"
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
	sourceToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, workspaceConfig := range workspaces.WorkspaceSourcesMap {
		for _, source := range workspaceConfig.Sources {
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			sourceToWorkspaceIDMap[source.ID] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = workspaceConfig.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, workspaceConfig.Sources...)
	}
	workspaceConfig.workspaceWriteKeysMapLock.Lock()
	workspaceConfig.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	workspaceConfig.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	workspaceConfig.workspaceIDToLibrariesMap = make(map[string]LibrariesT)
	workspaceConfig.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	workspaceConfig.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, true
}

func (workspaceConfig *MultiTenantWorkspaceConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}
	//TODO : Uncomment it and add tests once we have cluster managers ready
	// req.SetBasicAuth(multitenantWorkspaceSecret, "")
	req.SetBasicAuth(multiWorkspaceSecret, "")
	//To support existing multitenant behaviour

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
