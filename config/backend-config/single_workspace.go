package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
)

type SingleWorkspaceConfig struct {
	CommonBackendConfig
	workspaceID string
	Token       string

	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (workspaceConfig *SingleWorkspaceConfig) SetUp() {
	if workspaceConfig.Token == "" {
		workspaceConfig.Token = config.GetWorkspaceToken()
	}
}

func (workspaceConfig *SingleWorkspaceConfig) AccessToken() string {
	return workspaceConfig.Token
}

func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceIDForWriteKey(_ string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceIDForSourceID(_ string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

//GetWorkspaceLibrariesFromWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()
	if workspaceConfig.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return workspaceConfig.workspaceIDToLibrariesMap[workspaceID]
}

//Get returns sources from the workspace
func (workspaceConfig *SingleWorkspaceConfig) Get(workspace string) (ConfigT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI(workspace)
	}
}

// getFromApi gets the workspace config from api
func (workspaceConfig *SingleWorkspaceConfig) getFromAPI(workspace string) (ConfigT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)
	var respBody []byte
	var statusCode int

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url, workspace)
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
		pkgLogger.Error("Error while parsing request", err, statusCode)
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
func (_ *SingleWorkspaceConfig) getFromFile() (ConfigT, bool) {
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

func (_ *SingleWorkspaceConfig) makeHTTPRequest(url string, workspaceToken string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	defer resp.Body.Close()

	respBody, err := IoUtil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, 400, err
	}

	return respBody, resp.StatusCode, nil
}
