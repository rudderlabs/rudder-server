package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/config"
)

type MultiTenantWorkspacesConfig struct {
	CommonBackendConfig
	Token                     string
	writeKeyToWorkspaceIDMap  map[string]string
	sourceToWorkspaceIDMap    map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

func (workspaceConfig *MultiTenantWorkspacesConfig) SetUp() {
	workspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)

	if workspaceConfig.Token == "" {
		workspaceConfig.Token = config.GetEnv("HOSTED_MULTITENANT_SERVICE_SECRET", "")
	}
}

func (workspaceConfig *MultiTenantWorkspacesConfig) AccessToken() string {
	return workspaceConfig.Token
}

func (workspaceConfig *MultiTenantWorkspacesConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (workspaceConfig *MultiTenantWorkspacesConfig) GetWorkspaceIDForSourceID(source string) string {
	// TODO use another map later
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesFromWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *MultiTenantWorkspacesConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := workspaceConfig.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspacesConfig) Get(workspaces string) (ConfigT, bool) {
	return workspaceConfig.getFromAPI(workspaces)
}

// getFromApi gets the workspace config from api
func (workspaceConfig *MultiTenantWorkspacesConfig) getFromAPI(workspaceArr string) (ConfigT, bool) {
	// added this to avoid unnecessary calls to backend config and log better until workspace IDs are not present
	if workspaceArr == workspaceConfig.Token {
		pkgLogger.Infof("no workspace IDs provided, skipping backend config fetch")
		return ConfigT{}, false
	}
	var url string
	// TODO: hacky way to get the backend config for multi tenant through older hosted backed config
	if config.GetBool("BackendConfig.useHostedBackendConfig", false) {
		if config.GetBool("BackendConfig.cachedHostedWorkspaceConfig", false) {
			url = fmt.Sprintf("%s/cachedHostedWorkspaceConfig", configBackendURL)
		} else {
			url = fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL)
		}
	} else {
		wIds := strings.Split(workspaceArr, ",")
		for i := range wIds {
			wIds[i] = strings.Trim(wIds[i], " ")
		}
		encodedWorkspaces, err := jsonfast.MarshalToString(wIds)
		if err != nil {
			pkgLogger.Errorf("Error fetching config: preparing request URL: %v", err)
			return ConfigT{}, false
		}
		url = fmt.Sprintf("%s/multitenantWorkspaceConfig?workspaceIds=%s", configBackendURL, encodedWorkspaces)
		url = url + "&fetchAll=true"
	}
	var respBody []byte
	var statusCode int

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
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
	workspaceConfig.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	workspaceConfig.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, true
}

func (workspaceConfig *MultiTenantWorkspacesConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}
	// TODO: hacky way to get the backend config for multi tenant through older hosted backed config
	if config.GetBool("BackendConfig.useHostedBackendConfig", false) {
		req.SetBasicAuth(config.GetEnv("HOSTED_SERVICE_SECRET", ""), "")
	} else {
		req.SetBasicAuth(workspaceConfig.Token, "")
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
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

func (workspaceConfig *MultiTenantWorkspacesConfig) IsConfigured() bool {
	return workspaceConfig.Token != ""
}
