package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// WorkspacesT holds sources of workspaces
type WorkspacesT struct {
	WorkspaceSourcesMap map[string]ConfigT `json:"-"`
}

type MultiTenantWorkspacesConfig struct {
	CommonBackendConfig
	Token                     string
	writeKeyToWorkspaceIDMap  map[string]string
	sourceToWorkspaceIDMap    map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

func (workspaceConfig *MultiTenantWorkspacesConfig) SetUp() error {
	workspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)
	if workspaceConfig.Token == "" {
		workspaceConfig.Token = config.GetEnv("HOSTED_MULTITENANT_SERVICE_SECRET", "")
	}
	if workspaceConfig.Token == "" {
		return fmt.Errorf("multi tenant workspace: empty workspace config token")
	}
	return nil
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

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *MultiTenantWorkspacesConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := workspaceConfig.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (workspaceConfig *MultiTenantWorkspacesConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return workspaceConfig.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (workspaceConfig *MultiTenantWorkspacesConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	var (
		url        string
		respBody   []byte
		statusCode int
	)
	if config.GetBool("BackendConfig.cachedHostedWorkspaceConfig", false) {
		url = fmt.Sprintf("%s/cachedHostedWorkspaceConfig", configBackendURL)
	} else {
		url = fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL)
	}
	operation := func() error {
		var fetchError error
		pkgLogger.Debugf("Fetching config from %s", url)
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(ctx, url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Errorf("Error sending request to the server: %v", err)
		return ConfigT{}, err
	}
	configEnvHandler := workspaceConfig.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, err
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

	return sourcesJSON, nil
}

func (workspaceConfig *MultiTenantWorkspacesConfig) makeHTTPRequest(
	ctx context.Context, url string,
) ([]byte, int, error) {
	req, err := Http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return []byte{}, 400, err
	}
	// TODO: hacky way to get the backend config for multi tenant through older hosted backend config
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
		defer func() { _ = resp.Body.Close() }()
	}

	return respBody, resp.StatusCode, nil
}
