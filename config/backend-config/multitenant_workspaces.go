package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// WorkspacesT holds sources of workspaces
type WorkspacesT struct {
	WorkspaceSourcesMap map[string]ConfigT `json:"-"`
}

type MultiTenantWorkspacesConfig struct { // TODO unexport all these
	Token                     string
	configBackendURL          string
	configEnvHandler          types.ConfigEnvI
	writeKeyToWorkspaceIDMap  map[string]string
	sourceToWorkspaceIDMap    map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

func (wc *MultiTenantWorkspacesConfig) SetUp() error {
	wc.writeKeyToWorkspaceIDMap = make(map[string]string)
	if wc.Token == "" {
		wc.Token = config.GetEnv("HOSTED_MULTITENANT_SERVICE_SECRET", "")
	}
	if wc.Token == "" {
		return fmt.Errorf("multi tenant workspace: empty workspace config token")
	}
	return nil
}

func (wc *MultiTenantWorkspacesConfig) AccessToken() string {
	return wc.Token
}

func (wc *MultiTenantWorkspacesConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := wc.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (wc *MultiTenantWorkspacesConfig) GetWorkspaceIDForSourceID(source string) string {
	// TODO use another map later
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := wc.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (wc *MultiTenantWorkspacesConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := wc.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (wc *MultiTenantWorkspacesConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return wc.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (wc *MultiTenantWorkspacesConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	var (
		url        string
		respBody   []byte
		statusCode int
	)
	if config.GetBool("BackendConfig.cachedHostedWorkspaceConfig", false) {
		url = fmt.Sprintf("%s/cachedHostedWorkspaceConfig", wc.configBackendURL)
	} else {
		url = fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", wc.configBackendURL)
	}
	operation := func() error {
		var fetchError error
		pkgLogger.Debugf("Fetching config from %s", url)
		respBody, statusCode, fetchError = wc.makeHTTPRequest(ctx, url)
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
	configEnvHandler := wc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, fmt.Errorf("invalid response from backend config: %v", err)
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
	wc.workspaceWriteKeysMapLock.Lock()
	wc.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	wc.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	wc.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	wc.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, nil
}

func (wc *MultiTenantWorkspacesConfig) makeHTTPRequest(
	ctx context.Context, url string,
) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return []byte{}, 400, err
	}
	// TODO: hacky way to get the backend config for multi tenant through older hosted backend config
	if config.GetBool("BackendConfig.useHostedBackendConfig", false) {
		req.SetBasicAuth(config.GetEnv("HOSTED_SERVICE_SECRET", ""), "")
	} else {
		req.SetBasicAuth(wc.Token, "")
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, resp.StatusCode, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, resp.StatusCode, nil
}
