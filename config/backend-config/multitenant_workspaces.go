package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type multiTenantWorkspacesConfig struct {
	Token                     string
	configBackendURL          *url.URL
	configEnvHandler          types.ConfigEnvI
	cpRouterURL               string
	writeKeyToWorkspaceIDMap  map[string]string
	sourceToWorkspaceIDMap    map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

func (wc *multiTenantWorkspacesConfig) SetUp() error {
	wc.writeKeyToWorkspaceIDMap = make(map[string]string)
	if wc.Token == "" {
		wc.Token = config.GetEnv("HOSTED_SERVICE_SECRET", "")
	}
	if wc.Token == "" {
		return fmt.Errorf("multi tenant workspace: empty workspace config token")
	}
	return nil
}

func (wc *multiTenantWorkspacesConfig) AccessToken() string {
	return wc.Token
}

func (wc *multiTenantWorkspacesConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := wc.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (wc *multiTenantWorkspacesConfig) GetWorkspaceIDForSourceID(source string) string {
	// TODO use another map later
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := wc.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (wc *multiTenantWorkspacesConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	wc.workspaceWriteKeysMapLock.RLock()
	defer wc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := wc.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (wc *multiTenantWorkspacesConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return wc.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (wc *multiTenantWorkspacesConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	if wc.configBackendURL == nil {
		return ConfigT{}, fmt.Errorf("multi tenant workspace: config backend url is nil")
	}

	var (
		u        string
		respBody []byte
	)
	if config.GetBool("BackendConfig.cachedHostedWorkspaceConfig", false) {
		u = fmt.Sprintf("%s/cachedHostedWorkspaceConfig", wc.configBackendURL)
	} else {
		u = fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", wc.configBackendURL)
	}
	operation := func() error {
		var fetchError error
		pkgLogger.Debugf("Fetching config from %s", u)
		respBody, fetchError = wc.makeHTTPRequest(ctx, u)
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
		pkgLogger.Errorf("Error while parsing request: %v", err)
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
	sourcesJSON.ConnectionFlags.URL = wc.cpRouterURL
	// always set connection flags to true for hosted and multi-tenant warehouse service
	sourcesJSON.ConnectionFlags.Services = map[string]bool{"warehouse": true}
	wc.workspaceWriteKeysMapLock.Lock()
	wc.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	wc.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	wc.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	wc.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, nil
}

func (wc *multiTenantWorkspacesConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	// TODO: hacky way to get the backend config for multi tenant through older hosted backend config
	if config.GetBool("BackendConfig.useHostedBackendConfig", false) {
		req.SetBasicAuth(config.GetEnv("HOSTED_SERVICE_SECRET", ""), "")
	} else {
		req.SetBasicAuth(wc.Token, "")
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.backendConfig.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, nil
}
