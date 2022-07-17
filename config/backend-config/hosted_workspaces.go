package backendconfig

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
)

// HostedWorkspacesConfig is a struct to hold variables necessary for supporting multiple workspaces.
type HostedWorkspacesConfig struct {
	CommonBackendConfig
	Token                     string
	writeKeyToWorkspaceIDMap  map[string]string
	sourceIDToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceWriteKeysMapLock sync.RWMutex
}

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// WorkspacesT holds sources of workspaces
type WorkspacesT struct {
	WorkspaceSourcesMap map[string]ConfigT `json:"-"`
}

// SetUp sets up MultiWorkspaceConfig
func (multiWorkspaceConfig *HostedWorkspacesConfig) SetUp() {
	multiWorkspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)
	if multiWorkspaceConfig.Token == "" {
		multiWorkspaceConfig.Token = config.GetEnv("HOSTED_SERVICE_SECRET", "")
	}
}

func (multiWorkspaceConfig *HostedWorkspacesConfig) AccessToken() string {
	return multiWorkspaceConfig.Token
}

// GetWorkspaceIDForWriteKey returns workspaceID for the given writeKey
func (multiWorkspaceConfig *HostedWorkspacesConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	multiWorkspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer multiWorkspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := multiWorkspaceConfig.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceIDForSourceID returns workspaceID for the given writeKey
func (multiWorkspaceConfig *HostedWorkspacesConfig) GetWorkspaceIDForSourceID(sourceID string) string {
	multiWorkspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer multiWorkspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := multiWorkspaceConfig.sourceIDToWorkspaceIDMap[sourceID]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (multiWorkspaceConfig *HostedWorkspacesConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	multiWorkspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer multiWorkspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := multiWorkspaceConfig.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from all hosted workspaces
func (multiWorkspaceConfig *HostedWorkspacesConfig) Get(ctx context.Context, _ string) (ConfigT, error) {
	var url string
	if config.GetBool("BackendConfig.cachedHostedWorkspaceConfig", false) {
		url = fmt.Sprintf("%s/cachedHostedWorkspaceConfig", configBackendURL)
	} else {
		url = fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL)
	}

	var respBody []byte
	var statusCode int

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = multiWorkspaceConfig.makeHTTPRequest(ctx, url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[[ Multi-workspace-config ]] Failed to fetch multi workspace config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server", err)
		return ConfigT{}, newError(true, err)
	}
	var workspaces WorkspacesT
	err = jsonfast.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, newError(true, err)
	}

	writeKeyToWorkspaceIDMap := make(map[string]string)
	sourceIDToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, workspaceConfig := range workspaces.WorkspaceSourcesMap {
		for _, source := range workspaceConfig.Sources {
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			sourceIDToWorkspaceIDMap[source.ID] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = workspaceConfig.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, workspaceConfig.Sources...)
	}
	sourcesJSON.ConnectionFlags.URL = config.GetEnv("CP_ROUTER_URL", "")
	sourcesJSON.ConnectionFlags.Services = map[string]bool{"warehouse": true} // always set connection flags to true for hosted warehouse service
	multiWorkspaceConfig.workspaceWriteKeysMapLock.Lock()
	multiWorkspaceConfig.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	multiWorkspaceConfig.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	multiWorkspaceConfig.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, nil
}

func (multiWorkspaceConfig *HostedWorkspacesConfig) makeHTTPRequest(
	ctx context.Context, url string,
) ([]byte, int, error) {
	req, err := Http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(multiWorkspaceConfig.Token, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, 400, err
	}

	return respBody, resp.StatusCode, nil
}

func (multiWorkspaceConfig *HostedWorkspacesConfig) IsConfigured() bool {
	return multiWorkspaceConfig.AccessToken() != ""
}
