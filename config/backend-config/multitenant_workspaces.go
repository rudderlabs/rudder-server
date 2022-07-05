package backendconfig

import (
	"context"
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
func (workspaceConfig *MultiTenantWorkspacesConfig) Get(ctx context.Context, workspaces string) (ConfigT, *Error) {
	return workspaceConfig.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (workspaceConfig *MultiTenantWorkspacesConfig) getFromAPI(
	ctx context.Context, workspaceArr string,
) (ConfigT, *Error) {
	// added this to avoid unnecessary calls to backend config and log better until workspace IDs are not present
	if workspaceArr == workspaceConfig.Token {
		return ConfigT{}, newError(false, fmt.Errorf("no workspace IDs provided, skipping backend config fetch"))
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
		var (
			rawWorkspaceIDs = strings.Split(workspaceArr, ",")
			wIds            = make([]string, 0, len(rawWorkspaceIDs))
		)
		for _, wId := range rawWorkspaceIDs {
			trimmed := strings.Trim(wId, " ")
			if trimmed == "" {
				pkgLogger.Warn("empty workspace ID provided")
				continue
			}
			wIds = append(wIds, trimmed)
		}
		if len(wIds) == 0 {
			return ConfigT{}, newError(false, fmt.Errorf("no workspace IDs provided, skipping backend config fetch"))
		}

		encodedWorkspaces, err := jsonfast.MarshalToString(wIds)
		if err != nil {
			return ConfigT{}, newError(false, fmt.Errorf("could not marshal workspace IDs, skipping backend config fetch"))
		}
		url = fmt.Sprintf("%s/multitenantWorkspaceConfig?workspaceIds=%s", configBackendURL, encodedWorkspaces)
		url = url + "&fetchAll=true"
	}
	var respBody []byte
	var statusCode int

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
		return ConfigT{}, newError(true, err)
	}
	configEnvHandler := workspaceConfig.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspaces WorkspacesT
	err = json.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, newError(true, err)
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
	req, err := Http.NewRequestWithContext(ctx, "GET", url, nil)
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
		defer func() { _ = resp.Body.Close() }()
	}

	return respBody, resp.StatusCode, nil
}

func (workspaceConfig *MultiTenantWorkspacesConfig) IsConfigured() bool {
	return workspaceConfig.Token != ""
}
