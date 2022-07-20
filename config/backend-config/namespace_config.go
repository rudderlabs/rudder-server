package backendconfig

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/config"
)

type NamespaceConfig struct {
	CommonBackendConfig

	workspaceWriteKeysMapLock sync.RWMutex
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	sourceToWorkspaceIDMap    map[string]string

	Client            *http.Client
	BasicAuthUsername string
	BasicAuthPassword string
	Namespace         string
	ConfigBackendURL  string
}

func (workspaceConfig *NamespaceConfig) SetUp() {
	workspaceConfig.writeKeyToWorkspaceIDMap = make(map[string]string)

	if workspaceConfig.Namespace == "" {
		workspaceConfig.Namespace = config.GetEnv("WORKSPACE_NAMESPACE", "")
	}
	if workspaceConfig.BasicAuthUsername == "" {
		workspaceConfig.BasicAuthUsername = config.GetEnv("CONTROL_PLANE_BASIC_AUTH_USERNAME", "")
	}
	if workspaceConfig.BasicAuthPassword == "" {
		workspaceConfig.BasicAuthPassword = config.GetEnv("CONTROL_PLANE_BASIC_AUTH_PASSWORD", "")
	}
	if workspaceConfig.ConfigBackendURL == "" {
		workspaceConfig.ConfigBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	}

	if workspaceConfig.Client == nil {
		workspaceConfig.Client = &http.Client{
			Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second),
		}
	}

}

func (workspaceConfig *NamespaceConfig) AccessToken() string {
	panic("not supported")
}

func (workspaceConfig *NamespaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (workspaceConfig *NamespaceConfig) GetWorkspaceIDForSourceID(source string) string {
	// TODO use another map later
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := workspaceConfig.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *NamespaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceWriteKeysMapLock.RLock()
	defer workspaceConfig.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := workspaceConfig.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (workspaceConfig *NamespaceConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return workspaceConfig.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (workspaceConfig *NamespaceConfig) getFromAPI(
	ctx context.Context, workspaceArr string,
) (ConfigT, error) {
	// added this to avoid unnecessary calls to backend config and log better until workspace IDs are not present
	if workspaceArr == "" {
		return ConfigT{}, newError(false, fmt.Errorf("no workspace IDs provided, skipping backend config fetch"))
	}

	var (
		respBody   []byte
		statusCode int
	)

	u, err := url.Parse(workspaceConfig.ConfigBackendURL)
	if err != nil {
		return ConfigT{}, newError(false, err)
	}
	u.Path = fmt.Sprintf("/dataPlane/v1/namespace/%s/config", workspaceConfig.Namespace)

	operation := func() error {
		var fetchError error
		pkgLogger.Debugf("Fetching config from %s", u.String())
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(ctx, u.String())
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
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
	err = jsonfast.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
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

func (workspaceConfig *NamespaceConfig) makeHTTPRequest(
	ctx context.Context, url string,
) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceConfig.BasicAuthUsername, workspaceConfig.BasicAuthPassword)
	resp, err := workspaceConfig.Client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, 400, err
	}

	defer func() { _ = resp.Body.Close() }()

	return respBody, resp.StatusCode, nil
}

func (workspaceConfig *NamespaceConfig) IsConfigured() bool {
	return false
}
