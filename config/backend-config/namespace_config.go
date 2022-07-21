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
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type NamespaceConfig struct {
	CommonBackendConfig

	workspaceWriteKeysMapLock sync.RWMutex
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	sourceToWorkspaceIDMap    map[string]string

	Logger            logger.LoggerI
	Client            *http.Client
	BasicAuthUsername string
	BasicAuthPassword string
	Namespace         string
	ConfigBackendURL  string
}

func (nc *NamespaceConfig) SetUp() {
	nc.writeKeyToWorkspaceIDMap = make(map[string]string)

	if nc.Namespace == "" {
		nc.Namespace = config.GetEnv("WORKSPACE_NAMESPACE", "")
	}
	if nc.BasicAuthUsername == "" {
		nc.BasicAuthUsername = config.GetEnv("CONTROL_PLANE_BASIC_AUTH_USERNAME", "")
	}
	if nc.BasicAuthPassword == "" {
		nc.BasicAuthPassword = config.GetEnv("CONTROL_PLANE_BASIC_AUTH_PASSWORD", "")
	}
	if nc.ConfigBackendURL == "" {
		nc.ConfigBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	}

	if nc.Client == nil {
		nc.Client = &http.Client{
			Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second),
		}
	}

	if nc.Logger == nil {
		nc.Logger = logger.NewLogger().Child("backend-config")
	}

}

func (nc *NamespaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	nc.workspaceWriteKeysMapLock.RLock()
	defer nc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := nc.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (nc *NamespaceConfig) GetWorkspaceIDForSourceID(source string) string {
	// TODO use another map later
	nc.workspaceWriteKeysMapLock.RLock()
	defer nc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceID, ok := nc.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (nc *NamespaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	nc.workspaceWriteKeysMapLock.RLock()
	defer nc.workspaceWriteKeysMapLock.RUnlock()

	if workspaceLibraries, ok := nc.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (nc *NamespaceConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return nc.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (nc *NamespaceConfig) getFromAPI(
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

	u, err := url.Parse(nc.ConfigBackendURL)
	if err != nil {
		return ConfigT{}, newError(false, err)
	}
	u.Path = fmt.Sprintf("/dataPlane/v1/namespace/%s/config", nc.Namespace)

	operation := func() error {
		var fetchError error
		nc.Logger.Debugf("Fetching config from %s", u.String())
		respBody, statusCode, fetchError = nc.makeHTTPRequest(ctx, u.String())
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		nc.Logger.Errorf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		nc.Logger.Errorf("Error sending request to the server: %v", err)
		return ConfigT{}, newError(true, err)
	}
	configEnvHandler := nc.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}
	if statusCode != http.StatusOK {
		return ConfigT{}, newError(true, fmt.Errorf("unexpected status code: %d", statusCode))
	}

	var workspaces WorkspacesT
	err = jsonfast.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		nc.Logger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, newError(true, err)
	}

	writeKeyToWorkspaceIDMap := make(map[string]string)
	sourceToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, nc := range workspaces.WorkspaceSourcesMap {
		for _, source := range nc.Sources {
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			sourceToWorkspaceIDMap[source.ID] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = nc.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, nc.Sources...)
	}
	nc.workspaceWriteKeysMapLock.Lock()
	nc.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	nc.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	nc.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	nc.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, nil
}

func (nc *NamespaceConfig) makeHTTPRequest(
	ctx context.Context, url string,
) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(nc.BasicAuthUsername, nc.BasicAuthPassword)
	resp, err := nc.Client.Do(req)
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

func (nc *NamespaceConfig) IsConfigured() bool {
	return false
}

func (nc *NamespaceConfig) AccessToken() string {
	panic("not supported")
}
