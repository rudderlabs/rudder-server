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
	"github.com/rudderlabs/rudder-server/utils/types"
)

// NamespaceConfig TODO check what attributes can be unexported
type NamespaceConfig struct {
	configEnvHandler          types.ConfigEnvI
	mapsMutex                 sync.RWMutex
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	sourceToWorkspaceIDMap    map[string]string

	Logger logger.LoggerI
	Client *http.Client

	HostedServiceSecret string

	Namespace        string
	ConfigBackendURL *url.URL
}

func (nc *NamespaceConfig) SetUp() (err error) {
	nc.writeKeyToWorkspaceIDMap = make(map[string]string)

	if nc.Namespace == "" {
		nc.Namespace, err = config.GetEnvErr("WORKSPACE_NAMESPACE")
		if err != nil {
			return err
		}
	}
	if nc.HostedServiceSecret == "" {
		nc.HostedServiceSecret, err = config.GetEnvErr("HOSTED_MULTITENANT_SERVICE_SECRET")
		if err != nil {
			return err
		}
	}
	if nc.ConfigBackendURL == nil {
		configBackendURL := config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
		nc.ConfigBackendURL, err = url.Parse(configBackendURL)
		if err != nil {
			return err
		}

	}
	if nc.Client == nil {
		nc.Client = &http.Client{
			Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second),
		}
	}
	if nc.Logger == nil {
		nc.Logger = logger.NewLogger().Child("backend-config")
	}

	return nil
}

func (nc *NamespaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

	if workspaceID, ok := nc.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (nc *NamespaceConfig) GetWorkspaceIDForSourceID(source string) string {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

	if workspaceID, ok := nc.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (nc *NamespaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

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
func (nc *NamespaceConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	if nc.Namespace == "" {
		return ConfigT{}, fmt.Errorf("namespace is not configured")
	}

	var (
		respBody   []byte
		statusCode int
	)

	u := *nc.ConfigBackendURL
	u.Path = fmt.Sprintf("/data-plane/v1/namespace/%s/config", nc.Namespace)
	operation := func() (fetchError error) {
		nc.Logger.Debugf("Fetching config from %s", u.String())
		respBody, statusCode, fetchError = nc.makeHTTPRequest(ctx, u.String())
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		nc.Logger.Warnf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		nc.Logger.Errorf("Error sending request to the server: %v", err)
		return ConfigT{}, err
	}
	configEnvHandler := nc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspaces WorkspacesT
	err = jsonfast.Unmarshal(respBody, &workspaces.WorkspaceSourcesMap)
	if err != nil {
		nc.Logger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, err
	}

	writeKeyToWorkspaceIDMap := make(map[string]string)
	sourceToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, nc := range workspaces.WorkspaceSourcesMap {
		for i := range nc.Sources {
			source := &nc.Sources[i]
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			sourceToWorkspaceIDMap[source.ID] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = nc.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, nc.Sources...)
	}

	nc.mapsMutex.Lock()
	nc.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	nc.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	nc.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	nc.mapsMutex.Unlock()

	return sourcesJSON, nil
}

func (nc *NamespaceConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, int, error) {
	req, err := Http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", nc.HostedServiceSecret))
	resp, err := nc.Client.Do(req)
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

func (nc *NamespaceConfig) AccessToken() string {
	return nc.HostedServiceSecret
}
