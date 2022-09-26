package backendconfig

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type namespaceConfig struct {
	configEnvHandler          types.ConfigEnvI
	mapsMutex                 sync.RWMutex
	writeKeyToWorkspaceIDMap  map[string]string
	workspaceIDToLibrariesMap map[string]LibrariesT
	sourceToWorkspaceIDMap    map[string]string
	cpRouterURL               string

	Logger logger.LoggerI
	Client *http.Client

	HostedServiceSecret string

	Namespace        string
	ConfigBackendURL *url.URL
}

func (nc *namespaceConfig) SetUp() (err error) {
	nc.writeKeyToWorkspaceIDMap = make(map[string]string)

	if nc.Namespace == "" {
		if !config.IsSet("WORKSPACE_NAMESPACE") {
			return errors.New("workspaceNamespace is not configured")
		}
		nc.Namespace = config.GetString("WORKSPACE_NAMESPACE", "")
	}
	if nc.HostedServiceSecret == "" {
		if !config.IsSet("HOSTED_SERVICE_SECRET") {
			return errors.New("hostedServiceSecret is not configured")
		}
		nc.HostedServiceSecret = config.GetString("HOSTED_SERVICE_SECRET", "")
	}
	if nc.ConfigBackendURL == nil {
		configBackendURL := config.GetString("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
		nc.ConfigBackendURL, err = url.Parse(configBackendURL)
		if err != nil {
			return err
		}
	}
	if nc.Client == nil {
		nc.Client = &http.Client{
			Timeout: config.GetDuration("HttpClient.backendConfig.timeout", 30, time.Second),
		}
	}
	if nc.Logger == nil {
		nc.Logger = logger.NewLogger().Child("backend-config")
	}

	nc.Logger.Infof("Fetching config for namespace %s", nc.Namespace)

	return nil
}

func (nc *namespaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

	if workspaceID, ok := nc.writeKeyToWorkspaceIDMap[writeKey]; ok {
		return workspaceID
	}

	return ""
}

func (nc *namespaceConfig) GetWorkspaceIDForSourceID(source string) string {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

	if workspaceID, ok := nc.sourceToWorkspaceIDMap[source]; ok {
		return workspaceID
	}

	return ""
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (nc *namespaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	nc.mapsMutex.RLock()
	defer nc.mapsMutex.RUnlock()

	if workspaceLibraries, ok := nc.workspaceIDToLibrariesMap[workspaceID]; ok {
		return workspaceLibraries
	}
	return LibrariesT{}
}

// Get returns sources from the workspace
func (nc *namespaceConfig) Get(ctx context.Context, workspaces string) (ConfigT, error) {
	return nc.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (nc *namespaceConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	if nc.Namespace == "" {
		return ConfigT{}, fmt.Errorf("namespace is not configured")
	}

	var respBody []byte
	u := *nc.ConfigBackendURL
	u.Path = fmt.Sprintf("/data-plane/v1/namespaces/%s/config", nc.Namespace)
	operation := func() (fetchError error) {
		nc.Logger.Debugf("Fetching config from %s", u.String())
		respBody, fetchError = nc.makeHTTPRequest(ctx, u.String())
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		nc.Logger.Warnf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		if ctx.Err() == nil {
			nc.Logger.Errorf("Error sending request to the server: %v", err)
		}
		return ConfigT{}, err
	}
	configEnvHandler := nc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspacesConfig map[string]ConfigT
	err = jsonfast.Unmarshal(respBody, &workspacesConfig)
	if err != nil {
		nc.Logger.Errorf("Error while parsing request: %v", err)
		return ConfigT{}, err
	}

	writeKeyToWorkspaceIDMap := make(map[string]string)
	sourceToWorkspaceIDMap := make(map[string]string)
	workspaceIDToLibrariesMap := make(map[string]LibrariesT)
	sourcesJSON := ConfigT{}
	sourcesJSON.Sources = make([]SourceT, 0)
	for workspaceID, nc := range workspacesConfig {
		for i := range nc.Sources {
			source := &nc.Sources[i]
			writeKeyToWorkspaceIDMap[source.WriteKey] = workspaceID
			sourceToWorkspaceIDMap[source.ID] = workspaceID
			workspaceIDToLibrariesMap[workspaceID] = nc.Libraries
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, nc.Sources...)
	}
	sourcesJSON.ConnectionFlags.URL = nc.cpRouterURL
	// always set connection flags to true for hosted and multi-tenant warehouse service
	sourcesJSON.ConnectionFlags.Services = map[string]bool{"warehouse": true}
	nc.mapsMutex.Lock()
	nc.writeKeyToWorkspaceIDMap = writeKeyToWorkspaceIDMap
	nc.sourceToWorkspaceIDMap = sourceToWorkspaceIDMap
	nc.workspaceIDToLibrariesMap = workspaceIDToLibrariesMap
	nc.mapsMutex.Unlock()

	return sourcesJSON, nil
}

func (nc *namespaceConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(nc.Identity().BasicAuth())

	resp, err := nc.Client.Do(req)
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

func (nc *namespaceConfig) AccessToken() string {
	return nc.HostedServiceSecret
}

func (nc *namespaceConfig) Identity() identity.Identifier {
	return &identity.Namespace{
		Namespace:    nc.Namespace,
		HostedSecret: nc.HostedServiceSecret,
	}
}
