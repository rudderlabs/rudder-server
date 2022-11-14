package backendconfig

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	configEnvHandler types.ConfigEnvI
	cpRouterURL      string

	Logger logger.Logger
	Client *http.Client

	HostedServiceSecret string

	Namespace        string
	ConfigBackendURL *url.URL
}

func (nc *namespaceConfig) SetUp() (err error) {
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
		configBackendURL := config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
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

// Get returns sources from the workspace
func (nc *namespaceConfig) Get(ctx context.Context, workspaces string) (map[string]ConfigT, error) {
	return nc.getFromAPI(ctx, workspaces)
}

// getFromApi gets the workspace config from api
func (nc *namespaceConfig) getFromAPI(ctx context.Context, _ string) (map[string]ConfigT, error) {
	config := make(map[string]ConfigT)
	if nc.Namespace == "" {
		return config, fmt.Errorf("namespace is not configured")
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
		return config, err
	}
	configEnvHandler := nc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var workspacesConfig map[string]ConfigT
	err = jsonfast.Unmarshal(respBody, &workspacesConfig)
	if err != nil {
		nc.Logger.Errorf("Error while parsing request: %v", err)
		return config, err
	}

	for workspaceID, wc := range workspacesConfig {
		// always set connection flags to true for hosted and multi-tenant warehouse service
		wc.ConnectionFlags.URL = nc.cpRouterURL
		wc.ConnectionFlags.Services = map[string]bool{"warehouse": true}
		workspacesConfig[workspaceID] = wc
	}

	return workspacesConfig, nil
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
