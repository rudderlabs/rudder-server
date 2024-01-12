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

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	jsonfast                   = jsoniter.ConfigCompatibleWithStandardLibrary
	updatedAfterTimeFormat     = "2006-01-02T15:04:05.000Z"
	ErrIncrementalUpdateFailed = errors.New("incremental update failed")
)

type namespaceConfig struct {
	configEnvHandler types.ConfigEnvI
	cpRouterURL      string

	config *config.Config
	logger logger.Logger
	client *http.Client

	hostedServiceSecret string

	namespace                string
	configBackendURL         *url.URL
	region                   string
	incrementalConfigUpdates bool
	lastUpdatedAt            time.Time
	workspacesConfig         map[string]ConfigT

	httpCallsStat        stats.Counter
	httpResponseSizeStat stats.Histogram
}

func (nc *namespaceConfig) SetUp() (err error) {
	if nc.config == nil {
		nc.config = config.Default
	}
	if nc.namespace == "" {
		if !nc.config.IsSet("WORKSPACE_NAMESPACE") {
			return errors.New("workspaceNamespace is not configured")
		}
		nc.namespace = nc.config.GetString("WORKSPACE_NAMESPACE", "")
	}
	if nc.hostedServiceSecret == "" {
		if !nc.config.IsSet("HOSTED_SERVICE_SECRET") {
			return errors.New("hostedServiceSecret is not configured")
		}
		nc.hostedServiceSecret = nc.config.GetString("HOSTED_SERVICE_SECRET", "")
	}
	if nc.configBackendURL == nil {
		configBackendURL := nc.config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
		nc.configBackendURL, err = url.Parse(configBackendURL)
		if err != nil {
			return err
		}
	}
	if nc.client == nil {
		nc.client = &http.Client{
			Timeout: nc.config.GetDuration("HttpClient.backendConfig.timeout", 30, time.Second),
		}
	}
	nc.workspacesConfig = make(map[string]ConfigT)
	nc.httpCallsStat = stats.Default.NewStat("backend_config_http_calls", stats.CountType)
	nc.httpResponseSizeStat = stats.Default.NewStat("backend_config_http_response_size", stats.HistogramType)

	if nc.logger == nil {
		nc.logger = logger.NewLogger().Child("backend-config").Withn(obskit.Namespace(nc.namespace))
	}
	nc.logger.Infon("Setup backend config complete")

	return nil
}

// Get returns sources from the workspace
func (nc *namespaceConfig) Get(ctx context.Context) (map[string]ConfigT, error) {
	conf, err := nc.getFromAPI(ctx)
	if errors.Is(err, ErrIncrementalUpdateFailed) {
		// reset state here
		// this triggers a full update
		nc.lastUpdatedAt = time.Time{}
		return nc.getFromAPI(ctx)
	}
	return conf, err
}

// getFromApi gets the workspace config from api
func (nc *namespaceConfig) getFromAPI(ctx context.Context) (map[string]ConfigT, error) {
	configOnError := make(map[string]ConfigT)
	if nc.namespace == "" {
		return configOnError, fmt.Errorf("namespace is not configured")
	}

	var respBody []byte
	u := *nc.configBackendURL
	u.Path = fmt.Sprintf("/data-plane/v1/namespaces/%s/config", nc.namespace)
	if nc.incrementalConfigUpdates && !nc.lastUpdatedAt.IsZero() {
		values := u.Query()
		values.Add("updatedAfter", nc.lastUpdatedAt.Format(updatedAfterTimeFormat))
		u.RawQuery = values.Encode()
	}

	urlString := u.String()
	req, err := nc.prepareHTTPRequest(ctx, urlString)
	if err != nil {
		return configOnError, fmt.Errorf("error preparing request: %s: %w", urlString, err)
	}

	operation := func() (fetchError error) {
		defer nc.httpCallsStat.Increment()
		nc.logger.Debugn("Fetching backend config", logger.NewStringField("url", urlString))
		respBody, fetchError = nc.makeHTTPRequest(req)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx)
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		nc.logger.Warnn("Failed to fetch backend config from API",
			obskit.Error(err), logger.NewDurationField("retryAfter", t),
		)
	})
	if err != nil {
		if ctx.Err() == nil {
			nc.logger.Errorn("Error sending request to the server", obskit.Error(err))
		}
		return configOnError, err
	}
	configEnvHandler := nc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var requestData map[string]*ConfigT
	err = jsonfast.Unmarshal(respBody, &requestData)
	if err != nil {
		nc.logger.Errorn("Error while parsing request", obskit.Error(err))
		return configOnError, err
	}

	workspacesConfig := make(map[string]ConfigT, len(requestData))
	for workspaceID, workspace := range requestData {
		if workspace == nil { // this workspace was not updated, populate it with the previous config
			previousConfig, ok := nc.workspacesConfig[workspaceID]
			if !ok {
				nc.logger.Errorn(
					"workspace was not updated but was not present in previous config",
					obskit.WorkspaceID(workspaceID),
					logger.NewField("req", req),
				)
				return configOnError, ErrIncrementalUpdateFailed
			}
			workspace = &previousConfig
		} else {
			workspace.ApplyReplaySources()
		}

		// always set connection flags to true for hosted and multi-tenant warehouse service
		workspace.ConnectionFlags.URL = nc.cpRouterURL
		workspace.ConnectionFlags.Services = map[string]bool{"warehouse": true}
		workspacesConfig[workspaceID] = *workspace
		if nc.incrementalConfigUpdates && workspace.UpdatedAt.After(nc.lastUpdatedAt) {
			nc.lastUpdatedAt = workspace.UpdatedAt
		}
	}

	nc.workspacesConfig = workspacesConfig

	return nc.workspacesConfig, nil
}

func (nc *namespaceConfig) prepareHTTPRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(nc.Identity().BasicAuth())
	if nc.region != "" {
		q := req.URL.Query()
		q.Add("region", nc.region)
		req.URL.RawQuery = q.Encode()
	}

	return req, nil
}

func (nc *namespaceConfig) makeHTTPRequest(req *http.Request) ([]byte, error) {
	resp, err := nc.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { kithttputil.CloseResponse(resp) }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	nc.httpResponseSizeStat.Observe(float64(len(respBody)))

	if resp.StatusCode >= 300 {
		return nil, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, nil
}

func (nc *namespaceConfig) AccessToken() string {
	return nc.hostedServiceSecret
}

func (nc *namespaceConfig) Identity() identity.Identifier {
	return &identity.Namespace{
		Namespace:    nc.namespace,
		HostedSecret: nc.hostedServiceSecret,
	}
}
