package backendconfig

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v5"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// v1ConfigFetcher fetches the namespace's workspace configs from the v1
// data-plane endpoint, which already serves them as ConfigT.
type v1ConfigFetcher struct {
	logger           logger.Logger
	client           *http.Client
	configEnvHandler types.ConfigEnvI

	namespace                string
	identity                 identity.Identifier
	configBackendURL         *url.URL
	cpRouterURL              string
	incrementalConfigUpdates bool

	lastUpdatedAt      time.Time
	workspacesConfig   map[string]ConfigT
	dynamicConfigCache dynamicconfig.Cache

	httpCallsStat        stats.Counter
	httpResponseSizeStat stats.Histogram
}

func newV1ConfigFetcher(nc *namespaceConfig) *v1ConfigFetcher {
	return &v1ConfigFetcher{
		logger:           nc.logger,
		client:           nc.client,
		configEnvHandler: nc.configEnvHandler,

		namespace:                nc.namespace,
		identity:                 nc.Identity(),
		configBackendURL:         nc.configBackendURL,
		cpRouterURL:              nc.cpRouterURL,
		incrementalConfigUpdates: nc.incrementalConfigUpdates,

		workspacesConfig:   make(map[string]ConfigT),
		dynamicConfigCache: make(DynamicConfigMapCache),

		httpCallsStat:        nc.stats.NewStat("backend_config_http_calls", stats.CountType),
		httpResponseSizeStat: nc.stats.NewStat("backend_config_http_response_size", stats.HistogramType),
	}
}

// Get returns the configs of all workspaces in the namespace
func (f *v1ConfigFetcher) Get(ctx context.Context) (map[string]ConfigT, error) {
	conf, err := f.getFromAPI(ctx)
	if errors.Is(err, ErrIncrementalUpdateFailed) {
		// reset state here
		// this triggers a full update
		f.lastUpdatedAt = time.Time{}
		return f.getFromAPI(ctx)
	}
	return conf, err
}

// getFromAPI gets the workspace config from api
func (f *v1ConfigFetcher) getFromAPI(ctx context.Context) (map[string]ConfigT, error) {
	configOnError := make(map[string]ConfigT)
	if f.namespace == "" {
		return configOnError, fmt.Errorf("namespace is not configured")
	}

	var respBody []byte
	u := *f.configBackendURL
	u.Path = fmt.Sprintf("/data-plane/v1/namespaces/%s/config", f.namespace)
	if f.incrementalConfigUpdates && !f.lastUpdatedAt.IsZero() {
		values := u.Query()
		values.Add("updatedAfter", f.lastUpdatedAt.Format(updatedAfterTimeFormat))
		u.RawQuery = values.Encode()
	}

	urlString := u.String()
	req, err := f.prepareHTTPRequest(ctx, urlString)
	if err != nil {
		return configOnError, fmt.Errorf("error preparing request: %s: %w", urlString, err)
	}

	operation := func() (fetchError error) {
		defer f.httpCallsStat.Increment()
		f.logger.Debugn("Fetching backend config", logger.NewStringField("url", urlString))
		respBody, fetchError = f.makeHTTPRequest(req)
		return fetchError
	}

	err = backoffvoid.Retry(ctx, operation,
		backoff.WithMaxTries(3+1),
		backoff.WithNotify(func(err error, t time.Duration) {
			f.logger.Warnn("Failed to fetch backend config from API",
				obskit.Error(err), logger.NewDurationField("retryAfter", t),
			)
		}),
	)
	if err != nil {
		if ctx.Err() == nil {
			f.logger.Errorn("Error sending request to the server", obskit.Error(err))
		}
		return configOnError, err
	}
	configEnvHandler := f.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var requestData map[string]*ConfigT
	err = jsonrs.Unmarshal(respBody, &requestData)
	if err != nil {
		f.logger.Errorn("Error while parsing request", obskit.Error(err))
		return configOnError, err
	}

	workspacesConfig := make(map[string]ConfigT, len(requestData))
	for workspaceID, workspace := range requestData {
		if workspace == nil { // this workspace was not updated, populate it with the previous config
			previousConfig, ok := f.workspacesConfig[workspaceID]
			if !ok {
				f.logger.Errorn(
					"workspace was not updated but was not present in previous config",
					obskit.WorkspaceID(workspaceID),
					logger.NewStringField("requestURL", req.URL.String()),
				)
				return configOnError, ErrIncrementalUpdateFailed
			}
			workspace = &previousConfig
		} else {
			workspace.ApplyReplaySources()
			workspace.processAccountAssociations()
			// Process dynamic config with the instance cache
			ProcessDestinationsInSources(workspace.Sources, f.dynamicConfigCache)
		}
		// always set connection flags to true for hosted and multi-tenant warehouse and processor services
		workspace.ConnectionFlags.URL = f.cpRouterURL
		workspace.ConnectionFlags.Services = map[string]bool{"warehouse": true, "rudderstack-processor": true}
		workspacesConfig[workspaceID] = *workspace
		if f.incrementalConfigUpdates && workspace.UpdatedAt.After(f.lastUpdatedAt) {
			f.lastUpdatedAt = workspace.UpdatedAt
		}
	}

	f.workspacesConfig = workspacesConfig

	return f.workspacesConfig, nil
}

func (f *v1ConfigFetcher) prepareHTTPRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(f.identity.BasicAuth())

	return req, nil
}

func (f *v1ConfigFetcher) makeHTTPRequest(req *http.Request) ([]byte, error) {
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { kithttputil.CloseResponse(resp) }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	f.httpResponseSizeStat.Observe(float64(len(respBody)))

	if resp.StatusCode >= 300 {
		return nil, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, nil
}
