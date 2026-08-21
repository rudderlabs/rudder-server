package backendconfig

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v5"

	cpsdk "github.com/rudderlabs/rudder-cp-sdk"
	"github.com/rudderlabs/rudder-cp-sdk/diff"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// v2ConfigFetcher fetches the namespace's workspace configs from the v2 endpoint, which serves
// them in a shape of its own, and maps each one to a v1 ConfigT.
type v2ConfigFetcher struct {
	logger logger.Logger
	client v2Client
	mapper mapper

	cpRouterURL              string
	incrementalConfigUpdates bool

	updater            diff.Updater[string]
	cache              v2NamespaceConfig
	lastUpdatedAt      time.Time
	memo               map[string]v2MemoEntry
	dynamicConfigCache dynamicconfig.Cache

	httpCallsStat stats.Counter
}

// v2Client is the part of the control plane SDK this fetcher needs.
type v2Client interface {
	GetWorkspaceConfigs(ctx context.Context, object any, updatedAfter time.Time) error
}

// v2MemoEntry is a workspace's last transform, kept so that a poll in which the workspace did not
// change, and neither did the definitions it was mapped against, can skip the transform entirely.
type v2MemoEntry struct {
	generation v2Generation
	config     ConfigT
}

func newV2ConfigFetcher(nc *namespaceConfig) (*v2ConfigFetcher, error) {
	httpCallsStat := nc.stats.NewStat("backend_config_http_calls", stats.CountType)

	client, err := cpsdk.New(
		cpsdk.WithNamespaceIdentity(nc.namespace, nc.hostedServiceSecret),
		// both versions are served from the same host, the one CONFIG_BACKEND_URL points at
		cpsdk.WithBaseUrl(nc.configBackendURL.String()),
		// account secrets are embedded in the response, as v1 serves them: without this the
		// destinations arrive without their credentials and every delivery fails authentication
		cpsdk.WithSecrets(cpsdk.SecretsEmbed),
		cpsdk.WithRequestDoer(&v2RequestDoer{
			doer:                 nc.client,
			configEnvHandler:     nc.configEnvHandler,
			httpResponseSizeStat: nc.stats.NewStat("backend_config_http_response_size", stats.HistogramType),
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("creating control plane client: %w", err)
	}

	return &v2ConfigFetcher{
		logger:                   nc.logger.Withn(logger.NewStringField("configVersion", "v2")),
		client:                   client,
		mapper:                   &stubMapper{},
		cpRouterURL:              nc.cpRouterURL,
		incrementalConfigUpdates: nc.incrementalConfigUpdates,
		memo:                     make(map[string]v2MemoEntry),
		dynamicConfigCache:       make(DynamicConfigMapCache),
		httpCallsStat:            httpCallsStat,
	}, nil
}

// Get returns the configs of all workspaces in the namespace
func (f *v2ConfigFetcher) Get(ctx context.Context) (map[string]ConfigT, error) {
	conf, err := f.getFromAPI(ctx)
	if errors.Is(err, ErrIncrementalUpdateFailed) {
		// the cache can no longer be reconciled with what the control plane is sending,
		// so drop it and ask for everything
		f.reset()
		return f.getFromAPI(ctx)
	}
	return conf, err
}

// reset drops everything that was accumulated across polls, so that the next request asks for the
// full config rather than for a delta.
func (f *v2ConfigFetcher) reset() {
	f.updater = diff.Updater[string]{}
	f.cache = v2NamespaceConfig{}
	f.lastUpdatedAt = time.Time{}
	f.memo = make(map[string]v2MemoEntry)
}

func (f *v2ConfigFetcher) getFromAPI(ctx context.Context) (map[string]ConfigT, error) {
	configOnError := make(map[string]ConfigT)

	var response v2NamespaceConfig
	operation := func() error {
		defer f.httpCallsStat.Increment()
		response = v2NamespaceConfig{} // a retry must not decode on top of a half filled response
		var updatedAfter time.Time
		if f.incrementalConfigUpdates {
			updatedAfter = f.lastUpdatedAt
		}
		return f.client.GetWorkspaceConfigs(ctx, &response, updatedAfter)
	}

	err := backoffvoid.Retry(ctx, operation,
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

	if response.Version != v2ConfigVersion {
		return configOnError, fmt.Errorf("unexpected config version %d, expected %d", response.Version, v2ConfigVersion)
	}
	if len(response.Workspaces) == 0 { // zero workspaces means invalid response
		if !f.lastUpdatedAt.IsZero() { // by returning ErrIncrementalUpdateFailed we force to reset lastUpdatedAt amd retry
			return configOnError, fmt.Errorf("%w: no workspaces in response", ErrIncrementalUpdateFailed)
		}
		return configOnError, fmt.Errorf("no workspaces in response")
	}

	// both the members and the changed set are read here, off the response and before the cache
	// is updated: UpdateCache writes cached workspaces back over the nils
	members := make([]string, 0, len(response.Workspaces))
	changed := make(map[string]struct{}, len(response.Workspaces))
	for workspaceID, workspace := range response.Workspaces {
		members = append(members, workspaceID)
		if workspace != nil {
			changed[workspaceID] = struct{}{}
		}
	}

	lastUpdatedAt, _, err := f.updater.UpdateCache(&response, &f.cache)
	if err != nil {
		return configOnError, fmt.Errorf("%w: %w", ErrIncrementalUpdateFailed, err)
	}

	generation := f.cache.generation()
	workspacesConfig := make(map[string]ConfigT, len(members))
	memo := make(map[string]v2MemoEntry, len(members))
	for _, workspaceID := range members {
		workspace := response.Workspaces[workspaceID]
		if workspace == nil { // UpdateCache should have filled this one in from the cache
			f.logger.Errorn("workspace was not updated but was not present in previous config",
				obskit.WorkspaceID(workspaceID),
			)
			return configOnError, ErrIncrementalUpdateFailed
		}

		config, memoized := f.memoized(workspaceID, changed, generation)
		if !memoized {
			config, err = f.mapper.Map(workspaceID, workspace.Raw, f.cache.v2Catalogues)
			if err != nil {
				return configOnError, fmt.Errorf("mapping workspace %q: %w", workspaceID, err)
			}
			config.ApplyReplaySources()
			config.processAccountAssociations()
			// Process dynamic config with the instance cache
			ProcessDestinationsInSources(config.Sources, f.dynamicConfigCache)
		}
		memo[workspaceID] = v2MemoEntry{generation: generation, config: config}

		// always set connection flags to true for hosted and multi-tenant warehouse and processor services
		config.ConnectionFlags.URL = f.cpRouterURL
		config.ConnectionFlags.Services = map[string]bool{"warehouse": true, "rudderstack-processor": true}
		workspacesConfig[workspaceID] = config
	}
	// both lastUpdatedAt & memo should move together once the whole poll has succeeded
	f.lastUpdatedAt = lastUpdatedAt
	// rebuilt rather than updated, so that a workspace which left the namespace leaves the memo with it
	f.memo = memo

	return workspacesConfig, nil
}

// memoized returns the workspace's last transform, if it can still stand in for a new one.
func (f *v2ConfigFetcher) memoized(workspaceID string, changed map[string]struct{}, generation v2Generation) (ConfigT, bool) {
	if _, ok := changed[workspaceID]; ok {
		return ConfigT{}, false
	}
	entry, ok := f.memo[workspaceID]
	if !ok || entry.generation != generation {
		return ConfigT{}, false
	}
	return entry.config, true
}

// v2Generation identifies a state of the definition catalogues, which every workspace is mapped
// against. Cardinalities are part of it because a deletion bumps no timestamp.
//
// It is compared with ==, which is safe for the time it holds: that only ever comes off the wire,
// so it carries no monotonic clock reading, the one thing == would read and Equal would not.
type v2Generation struct {
	maxUpdatedAt                             time.Time
	sourceDefs, destinationDefs, accountDefs int
}

func (c *v2NamespaceConfig) generation() v2Generation {
	generation := v2Generation{
		sourceDefs:      len(c.SourceDefinitions),
		destinationDefs: len(c.DestinationDefinitions),
		accountDefs:     len(c.AccountDefinitions),
	}
	generation.maxUpdatedAt = newestOf(generation.maxUpdatedAt, c.SourceDefinitions)
	generation.maxUpdatedAt = newestOf(generation.maxUpdatedAt, c.DestinationDefinitions)
	generation.maxUpdatedAt = newestOf(generation.maxUpdatedAt, c.AccountDefinitions)
	return generation
}

// newestOf returns the later of newest and the most recently updated entry of the catalogue.
func newestOf[M ~map[string]T, T interface{ updatedAt() time.Time }](newest time.Time, catalogue M) time.Time {
	for _, definition := range catalogue {
		if updatedAt := definition.updatedAt(); updatedAt.After(newest) {
			newest = updatedAt
		}
	}
	return newest
}

// v2RequestDoer carries over what the SDK's client does not do itself: the environment variable
// replacement v1 applies to the payload, and the response size stat.
type v2RequestDoer struct {
	doer                 *http.Client
	configEnvHandler     types.ConfigEnvI
	httpResponseSizeStat stats.Histogram
}

func (d *v2RequestDoer) Do(req *http.Request) (*http.Response, error) {
	resp, err := d.doer.Do(req)
	if err != nil {
		return nil, err
	}

	configEnvHandler := d.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		// the replacement needs the whole document, so this is the one case the response cannot
		// be decoded as it arrives
		defer func() { kithttputil.CloseResponse(resp) }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		d.httpResponseSizeStat.Observe(float64(len(body)))
		replaced := configEnvHandler.ReplaceConfigWithEnvVariables(body)
		return &http.Response{
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body:       io.NopCloser(bytes.NewReader(replaced)),
		}, nil
	}

	resp.Body = &observedReadCloser{ReadCloser: resp.Body, observe: d.httpResponseSizeStat.Observe}
	return resp, nil
}

// observedReadCloser reports how much was read out of it once it is closed.
type observedReadCloser struct {
	io.ReadCloser
	read    int
	observe func(float64)
}

func (r *observedReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.read += n
	return n, err
}

func (r *observedReadCloser) Close() error {
	r.observe(float64(r.read))
	return r.ReadCloser.Close()
}
