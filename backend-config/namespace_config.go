package backendconfig

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	updatedAfterTimeFormat     = "2006-01-02T15:04:05.000Z"
	ErrIncrementalUpdateFailed = errors.New("incremental update failed")
)

// configFetcher fetches the configs of all workspaces in a namespace, keyed by workspace ID.
// Implementations own their own incremental update state and are responsible for
// recovering from a failed incremental update.
type configFetcher interface {
	Get(ctx context.Context) (map[string]ConfigT, error)
}

type namespaceConfig struct {
	configEnvHandler types.ConfigEnvI
	cpRouterURL      string

	config *config.Config
	logger logger.Logger
	client *http.Client
	stats  stats.Stats

	hostedServiceSecret string

	namespace                string
	configBackendURL         *url.URL
	incrementalConfigUpdates bool

	fetcher configFetcher
}

func (nc *namespaceConfig) SetUp() (err error) {
	if nc.config == nil {
		nc.config = config.Default
	}
	if nc.namespace == "" {
		if !nc.config.IsSet("WORKSPACE_NAMESPACE") {
			return errors.New("workspaceNamespace is not configured")
		}
		nc.namespace = nc.config.GetStringVar("", "WORKSPACE_NAMESPACE")
	}
	if nc.hostedServiceSecret == "" {
		if !nc.config.IsSet("HOSTED_SERVICE_SECRET") {
			return errors.New("hostedServiceSecret is not configured")
		}
		nc.hostedServiceSecret = nc.config.GetStringVar("", "HOSTED_SERVICE_SECRET")
	}
	if nc.configBackendURL == nil {
		configBackendURL := nc.config.GetStringVar("https://api.rudderstack.com", "CONFIG_BACKEND_URL")
		nc.configBackendURL, err = url.Parse(configBackendURL)
		if err != nil {
			return err
		}
	}
	if nc.client == nil {
		nc.client = &http.Client{
			Timeout: nc.config.GetDurationVar(30, time.Second, "HttpClient.backendConfig.timeout"),
		}
	}

	if nc.stats == nil {
		nc.stats = stats.Default
	}

	if nc.logger == nil {
		nc.logger = logger.NewLogger().Child("backend-config").Withn(obskit.Namespace(nc.namespace))
	}

	// the mode is fixed at setup: each fetcher owns its own incremental update state, so they
	// cannot be swapped underneath a running poll loop
	switch mode := nc.config.GetStringVar("v1", "BackendConfig.namespaceConfigMode"); mode {
	case "v1":
		nc.fetcher = newV1ConfigFetcher(nc)
	case "v2":
		if nc.fetcher, err = newV2ConfigFetcher(nc); err != nil {
			return err
		}
	case "shadow": // v1 serves, v2 is fetched on the side and compared
		if nc.fetcher, err = newShadowConfigFetcher(nc); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown namespace config mode %q", mode)
	}

	nc.logger.Infon("Setup backend config complete")

	return nil
}

// Get returns the configs of all workspaces in the namespace
func (nc *namespaceConfig) Get(ctx context.Context) (map[string]ConfigT, error) {
	return nc.fetcher.Get(ctx)
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
