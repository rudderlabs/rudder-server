package backendconfig

//go:generate mockgen -destination=../mocks/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/backend-config BackendConfig
//go:generate mockgen -destination=./mock_workspaceconfig.go -package=backendconfig -source=./backend-config.go workspaceConfig

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/backend-config/internal/cache"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var (
	// environment variables
	configBackendURL            string
	cpRouterURL                 string
	pollInterval                config.ValueLoader[time.Duration]
	configJSONPath              string
	configFromFile              bool
	configEnvReplacementEnabled bool
	dbCacheEnabled              bool

	LastSync           string
	LastRegulationSync string

	// DefaultBackendConfig will be initialized be Setup to either a WorkspaceConfig or MultiWorkspaceConfig.
	DefaultBackendConfig     BackendConfig
	pkgLogger                = logger.NewLogger().Child("backend-config")
	IoUtil                   = sysUtils.NewIoUtil()
	Diagnostics              diagnostics.DiagnosticsI
	cacheOverride            cache.Cache
	incrementalConfigUpdates bool
)

func disableCache() {
	cacheOverride = new(noCache)
}

type noCache struct{}

func (*noCache) Get(context.Context) ([]byte, error) {
	return nil, fmt.Errorf(`noCache: cache disabled`)
}

type workspaceConfig interface {
	SetUp() error
	// Deprecated: use Identity() instead.
	AccessToken() string
	Get(context.Context) (map[string]ConfigT, error)
	Identity() identity.Identifier
}

type BackendConfig interface {
	workspaceConfig
	WaitForConfig(ctx context.Context)
	Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel
	Stop()
	StartWithIDs(ctx context.Context, workspaces string)
}

type backendConfigImpl struct {
	workspaceConfig
	eb                *pubsub.PublishSubscriber
	ctx               context.Context
	cancel            context.CancelFunc
	blockChan         chan struct{}
	initializedLock   sync.RWMutex
	initialized       bool
	curSourceJSON     map[string]ConfigT
	curSourceJSONLock sync.RWMutex
	usingCache        bool
	cache             cache.Cache
}

func loadConfig() {
	configBackendURL = config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	cpRouterURL = config.GetString("CP_ROUTER_URL", "https://cp-router.rudderlabs.com")
	pollInterval = config.GetReloadableDurationVar(5, time.Second, "BackendConfig.pollInterval", "BackendConfig.pollIntervalInS")
	configJSONPath = config.GetStringVar("/etc/rudderstack/workspaceConfig.json", "BackendConfig.configJSONPath")
	configFromFile = config.GetBoolVar(false, "BackendConfig.configFromFile")
	configEnvReplacementEnabled = config.GetBoolVar(true, "BackendConfig.envReplacementEnabled")
	incrementalConfigUpdates = config.GetBoolVar(false, "BackendConfig.incrementalConfigUpdates")
	dbCacheEnabled = config.GetBoolVar(true, "BackendConfig.dbCacheEnabled")
}

func Init() {
	Diagnostics = diagnostics.Diagnostics
	loadConfig()
}

func trackConfig(preConfig, curConfig ConfigT) {
	Diagnostics.DisableMetrics(curConfig.EnableMetrics)
	if diagnostics.EnableConfigIdentifyMetric {
		if len(preConfig.Sources) == 0 && len(curConfig.Sources) > 0 {
			Diagnostics.Identify(map[string]interface{}{
				diagnostics.ConfigIdentify: curConfig.Sources[0].WorkspaceID,
			})
		}
	}
	if diagnostics.EnableConfigProcessedMetric {
		noOfSources := len(curConfig.Sources)
		noOfDestinations := 0
		for _, source := range curConfig.Sources {
			noOfDestinations += len(source.Destinations)
		}
		Diagnostics.Track(diagnostics.ConfigProcessed, map[string]interface{}{
			diagnostics.SourcesCount:      noOfSources,
			diagnostics.DesitanationCount: noOfDestinations,
		})
	}
}

func filterProcessorEnabledWorkspaceConfig(config map[string]ConfigT) map[string]ConfigT {
	filterConfig := make(map[string]ConfigT, len(config))
	for workspaceID, wConfig := range config {
		filterConfig[workspaceID] = filterProcessorEnabledDestinations(wConfig)
	}
	return filterConfig
}

func filterProcessorEnabledDestinations(config ConfigT) ConfigT {
	var modifiedConfig ConfigT
	modifiedConfig.Libraries = config.Libraries
	modifiedConfig.Sources = make([]SourceT, 0)
	modifiedConfig.Credentials = config.Credentials
	for _, source := range config.Sources {
		var destinations []DestinationT
		for _, destination := range source.Destinations { // TODO skipcq: CRT-P0006
			pkgLogger.Debug(destination.Name, " IsProcessorEnabled: ", destination.IsProcessorEnabled)
			if destination.IsProcessorEnabled {
				destinations = append(destinations, destination)
			}
		}
		source.Destinations = destinations
		modifiedConfig.Sources = append(modifiedConfig.Sources, source)
	}
	modifiedConfig.Settings = config.Settings
	return modifiedConfig
}

func (bc *backendConfigImpl) configUpdate(ctx context.Context) {
	statConfigBackendError := stats.Default.NewStat("config_backend.errors", stats.CountType)

	var (
		sourceJSON map[string]ConfigT
		err        error
	)
	defer func() {
		cacheConfigGauge := stats.Default.NewStat("config_from_cache", stats.GaugeType)
		if bc.usingCache {
			cacheConfigGauge.Gauge(1)
		} else {
			cacheConfigGauge.Gauge(0)
		}
	}()

	sourceJSON, err = bc.workspaceConfig.Get(ctx)
	if err != nil {
		statConfigBackendError.Increment()
		pkgLogger.Warnf("Error fetching config from backend: %v", err)

		bc.initializedLock.RLock()
		if bc.initialized {
			bc.initializedLock.RUnlock()
			return
		}
		bc.initializedLock.RUnlock()

		// try to get config from cache
		sourceJSONBytes, cacheErr := bc.cache.Get(ctx)
		if cacheErr != nil {
			pkgLogger.Warnf("Error fetching config from cache: %v", cacheErr)
			return
		}
		err = json.Unmarshal(sourceJSONBytes, &sourceJSON)
		if err != nil {
			pkgLogger.Warnf("Error unmarshalling cached config: %v", cacheErr)
			return
		}
		bc.usingCache = true
	} else {
		bc.usingCache = false
	}

	// sorting the sourceJSON.
	// json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	for workspace := range sourceJSON {
		sort.Slice(sourceJSON[workspace].Sources, func(i, j int) bool {
			return sourceJSON[workspace].Sources[i].ID < sourceJSON[workspace].Sources[j].ID
		})
	}

	bc.curSourceJSONLock.Lock()
	if !reflect.DeepEqual(bc.curSourceJSON, sourceJSON) {

		pkgLogger.Infow("Workspace Config changed",
			"workspaces", len(sourceJSON),
			"sources", lo.Sum(lo.Map(lo.Values(sourceJSON), func(c ConfigT, _ int) int { return len(c.Sources) })),
		)

		if len(sourceJSON) == 1 { // only use diagnostics if there is one workspace
			for _, wConfig := range sourceJSON {
				trackConfig(bc.curSourceJSON[wConfig.WorkspaceID], wConfig)
			}
		}
		filteredSourcesJSON := filterProcessorEnabledWorkspaceConfig(sourceJSON)
		bc.curSourceJSON = sourceJSON
		bc.curSourceJSONLock.Unlock()
		LastSync = time.Now().Format(time.RFC3339) // TODO fix concurrent access
		bc.eb.Publish(string(TopicBackendConfig), sourceJSON)
		bc.eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
	} else {
		bc.curSourceJSONLock.Unlock()
	}

	bc.initializedLock.Lock()
	bc.initialized = true
	bc.initializedLock.Unlock()
}

func (bc *backendConfigImpl) pollConfigUpdate(ctx context.Context) {
	for {
		bc.configUpdate(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval.Load()):
		}
	}
}

/*
Subscribe subscribes a channel to a specific topic of backend config updates.

Channel will receive a new pubsub.DataEvent each time the backend configuration is updated.

Data of the DataEvent should be a backendconfig.ConfigT struct.

Available topics are:

- TopicBackendConfig: Will receive complete backend configuration

- TopicProcessConfig: Will receive only backend configuration of processor enabled destinations

- TopicRegulations: Will receive all regulations
*/
func (bc *backendConfigImpl) Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel {
	return bc.eb.Subscribe(ctx, string(topic))
}

func newForDeployment(deploymentType deployment.Type, region string, configEnvHandler types.ConfigEnvI) (BackendConfig, error) {
	backendConfig := &backendConfigImpl{
		eb: pubsub.New(),
	}
	parsedConfigBackendURL, err := url.Parse(configBackendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid config backend URL: %v", err)
	}

	switch deploymentType {
	case deployment.DedicatedType:
		backendConfig.workspaceConfig = &singleWorkspaceConfig{
			configJSONPath:   configJSONPath,
			configBackendURL: parsedConfigBackendURL,
			configEnvHandler: configEnvHandler,
			region:           region,
		}
	case deployment.MultiTenantType:
		backendConfig.workspaceConfig = &namespaceConfig{
			configBackendURL:         parsedConfigBackendURL,
			configEnvHandler:         configEnvHandler,
			cpRouterURL:              cpRouterURL,
			region:                   region,
			incrementalConfigUpdates: incrementalConfigUpdates,
		}
	default:
		return nil, fmt.Errorf("deployment type %q not supported", deploymentType)
	}

	return backendConfig, backendConfig.SetUp()
}

// Setup backend config
func Setup(configEnvHandler types.ConfigEnvI) (err error) {
	deploymentType, err := deployment.GetFromEnv()
	region := config.GetString("region", "")
	if err != nil {
		return fmt.Errorf("deployment type from env: %w", err)
	}

	backendConfig, err := newForDeployment(deploymentType, region, configEnvHandler)
	if err != nil {
		return err
	}

	DefaultBackendConfig = backendConfig

	return nil
}

func (bc *backendConfigImpl) StartWithIDs(ctx context.Context, _ string) {
	var err error
	ctx, cancel := context.WithCancel(ctx)
	bc.ctx = ctx
	bc.cancel = cancel
	bc.blockChan = make(chan struct{})
	bc.cache = cacheOverride

	if !dbCacheEnabled {
		bc.cache = &noCache{}
	}
	if bc.cache == nil {
		identifier := bc.Identity()
		u, _ := identifier.BasicAuth()
		secret := sha256.Sum256([]byte(u))
		cacheKey := identifier.ID()
		bc.cache, err = cache.Start(
			ctx,
			secret,
			cacheKey,
			func() pubsub.DataChannel { return bc.Subscribe(ctx, TopicBackendConfig) },
		)
		if err != nil {
			// the only reason why we should resume by using no cache,
			// would be if no database configuration has been set
			if config.IsSet("DB.host") {
				panic(fmt.Errorf("error starting backend config cache: %w", err))
			} else {
				pkgLogger.Warnf("Failed to start backend config cache, no cache will be used: %w", err)
				bc.cache = &noCache{}
			}
		}
	}

	rruntime.Go(func() {
		bc.pollConfigUpdate(ctx)
		close(bc.blockChan)
	})
}

func (bc *backendConfigImpl) Stop() {
	if bc.cancel != nil {
		bc.cancel()
		<-bc.blockChan
	}
	bc.initializedLock.Lock()
	bc.initialized = false
	bc.initializedLock.Unlock()
}

// WaitForConfig waits until backend config has been initialized
func (bc *backendConfigImpl) WaitForConfig(ctx context.Context) {
	for {
		bc.initializedLock.RLock()
		if bc.initialized {
			bc.initializedLock.RUnlock()
			return
		}
		bc.initializedLock.RUnlock()

		pkgLogger.Info("Waiting for backend config")
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval.Load()):
		}
	}
}

func GetConfigBackendURL() string {
	return configBackendURL
}

func getNotOKError(respBody []byte, statusCode int) error {
	errMsg := ""
	if len(respBody) > 0 {
		errMsg = fmt.Sprintf(": %s", respBody)
	}
	return fmt.Errorf("backend config request failed with %d%s", statusCode, errMsg)
}

func (bc *backendConfigImpl) Identity() identity.Identifier {
	result := bc.workspaceConfig.Identity()
	if result.ID() == "" && bc.usingCache { // in case of a cached config the ID is not set when operating in single workspace mode
		bc.curSourceJSONLock.RLock()
		curConfig := bc.curSourceJSON
		bc.curSourceJSONLock.RUnlock()
		if len(curConfig) == 1 {
			for workspaceID := range curConfig {
				return &identity.IdentifierDecorator{
					Identifier: result,
					Id:         workspaceID,
				}
			}
		}
		return bc.workspaceConfig.Identity()
	}
	return result
}
