package backendconfig

//go:generate mockgen -destination=../../mocks/config/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/config/backend-config BackendConfig
//go:generate mockgen -destination=./mock_workspaceconfig.go -package=backendconfig -source=./backend-config.go workspaceConfig

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"time"

	adminpkg "github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var (
	// environment variables
	configBackendURL                      string
	cpRouterURL                           string
	pollInterval, regulationsPollInterval time.Duration
	configJSONPath                        string
	configFromFile                        bool
	maxRegulationsPerRequest              int
	configEnvReplacementEnabled           bool

	LastSync           string
	LastRegulationSync string

	// DefaultBackendConfig will be initialized be Setup to either a WorkspaceConfig or MultiWorkspaceConfig.
	DefaultBackendConfig BackendConfig
	pkgLogger            = logger.NewLogger().Child("backend-config")
	IoUtil               = sysUtils.NewIoUtil()
	Diagnostics          diagnostics.DiagnosticsI
)

type workspaceConfig interface {
	SetUp() error
	AccessToken() string
	Get(context.Context, string) (ConfigT, error)
	GetWorkspaceIDForWriteKey(string) string
	GetWorkspaceIDForSourceID(string) string
	GetWorkspaceLibrariesForWorkspaceID(string) LibrariesT
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
	curSourceJSON     ConfigT
	curSourceJSONLock sync.RWMutex
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	cpRouterURL = config.GetEnv("CP_ROUTER_URL", "https://cp-router.rudderlabs.com")
	config.RegisterDurationConfigVariable(5, &pollInterval, true, time.Second, []string{"BackendConfig.pollInterval", "BackendConfig.pollIntervalInS"}...)
	config.RegisterDurationConfigVariable(300, &regulationsPollInterval, true, time.Second, []string{"BackendConfig.regulationsPollInterval", "BackendConfig.regulationsPollIntervalInS"}...)
	config.RegisterStringConfigVariable("/etc/rudderstack/workspaceConfig.json", &configJSONPath, false, "BackendConfig.configJSONPath")
	config.RegisterBoolConfigVariable(false, &configFromFile, false, "BackendConfig.configFromFile")
	config.RegisterIntConfigVariable(1000, &maxRegulationsPerRequest, true, 1, "BackendConfig.maxRegulationsPerRequest")
	config.RegisterBoolConfigVariable(true, &configEnvReplacementEnabled, false, "BackendConfig.envReplacementEnabled")
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

func filterProcessorEnabledDestinations(config ConfigT) ConfigT {
	var modifiedConfig ConfigT
	modifiedConfig.Libraries = config.Libraries
	modifiedConfig.Sources = make([]SourceT, 0)
	for _, source := range config.Sources {
		destinations := make([]DestinationT, 0)
		for _, destination := range source.Destinations { // TODO skipcq: CRT-P0006
			pkgLogger.Debug(destination.Name, " IsProcessorEnabled: ", destination.IsProcessorEnabled)
			if destination.IsProcessorEnabled {
				destinations = append(destinations, destination)
			}
		}
		source.Destinations = destinations
		modifiedConfig.Sources = append(modifiedConfig.Sources, source)
	}
	return modifiedConfig
}

func (bc *backendConfigImpl) configUpdate(ctx context.Context, statConfigBackendError stats.RudderStats, workspaces string) {
	sourceJSON, err := bc.workspaceConfig.Get(ctx, workspaces)
	if err != nil {
		statConfigBackendError.Increment()
		pkgLogger.Warnf("Error fetching config from backend: %v", err)
		return
	}

	// sorting the sourceJSON.
	// json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	sort.Slice(sourceJSON.Sources, func(i, j int) bool {
		return sourceJSON.Sources[i].ID < sourceJSON.Sources[j].ID
	})

	bc.curSourceJSONLock.Lock()
	if !reflect.DeepEqual(bc.curSourceJSON, sourceJSON) {
		pkgLogger.Infof("Workspace Config changed: %s", workspaces)
		trackConfig(bc.curSourceJSON, sourceJSON)
		filteredSourcesJSON := filterProcessorEnabledDestinations(sourceJSON)
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

func (bc *backendConfigImpl) pollConfigUpdate(ctx context.Context, workspaces string) {
	statConfigBackendError := stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)
	for {
		bc.configUpdate(ctx, statConfigBackendError, workspaces)

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func getConfig() ConfigT {
	bc, _ := DefaultBackendConfig.(*backendConfigImpl)
	bc.curSourceJSONLock.RLock()
	defer bc.curSourceJSONLock.RUnlock()
	return bc.curSourceJSON
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

func newForDeployment(deploymentType deployment.Type, configEnvHandler types.ConfigEnvI) (BackendConfig, error) {
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
		}
	case deployment.MultiTenantType:
		isNamespaced := config.IsEnvSet("WORKSPACE_NAMESPACE")
		if isNamespaced {
			backendConfig.workspaceConfig = &namespaceConfig{
				ConfigBackendURL: parsedConfigBackendURL,
				configEnvHandler: configEnvHandler,
				cpRouterURL:      cpRouterURL,
			}
		} else {
			// DEPRECATED: This is the old way of configuring multi-tenant.
			backendConfig.workspaceConfig = &multiTenantWorkspacesConfig{
				configBackendURL: parsedConfigBackendURL,
				configEnvHandler: configEnvHandler,
				cpRouterURL:      cpRouterURL,
			}
		}
	default:
		return nil, fmt.Errorf("deployment type %q not supported", deploymentType)
	}

	return backendConfig, backendConfig.SetUp()
}

// Setup backend config
func Setup(configEnvHandler types.ConfigEnvI) (err error) {
	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("deployment type from env: %w", err)
	}

	backendConfig, err := newForDeployment(deploymentType, configEnvHandler)
	if err != nil {
		return err
	}

	DefaultBackendConfig = backendConfig

	adminpkg.RegisterAdminHandler("BackendConfig", &admin{})
	return nil
}

func (bc *backendConfigImpl) StartWithIDs(ctx context.Context, workspaces string) {
	ctx, cancel := context.WithCancel(ctx)
	bc.ctx = ctx
	bc.cancel = cancel
	bc.blockChan = make(chan struct{})
	rruntime.Go(func() {
		bc.pollConfigUpdate(ctx, workspaces)
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
		case <-time.After(pollInterval):
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
