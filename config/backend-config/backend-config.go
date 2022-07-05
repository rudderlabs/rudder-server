package backendconfig

//go:generate mockgen -destination=../../mocks/config/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/config/backend-config BackendConfig

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/admin"
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
	backendConfig                         BackendConfig
	configBackendURL                      string
	pollInterval, regulationsPollInterval time.Duration
	configFromFile                        bool
	configJSONPath                        string
	curSourceJSON                         ConfigT
	curSourceJSONLock                     sync.RWMutex
	initializedLock                       sync.RWMutex
	initialized                           bool
	LastSync                              string
	LastRegulationSync                    string
	maxRegulationsPerRequest              int
	configEnvReplacementEnabled           bool

	// DefaultBackendConfig will be initialized be Setup to either a WorkspaceConfig or MultiWorkspaceConfig.
	DefaultBackendConfig BackendConfig
	Http                 sysUtils.HttpI   = sysUtils.NewHttp()
	pkgLogger            logger.LoggerI   = logger.NewLogger().Child("backend-config")
	IoUtil               sysUtils.IoUtilI = sysUtils.NewIoUtil()
	Diagnostics          diagnostics.DiagnosticsI
)

type BackendConfig interface {
	SetUp()
	AccessToken() string
	Get(string) (ConfigT, bool)
	GetWorkspaceIDForWriteKey(string) string
	GetWorkspaceIDForSourceID(string) string
	GetWorkspaceLibrariesForWorkspaceID(string) LibrariesT
	WaitForConfig(ctx context.Context) error
	Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel
	Stop()
	StartWithIDs(workspaces string)
	IsConfigured() bool
}
type CommonBackendConfig struct {
	eb               *pubsub.PublishSubscriber
	configEnvHandler types.ConfigEnvI
	ctx              context.Context
	cancel           context.CancelFunc
	blockChan        chan struct{}
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")

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
			noOfDestinations = noOfDestinations + len(source.Destinations)
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
		for _, destination := range source.Destinations {
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

func configUpdate(eb *pubsub.PublishSubscriber, statConfigBackendError stats.RudderStats, workspaces string) {
	sourceJSON, ok := backendConfig.Get(workspaces)
	if !ok {
		statConfigBackendError.Increment()
		return
	}

	// sorting the sourceJSON.
	// json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	sort.Slice(sourceJSON.Sources[:], func(i, j int) bool {
		return sourceJSON.Sources[i].ID < sourceJSON.Sources[j].ID
	})

	if !reflect.DeepEqual(curSourceJSON, sourceJSON) {
		pkgLogger.Info("Workspace Config changed")
		curSourceJSONLock.Lock()
		trackConfig(curSourceJSON, sourceJSON)
		filteredSourcesJSON := filterProcessorEnabledDestinations(sourceJSON)
		curSourceJSON = sourceJSON
		curSourceJSONLock.Unlock()
		initializedLock.Lock()
		defer initializedLock.Unlock()
		initialized = true
		LastSync = time.Now().Format(time.RFC3339)
		eb.Publish(string(TopicBackendConfig), sourceJSON)
		eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
	}
}

func pollConfigUpdate(ctx context.Context, eb *pubsub.PublishSubscriber, workspaces string) {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		configUpdate(eb, statConfigBackendError, workspaces)

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func GetConfig() ConfigT {
	return curSourceJSON
}

func GetWorkspaceIDForWriteKey(writeKey string) string {
	return backendConfig.GetWorkspaceIDForWriteKey(writeKey)
}

func GetWorkspaceIDForSourceID(sourceID string) string {
	return backendConfig.GetWorkspaceIDForSourceID(sourceID)
}

func GetWorkspaceLibrariesForWorkspaceID(workspaceId string) LibrariesT {
	return backendConfig.GetWorkspaceLibrariesForWorkspaceID(workspaceId)
}

/*
Subscribe subscribes a channel to a specific topic of backend config updates.
Deprecated: Use an instance of BackendConfig instead of static function
*/
func Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel {
	return backendConfig.Subscribe(ctx, topic)
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
func (bc *CommonBackendConfig) Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel {
	return bc.eb.Subscribe(ctx, string(topic))
}

/*
WaitForConfig waits until backend config has been initialized
Deprecated: Use an instance of BackendConfig instead of static function
*/
func WaitForConfig(ctx context.Context) error {
	return backendConfig.WaitForConfig(ctx)
}

/*
WaitForConfig waits until backend config has been initialized
*/
func (bc *CommonBackendConfig) WaitForConfig(ctx context.Context) error {
	for {
		initializedLock.RLock()
		if initialized {
			initializedLock.RUnlock()
			break
		}
		initializedLock.RUnlock()
		pkgLogger.Info("Waiting for initializing backend config")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	return nil
}

func newForDeployment(deploymentType deployment.Type, configEnvHandler types.ConfigEnvI) (BackendConfig, error) {
	var backendConfig BackendConfig

	switch deploymentType {
	case deployment.DedicatedType:
		backendConfig = &SingleWorkspaceConfig{
			CommonBackendConfig: CommonBackendConfig{
				configEnvHandler: configEnvHandler,
				eb:               pubsub.New(),
			},
		}
	case deployment.HostedType:
		backendConfig = &HostedWorkspacesConfig{
			CommonBackendConfig: CommonBackendConfig{
				eb: pubsub.New(),
			},
		}
	case deployment.MultiTenantType:
		backendConfig = &MultiTenantWorkspacesConfig{
			CommonBackendConfig: CommonBackendConfig{
				configEnvHandler: configEnvHandler,
				eb:               pubsub.New(),
			},
		}
	// Fallback to dedicated
	default:
		return nil, fmt.Errorf("Deployment type %q not supported", deploymentType)
	}

	backendConfig.SetUp()
	if !backendConfig.IsConfigured() {
		return nil, fmt.Errorf("backend config token not available")
	}

	return backendConfig, nil
}

// Setup backend config
func Setup(configEnvHandler types.ConfigEnvI) (err error) {
	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("deployment type from env: %w", err)
	}

	backendConfig, err = newForDeployment(deploymentType, configEnvHandler)
	if err != nil {
		return err
	}

	DefaultBackendConfig = backendConfig

	admin.RegisterAdminHandler("BackendConfig", &BackendConfigAdmin{})
	return nil
}

func (bc *CommonBackendConfig) StartWithIDs(workspaces string) {
	ctx, cancel := context.WithCancel(context.Background())
	bc.ctx = ctx
	bc.cancel = cancel
	bc.blockChan = make(chan struct{})
	rruntime.Go(func() {
		pollConfigUpdate(ctx, bc.eb, workspaces)
		close(bc.blockChan)
	})
}

func (bc *CommonBackendConfig) Stop() {
	bc.cancel()
	<-bc.blockChan
	initializedLock.Lock()
	initialized = false
	initializedLock.Unlock()
}

func GetConfigBackendURL() string {
	return configBackendURL
}
