package backendconfig

//go:generate mockgen -destination=../../mocks/config/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/config/backend-config BackendConfig

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-server/admin"

	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var (
	backendConfig                         BackendConfig
	isMultiWorkspace                      bool
	multiWorkspaceSecret                  string
	multitenantWorkspaceSecret            string
	configBackendURL, workspaceToken      string
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

	//DefaultBackendConfig will be initialized be Setup to either a WorkspaceConfig or MultiWorkspaceConfig.
	DefaultBackendConfig BackendConfig
	Http                 sysUtils.HttpI   = sysUtils.NewHttp()
	pkgLogger            logger.LoggerI   = logger.NewLogger().Child("backend-config")
	IoUtil               sysUtils.IoUtilI = sysUtils.NewIoUtil()
	Diagnostics          diagnostics.DiagnosticsI
)

var Eb utils.PublishSubscriber = new(utils.EventBus)

// Topic refers to a subset of backend config's updates, received after subscribing using the backend config's Subscribe function.
type Topic string

type Regulation string

const (
	/*TopicBackendConfig topic provides updates on full backend config, via Subscribe function */
	TopicBackendConfig Topic = "backendConfig"

	/*TopicProcessConfig topic provides updates on backend config of processor enabled destinations, via Subscribe function */
	TopicProcessConfig Topic = "processConfig"

	/*RegulationSuppress refers to Suppress Regulation */
	RegulationSuppress Regulation = "Suppress"

	//TODO Will add support soon.
	/*RegulationDelete refers to Suppress and Delete Regulation */
	RegulationDelete Regulation = "Delete"

	/*RegulationSuppressAndDelete refers to Suppress and Delete Regulation */
	RegulationSuppressAndDelete Regulation = "Suppress_With_Delete"

	GlobalEventType = "global"
)

type DestinationDefinitionT struct {
	ID            string
	Name          string
	DisplayName   string
	Config        map[string]interface{}
	ResponseRules map[string]interface{}
}

type SourceDefinitionT struct {
	ID       string
	Name     string
	Category string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                map[string]interface{}
	Enabled               bool
	Transformations       []TransformationT
	IsProcessorEnabled    bool
}

type SourceT struct {
	ID                         string
	Name                       string
	SourceDefinition           SourceDefinitionT
	Config                     map[string]interface{}
	Enabled                    bool
	WorkspaceID                string
	Destinations               []DestinationT
	WriteKey                   string
	DgSourceTrackingPlanConfig DgSourceTrackingPlanConfigT
}

type WorkspaceRegulationT struct {
	ID             string
	RegulationType string
	WorkspaceID    string
	UserID         string
}

type SourceRegulationT struct {
	ID             string
	RegulationType string
	WorkspaceID    string
	SourceID       string
	UserID         string
}

type ConfigT struct {
	EnableMetrics   bool            `json:"enableMetrics"`
	WorkspaceID     string          `json:"workspaceId"`
	Sources         []SourceT       `json:"sources"`
	Libraries       LibrariesT      `json:"libraries"`
	ConnectionFlags ConnectionFlags `json:"flags"`
}

type ConnectionFlags struct {
	URL      string          `json:"url"`
	Services map[string]bool `json:"services"`
}

type WRegulationsT struct {
	WorkspaceRegulations []WorkspaceRegulationT `json:"workspaceRegulations"`
	Start                int                    `json:"start"`
	Limit                int                    `json:"limit"`
	Size                 int                    `json:"size"`
	End                  bool                   `json:"end"`
	Next                 int                    `json:"next"`
}

type SRegulationsT struct {
	SourceRegulations []SourceRegulationT `json:"sourceRegulations"`
	Start             int                 `json:"start"`
	Limit             int                 `json:"limit"`
	Size              int                 `json:"size"`
	End               bool                `json:"end"`
	Next              int                 `json:"next"`
}

type TransformationT struct {
	VersionID string
	ID        string
	Config    map[string]interface{}
}

type LibraryT struct {
	VersionID string
}

type LibrariesT []LibraryT

type DgSourceTrackingPlanConfigT struct {
	SourceId            string                            `json:"sourceId"`
	SourceConfigVersion int                               `json:"version"`
	Config              map[string]map[string]interface{} `json:"config"`
	MergedConfig        map[string]interface{}            `json:"mergedConfig"`
	Deleted             bool                              `json:"deleted"`
	TrackingPlan        TrackingPlanT                     `json:"trackingPlan"`
}

func (dgSourceTPConfigT *DgSourceTrackingPlanConfigT) GetMergedConfig(eventType string) map[string]interface{} {
	if dgSourceTPConfigT.MergedConfig == nil {
		globalConfig := dgSourceTPConfigT.fetchEventConfig(GlobalEventType)
		eventSpecificConfig := dgSourceTPConfigT.fetchEventConfig(eventType)
		outputConfig := misc.MergeMaps(globalConfig, eventSpecificConfig)
		dgSourceTPConfigT.MergedConfig = outputConfig
	}
	return dgSourceTPConfigT.MergedConfig
}

func (dgSourceTPConfigT *DgSourceTrackingPlanConfigT) fetchEventConfig(eventType string) map[string]interface{} {
	emptyMap := map[string]interface{}{}
	_, eventSpecificConfigPresent := dgSourceTPConfigT.Config[eventType]
	if !eventSpecificConfigPresent {
		return emptyMap
	}
	return dgSourceTPConfigT.Config[eventType]
}

type TrackingPlanT struct {
	Id      string `json:"id"`
	Version int    `json:"version"`
}

type BackendConfig interface {
	SetUp()
	Get(string) (ConfigT, bool)
	GetWorkspaceIDForWriteKey(string) string
	GetWorkspaceIDForSourceID(string) string
	GetWorkspaceLibrariesForWorkspaceID(string) LibrariesT
	WaitForConfig(ctx context.Context) error
	Subscribe(channel chan utils.DataEvent, topic Topic)
	StopPolling()
	StartPolling(workspaces string)
}
type CommonBackendConfig struct {
	configEnvHandler types.ConfigEnvI
	ctx              context.Context
	cancel           context.CancelFunc
	blockChan        chan struct{}
}

func loadConfig() {
	// Rudder supporting multiple workspaces. false by default
	isMultiWorkspace = config.GetEnvAsBool("HOSTED_SERVICE", false)
	// Secret to be sent in basic auth for supporting multiple workspaces. password by default
	multiWorkspaceSecret = config.GetEnv("HOSTED_SERVICE_SECRET", "password")
	multitenantWorkspaceSecret = config.GetEnv("HOSTED_MULTITENANT_SERVICE_SECRET", "password")
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	workspaceToken = config.GetWorkspaceToken()

	config.RegisterDurationConfigVariable(time.Duration(5), &pollInterval, true, time.Second, []string{"BackendConfig.pollInterval", "BackendConfig.pollIntervalInS"}...)

	config.RegisterDurationConfigVariable(time.Duration(300), &regulationsPollInterval, true, time.Second, []string{"BackendConfig.regulationsPollInterval", "BackendConfig.regulationsPollIntervalInS"}...)
	config.RegisterStringConfigVariable("/etc/rudderstack/workspaceConfig.json", &configJSONPath, false, "BackendConfig.configJSONPath")
	config.RegisterBoolConfigVariable(false, &configFromFile, false, "BackendConfig.configFromFile")
	config.RegisterIntConfigVariable(1000, &maxRegulationsPerRequest, true, 1, "BackendConfig.maxRegulationsPerRequest")
	config.RegisterBoolConfigVariable(true, &configEnvReplacementEnabled, false, "BackendConfig.envReplacementEnabled")
}

func Init() {
	Diagnostics = diagnostics.Diagnostics
	loadConfig()
}

func trackConfig(preConfig ConfigT, curConfig ConfigT) {
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

func configUpdate(statConfigBackendError stats.RudderStats, workspaces string) {

	sourceJSON, ok := backendConfig.Get(workspaces)
	if !ok {
		statConfigBackendError.Increment()
	}

	//sorting the sourceJSON.
	//json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	sort.Slice(sourceJSON.Sources[:], func(i, j int) bool {
		return sourceJSON.Sources[i].ID < sourceJSON.Sources[j].ID
	})

	if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
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
		Eb.Publish(string(TopicBackendConfig), sourceJSON)
		Eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
	}
}

func pollConfigUpdate(ctx context.Context, workspaces string) {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		configUpdate(statConfigBackendError, workspaces)

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
func Subscribe(channel chan utils.DataEvent, topic Topic) {
	backendConfig.Subscribe(channel, topic)
}

/*
Subscribe subscribes a channel to a specific topic of backend config updates.
Channel will receive a new utils.DataEvent each time the backend configuration is updated.
Data of the DataEvent should be a backendconfig.ConfigT struct.
Available topics are:
- TopicBackendConfig: Will receive complete backend configuration
- TopicProcessConfig: Will receive only backend configuration of processor enabled destinations
- TopicRegulations: Will receeive all regulations
*/
func (bc *CommonBackendConfig) Subscribe(channel chan utils.DataEvent, topic Topic) {
	Eb.Subscribe(string(topic), channel)
	curSourceJSONLock.RLock()

	if topic == TopicProcessConfig {
		filteredSourcesJSON := filterProcessorEnabledDestinations(curSourceJSON)
		Eb.PublishToChannel(channel, string(topic), filteredSourcesJSON)
	} else if topic == TopicBackendConfig {
		Eb.PublishToChannel(channel, string(topic), curSourceJSON)
	}
	curSourceJSONLock.RUnlock()
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
		case <-time.After(time.Duration(pollInterval)):
		}
	}
	return nil
}

// Setup backend config
func Setup(configEnvHandler types.ConfigEnvI) {
	if isMultiWorkspace && misc.IsMultiTenant() {
		backendConfig = new(MultiTenantWorkspaceConfig)
		backendConfig.(*MultiTenantWorkspaceConfig).CommonBackendConfig.configEnvHandler = configEnvHandler
	} else if isMultiWorkspace {
		backendConfig = new(MultiWorkspaceConfig)
	} else {
		backendConfig = new(WorkspaceConfig)
		backendConfig.(*WorkspaceConfig).CommonBackendConfig.configEnvHandler = configEnvHandler
	}

	backendConfig.SetUp()

	DefaultBackendConfig = backendConfig

	admin.RegisterAdminHandler("BackendConfig", &BackendConfigAdmin{})
}

func (bc *CommonBackendConfig) StartPolling(workspaces string) {
	ctx, cancel := context.WithCancel(context.Background())
	bc.ctx = ctx
	bc.cancel = cancel
	bc.blockChan = make(chan struct{})
	rruntime.Go(func() {
		pollConfigUpdate(ctx, workspaces)
		close(bc.blockChan)
	})
}

func (bc *CommonBackendConfig) StopPolling() {
	bc.cancel()
	<-bc.blockChan
	initializedLock.Lock()
	initialized = false
	initializedLock.Unlock()
}

func GetConfigBackendURL() string {
	return configBackendURL
}

// Gets the workspace token data for a single workspace or multi workspace case
func GetWorkspaceToken() (workspaceToken string) {
	workspaceToken = config.GetWorkspaceToken()
	isMultiWorkspace := config.GetEnvAsBool("HOSTED_SERVICE", false)
	if misc.IsMultiTenant() && isMultiWorkspace {
		workspaceToken = config.GetEnv("HOSTED_MULTITENANT_SERVICE_SECRET", "password")
	} else if isMultiWorkspace {
		workspaceToken = config.GetEnv("HOSTED_SERVICE_SECRET", "password")
	}

	return workspaceToken
}
