package backendconfig

// go:generate mockgen -destination=../mocks/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/backend-config BackendConfig
// go:generate mockgen -destination=./mock_workspaceconfig.go -package=backendconfig -source=./backend-config.go workspaceConfig

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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/backend-config/internal/cache"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/samber/lo"
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
	cacheOverride        cache.Cache
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
	return modifiedConfig
}

func (bc *backendConfigImpl) configUpdate(ctx context.Context, workspaces string) {
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
		if len(workspaces) > 0 {
			pkgLogger.Infof("Workspace Config changed: %d", len(workspaces))
		} else {
			pkgLogger.Infof("Workspace Config changed")
		}

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

func (bc *backendConfigImpl) pollConfigUpdate(ctx context.Context, workspaces string) {
	for {
		bc.configUpdate(ctx, workspaces)

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
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
			configBackendURL: parsedConfigBackendURL,
			configEnvHandler: configEnvHandler,
			cpRouterURL:      cpRouterURL,
			region:           region,
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

func (bc *backendConfigImpl) StartWithIDs(ctx context.Context, workspaces string) {
	var err error
	ctx, cancel := context.WithCancel(ctx)
	bc.ctx = ctx
	bc.cancel = cancel
	bc.blockChan = make(chan struct{})
	bc.cache = cacheOverride
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

/*
Lets the caller know if the event is allowed to flow through server for a `specific destination`
Introduced to support hybrid-mode, cloud-mode in a more scalable way

The template inside `destinationDefinition.Config.supportedConnectionModes` would look like this
```

	[connectionMode]: {
		[sourceType]: {
			[eventProperty]: [...supportedEventPropertyValues]
		}
	}

```

Example:

	{
		...
		"supportConnectionMode": {
	    "device": {
				"web": {
					"messageType": ["track", "page"]
				},
				"cloud": {
					"messageType": ["default"]
				},
				"cordova":{
					"messageType": ["screen", "page"]
				}
			},
		  "hybrid": {
				"web": {
					"messageType": ["track", "page", "screen"]
				},
				"ios": {
					"messageType": ["track","page", "screen"]
				}
			},
	  },
		...
	}
*/
func (destination *DestinationT) AllowEventToDestination(source *SourceT, event types.SingularEventT) bool {
	// We are setting this value through logic in rudder-server
	// Ideally we need to send this information from config-backend itself
	srcType := source.SourceDefinition.Type

	// Event-Type default check
	eventTypeI := misc.MapLookup(event, "type")
	if eventTypeI == nil {
		pkgLogger.Error("Event type is not being sent for the event")
		// We will allow the event to be sent to destination transformation
		return true
	}
	eventType, isEventTypeString := eventTypeI.(string)
	if !isEventTypeString {
		// Seems like it makes sense!
		pkgLogger.Errorf("Given event type :%v, cannot be casted to string", eventTypeI)
		return false
	}
	isSupportedMsgType := evaluateSupportedTypes(destination.DestinationDefinition.Config, "supportedMessageTypes", eventType)

	// Default behavior
	// When something is missing in "supportedConnectionModes" or if "supportedConnectionModes" is not defined
	// We would be checking for below things
	// 1. Check if the event.type value is present in destination.DestinationDefinition.Config["supportedMessageTypes"]
	// 2. Check if the connectionMode of destination is cloud or hybrid(evaluated through `IsProcessorEnabled`)
	// Only when 1 & 2 are true, we would allow the event to flow through to server
	evaluatedDefaultBehaviour := isSupportedMsgType && destination.IsProcessorEnabled

	/*
		In future(near one) this will not be a string but rather an object
		For every connection made to destination, we would basically have
		```
		{
			connectionMode: {
				[srcType]: oneof[cloud, hybrid, device]
			}
		}

		```
		Example
		{
			...
			"connectionMode": {
				"web": "", // one of cloud, hybrid or device
				"cordova": "", // one of cloud, hybrid or device
				"python": "", // one of cloud, hybrid or device
				"cloud": "" // one of cloud, hybrid or device
			}
			...
		}
	*/
	// New logic for evaluating "connectionMode"
	// var destConnModeI interface{}
	// for _, val := range []string{srcType, strings.ToLower(source.SourceDefinition.Name)} {
	// 	destConnModeI = misc.MapLookup(destination.Config, "connectionMode", val)
	// 	if destConnModeI != nil {
	// 		break
	// 	}
	// }
	destConnModeI := misc.MapLookup(destination.Config, "connectionMode")
	if destConnModeI == nil {
		return evaluatedDefaultBehaviour
	}
	destConnectionMode, isDestConnModeMapOfString := destConnModeI.(string)
	if !isDestConnModeMapOfString {
		pkgLogger.Errorf("Given destination connection mode :%v, cannot be casted to string", destConnModeI)
		return false
	}

	supportedConnectionModesI, connModesOk := destination.DestinationDefinition.Config["supportedConnectionModes"]
	if !connModesOk {
		// Probably the "supportedConnectionModes" key is not present, so we rely on Default behaviour
		return evaluatedDefaultBehaviour
	}
	supportedConnectionModes := supportedConnectionModesI.(map[string]interface{})

	supportedEventPropsMapI := misc.MapLookup(supportedConnectionModes, destConnectionMode, srcType)
	if supportedEventPropsMapI == nil {
		// Most probably sourceType would not be present in "supportedConnectionModes"
		// Might also occur when destination connection mode is not provided in "supportedConnectionModes"
		return evaluatedDefaultBehaviour
	}
	supportedEventPropsMap, isEventPropsMapTypeCastable := supportedEventPropsMapI.(map[string]interface{})
	if !isEventPropsMapTypeCastable {
		// fallback to default
		return evaluatedDefaultBehaviour
	}
	// Flag indicating to let the event pass through
	allowEvent := evaluatedDefaultBehaviour
	for eventProperty, supportedEventVals := range supportedEventPropsMap {
		if !allowEvent {
			allowEvent = evaluatedDefaultBehaviour
			break
		}
		if eventProperty == "messageType" {
			supportedVals := ConvertToArrayOfType[string](supportedEventVals)
			pkgLogger.Debugf("SupportedVals: %v -- EventType from event: %v\n", supportedVals, eventType)
			allowEvent = lo.Contains(supportedVals, eventType) && evaluatedDefaultBehaviour
		}
	}
	return allowEvent
}

type EventPropsTypes interface {
	~string
}

func evaluateSupportedTypes[T EventPropsTypes](destConfig map[string]interface{}, evalKey string, checkValue T) bool {
	if evalKey != "supportedMessageTypes" {
		return false
	}
	supportedValsI := misc.MapLookup(destConfig, evalKey)
	if supportedValsI == nil {
		return false
	}
	supportedVals := ConvertToArrayOfType[T](supportedValsI)
	return lo.Contains(supportedVals, checkValue)
}

func ConvertToArrayOfType[T EventPropsTypes](data interface{}) []T {
	switch value := data.(type) {
	case []interface{}:
		result := make([]T, len(value))
		for i, v := range value {
			var ok bool
			result[i], ok = v.(T)
			if !ok {
				return []T{}
			}
		}
		return result
	}
	return []T{}
}
