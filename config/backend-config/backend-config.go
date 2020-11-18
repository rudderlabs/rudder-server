package backendconfig

//go:generate mockgen -destination=../../mocks/config/backend-config/mock_backendconfig.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/config/backend-config BackendConfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"time"

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
	configBackendURL, workspaceToken      string
	pollInterval, regulationsPollInterval time.Duration
	configFromFile                        bool
	configJSONPath                        string
	curSourceJSON                         SourcesT
	curSourceJSONLock                     sync.RWMutex
	curRegulationJSON                     RegulationsT
	curRegulationJSONLock                 sync.RWMutex
	initializedLock                       sync.RWMutex
	initialized                           bool
	waitForRegulations                    bool
	LastSync                              string
	LastRegulationSync                    string
	maxRegulationsPerRequest              int
	configEnvReplacementEnabled           bool
	configEnvHandler                      types.ConfigEnvI

	//DefaultBackendConfig will be initialized be Setup to either a WorkspaceConfig or MultiWorkspaceConfig.
	DefaultBackendConfig BackendConfig
	Http                 sysUtils.HttpI           = sysUtils.NewHttp()
	log                  logger.LoggerI           = logger.NewLogger()
	IoUtil               sysUtils.IoUtilI         = sysUtils.NewIoUtil()
	Diagnostics          diagnostics.DiagnosticsI = diagnostics.Diagnostics
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

	/*TopicRegulations topic provides updates on regulations, via Subscribe function */
	TopicRegulations Topic = "regulations"

	/*RegulationSuppress refers to Suppress Regulation */
	RegulationSuppress Regulation = "Suppress"

	//TODO Will add support soon.
	/*RegulationDelete refers to Suppress and Delete Regulation */
	RegulationDelete Regulation = "Delete"

	/*RegulationSuppressAndDelete refers to Suppress and Delete Regulation */
	RegulationSuppressAndDelete Regulation = "Suppress_With_Delete"
)

type DestinationDefinitionT struct {
	ID          string
	Name        string
	DisplayName string
	Config      map[string]interface{}
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
	ID               string
	Name             string
	SourceDefinition SourceDefinitionT
	Config           map[string]interface{}
	Enabled          bool
	WorkspaceID      string
	Destinations     []DestinationT
	WriteKey         string
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

type SourcesT struct {
	EnableMetrics bool      `json:"enableMetrics"`
	WorkspaceID   string    `json:"workspaceId"`
	Sources       []SourceT `json:"sources"`
}

type RegulationsT struct {
	WorkspaceRegulations []WorkspaceRegulationT `json:"workspaceRegulations"`
	SourceRegulations    []SourceRegulationT    `json:"sourceRegulations"`
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
	ID          string
	Name        string
	Description string
	VersionID   string
}

type BackendConfig interface {
	SetUp()
	Get() (SourcesT, bool)
	GetRegulations() (RegulationsT, bool)
	GetWorkspaceIDForWriteKey(string) string
	WaitForConfig()
	Subscribe(channel chan utils.DataEvent, topic Topic)
}
type CommonBackendConfig struct {
	configEnvHandler types.ConfigEnvI
}

func loadConfig() {
	// Rudder supporting multiple workspaces. false by default
	isMultiWorkspace = config.GetEnvAsBool("HOSTED_SERVICE", false)
	// Secret to be sent in basic auth for supporting multiple workspaces. password by default
	multiWorkspaceSecret = config.GetEnv("HOSTED_SERVICE_SECRET", "password")

	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	workspaceToken = config.GetWorkspaceToken()

	pollInterval = config.GetDuration("BackendConfig.pollIntervalInS", 5) * time.Second
	regulationsPollInterval = config.GetDuration("BackendConfig.regulationsPollIntervalInS", 300) * time.Second
	configJSONPath = config.GetString("BackendConfig.configJSONPath", "/etc/rudderstack/workspaceConfig.json")
	configFromFile = config.GetBool("BackendConfig.configFromFile", false)
	maxRegulationsPerRequest = config.GetInt("BackendConfig.maxRegulationsPerRequest", 1000)
	configEnvReplacementEnabled = config.GetBool("BackendConfig.envReplacementEnabled", true)
}

func MakePostRequest(url string, endpoint string, data interface{}) (response []byte, ok bool) {
	client := &http.Client{}
	backendURL := fmt.Sprintf("%s%s", url, endpoint)
	dataJSON, _ := json.Marshal(data)
	request, err := Http.NewRequest("POST", backendURL, bytes.NewBuffer(dataJSON))
	if err != nil {
		log.Errorf("ConfigBackend: Failed to make request: %s, Error: %s", backendURL, err.Error())
		return []byte{}, false
	}

	request.SetBasicAuth(workspaceToken, "")
	request.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(request)
	// Not handling errors when sending alert to victorops
	if err != nil {
		log.Errorf("ConfigBackend: Failed to execute request: %s, Error: %s", backendURL, err.Error())
		return []byte{}, false
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		log.Errorf("ConfigBackend: Got error response %d", resp.StatusCode)
	}

	body, err := IoUtil.ReadAll(resp.Body)
	defer resp.Body.Close()

	log.Debugf("ConfigBackend: Successful %s", string(body))
	return body, true
}

func MakeBackendPostRequest(endpoint string, data interface{}) (response []byte, ok bool) {
	return MakePostRequest(configBackendURL, endpoint, data)
}

func init() {
	loadConfig()
}

func trackConfig(preConfig SourcesT, curConfig SourcesT) {
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

func filterProcessorEnabledDestinations(config SourcesT) SourcesT {
	var modifiedSources SourcesT
	modifiedSources.Sources = make([]SourceT, 0)
	for _, source := range config.Sources {
		destinations := make([]DestinationT, 0)
		for _, destination := range source.Destinations {
			log.Debug(destination.Name, " IsProcessorEnabled: ", destination.IsProcessorEnabled)
			if destination.IsProcessorEnabled {
				destinations = append(destinations, destination)
			}
		}
		source.Destinations = destinations
		modifiedSources.Sources = append(modifiedSources.Sources, source)
	}
	return modifiedSources
}

func regulationsUpdate(statConfigBackendError stats.RudderStats) {

	regulationJSON, ok := backendConfig.GetRegulations()
	if !ok {
		statConfigBackendError.Increment()
	}

	//sorting the regulationJSON.
	//json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	sort.Slice(regulationJSON.WorkspaceRegulations[:], func(i, j int) bool {
		return regulationJSON.WorkspaceRegulations[i].ID < regulationJSON.WorkspaceRegulations[j].ID
	})
	sort.Slice(regulationJSON.SourceRegulations[:], func(i, j int) bool {
		return regulationJSON.SourceRegulations[i].ID < regulationJSON.SourceRegulations[j].ID
	})

	if ok && !reflect.DeepEqual(curRegulationJSON, regulationJSON) {
		log.Info("Regulations changed")
		curRegulationJSONLock.Lock()
		curRegulationJSON = regulationJSON
		curRegulationJSONLock.Unlock()
		initializedLock.Lock() //Using initializedLock for waitForRegulations too.
		defer initializedLock.Unlock()
		waitForRegulations = false
		LastRegulationSync = time.Now().Format(time.RFC3339)
		Eb.Publish(string(TopicRegulations), regulationJSON)
	}
}

func configUpdate(statConfigBackendError stats.RudderStats) {

	sourceJSON, ok := backendConfig.Get()
	if !ok {
		statConfigBackendError.Increment()
	}

	//sorting the sourceJSON.
	//json unmarshal does not guarantee order. For DeepEqual to work as expected, sorting is necessary
	sort.Slice(sourceJSON.Sources[:], func(i, j int) bool {
		return sourceJSON.Sources[i].ID < sourceJSON.Sources[j].ID
	})

	if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
		log.Info("Workspace Config changed")
		curSourceJSONLock.Lock()
		trackConfig(curSourceJSON, sourceJSON)
		filteredSourcesJSON := filterProcessorEnabledDestinations(sourceJSON)
		curSourceJSON = sourceJSON
		curSourceJSONLock.Unlock()
		initializedLock.Lock()
		defer initializedLock.Unlock()
		initialized = true
		LastSync = time.Now().Format(time.RFC3339)
		Eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
		Eb.Publish(string(TopicBackendConfig), sourceJSON)
	}
}

func pollConfigUpdate() {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		configUpdate(statConfigBackendError)
		time.Sleep(time.Duration(pollInterval))
	}
}

func pollRegulations() {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		regulationsUpdate(statConfigBackendError)
		time.Sleep(time.Duration(regulationsPollInterval))
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func GetWorkspaceIDForWriteKey(writeKey string) string {
	return backendConfig.GetWorkspaceIDForWriteKey(writeKey)
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
Data of the DataEvent should be a backendconfig.SourcesT struct.
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
	} else if topic == TopicRegulations {
		Eb.PublishToChannel(channel, string(topic), curRegulationJSON)
	}
	curSourceJSONLock.RUnlock()
}

/*
WaitForConfig waits until backend config has been initialized
Deprecated: Use an instance of BackendConfig instead of static function
*/
func WaitForConfig() {
	backendConfig.WaitForConfig()
}

/*
WaitForConfig waits until backend config has been initialized
*/
func (bc *CommonBackendConfig) WaitForConfig() {
	for {
		initializedLock.RLock()
		if initialized && !waitForRegulations {
			initializedLock.RUnlock()
			break
		}
		initializedLock.RUnlock()
		log.Info("Waiting for initializing backend config")
		time.Sleep(time.Duration(pollInterval))
	}
}

// Setup backend config
func Setup(pollRegulations bool, configEnvHandler types.ConfigEnvI) {
	if isMultiWorkspace {
		backendConfig = new(MultiWorkspaceConfig)
	} else {
		backendConfig = new(WorkspaceConfig)
		backendConfig.(*WorkspaceConfig).CommonBackendConfig.configEnvHandler = configEnvHandler
	}

	backendConfig.SetUp()

	DefaultBackendConfig = backendConfig

	rruntime.Go(func() {
		pollConfigUpdate()
	})

	if pollRegulations {
		startRegulationPolling()
	}

	admin.RegisterAdminHandler("BackendConfig", &BackendConfigAdmin{})
}

// startRegulationPolling - starts enterprise backend regulations polling
func startRegulationPolling() {
	waitForRegulations = true

	rruntime.Go(func() {
		pollRegulations()
	})
}
