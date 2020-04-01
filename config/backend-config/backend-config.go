package backendconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
)

var (
	backendConfig                    BackendConfig
	isMultiWorkspace                 bool
	multiWorkspaceSecret             string
	configBackendURL, workspaceToken string
	pollInterval                     time.Duration
	configFromFile                   bool
	configJSONPath                   string
	curSourceJSON                    SourcesT
	curSourceJSONLock                sync.RWMutex
	initialized                      bool
	LastSync                         string
)

var Eb = new(utils.EventBus)

type DestinationDefinitionT struct {
	ID          string
	Name        string
	DisplayName string
	Config      interface{}
}

type SourceDefinitionT struct {
	ID   string
	Name string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                interface{}
	Enabled               bool
	Transformations       []TransformationT
	IsProcessorEnabled    bool
}

type SourceT struct {
	ID               string
	Name             string
	SourceDefinition SourceDefinitionT
	Config           interface{}
	Enabled          bool
	Destinations     []DestinationT
	WriteKey         string
}

type SourcesT struct {
	Sources []SourceT `json:"sources"`
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
	GetWorkspaceIDForWriteKey(string) string
}

func loadConfig() {
	// Rudder supporting multiple workspaces. false by default
	isMultiWorkspace = config.GetEnvAsBool("HOSTED_SERVICE", false)
	// Secret to be sent in basic auth for supporting multiple workspaces. password by default
	multiWorkspaceSecret = config.GetEnv("HOSTED_SERVICE_SECRET", "password")

	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	workspaceToken = config.GetWorkspaceToken()

	pollInterval = config.GetDuration("BackendConfig.pollIntervalInS", 5) * time.Second
	configJSONPath = config.GetString("BackendConfig.configJSONPath", "/etc/rudderstack/workspaceConfig.json")
	configFromFile = config.GetBool("BackendConfig.configFromFile", false)
}

func MakePostRequest(url string, endpoint string, data interface{}) (response []byte, ok bool) {
	client := &http.Client{}
	backendURL := fmt.Sprintf("%s%s", url, endpoint)
	dataJSON, _ := json.Marshal(data)
	request, err := http.NewRequest("POST", backendURL, bytes.NewBuffer(dataJSON))
	if err != nil {
		logger.Errorf("ConfigBackend: Failed to make request: %s, Error: %s", backendURL, err.Error())
		return []byte{}, false
	}

	request.SetBasicAuth(workspaceToken, "")
	request.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(request)
	// Not handling errors when sending alert to victorops
	if err != nil {
		logger.Errorf("ConfigBackend: Failed to make request: %s, Error: %s", backendURL, err.Error())
		return []byte{}, false
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		logger.Errorf("ConfigBackend: Got error response %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	logger.Debugf("ConfigBackend: Successful %s", string(body))
	return body, true
}

func MakeBackendPostRequest(endpoint string, data interface{}) (response []byte, ok bool) {
	return MakePostRequest(configBackendURL, endpoint, data)
}

func init() {
	config.Initialize()
	loadConfig()
}

func filterProcessorEnabledDestinations(config SourcesT) SourcesT {
	var modifiedSources SourcesT
	modifiedSources.Sources = make([]SourceT, 0)
	for _, source := range config.Sources {
		destinations := make([]DestinationT, 0)
		for _, destination := range source.Destinations {
			logger.Debug(destination.Name, " IsProcessorEnabled: ", destination.IsProcessorEnabled)
			if destination.IsProcessorEnabled {
				destinations = append(destinations, destination)
			}
		}
		source.Destinations = destinations
		modifiedSources.Sources = append(modifiedSources.Sources, source)
	}
	return modifiedSources
}

func pollConfigUpdate() {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		sourceJSON, ok := backendConfig.Get()
		if !ok {
			statConfigBackendError.Increment()
		}
		if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
			logger.Info("Workspace Config changed")
			curSourceJSONLock.Lock()
			filteredSourcesJSON := filterProcessorEnabledDestinations(sourceJSON)
			curSourceJSON = sourceJSON
			curSourceJSONLock.Unlock()
			initialized = true
			LastSync = time.Now().Format(time.RFC3339)
			Eb.Publish("processConfig", filteredSourcesJSON)
			Eb.Publish("backendConfig", sourceJSON)
		}
		time.Sleep(time.Duration(pollInterval))
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func GetWorkspaceIDForWriteKey(writeKey string) string {
	return backendConfig.GetWorkspaceIDForWriteKey(writeKey)
}

func Subscribe(channel chan utils.DataEvent, topic string) {
	Eb.Subscribe(topic, channel)
	curSourceJSONLock.RLock()
	filteredSourcesJSON := filterProcessorEnabledDestinations(curSourceJSON)

	if topic == "processConfig" {
		Eb.PublishToChannel(channel, topic, filteredSourcesJSON)
	} else if topic == "backendConfig" {
		Eb.PublishToChannel(channel, topic, curSourceJSON)
	}
	curSourceJSONLock.RUnlock()
}

func WaitForConfig() {
	for {
		if initialized {
			break
		}
		logger.Info("Waiting for initializing backend config")
		time.Sleep(time.Duration(pollInterval))

	}
}

// Setup backend config
func Setup() {
	if isMultiWorkspace {
		backendConfig = new(MultiWorkspaceConfig)
	} else {
		backendConfig = new(WorkspaceConfig)
	}

	backendConfig.SetUp()

	rruntime.Go(func() {
		pollConfigUpdate()
	})
}
