package blendoRegistry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"

	"github.com/rudderlabs/rudder-server/config"
)

type BlendoSourceT struct {
	Role    string      `json:"role"`
	Options interface{} `json:"options"`
}

type BlendoDestinationT struct {
	Role    string      `json:"role"`
	Options interface{} `json:"options"`
}

type BlendoScheduleT struct {
	Type   string `json:"type"`
	Times  string `json:"times"`
	Hour   int    `json:"hour"`
	Minute int    `json:"minute"`
	Second int    `json:"second"`
}

type BlendoResourcesT struct {
	Role string `json:"role"`
}

type BlendoRegistryPipelineConfigT struct {
	Source    BlendoSourceT      `json:"source"`
	Sink      BlendoDestinationT `json:"sink"`
	Schedule  BlendoScheduleT    `json:"schedule"`
	Resources []BlendoResourcesT `json:"resources"`
	Paused    bool               `json:"paused"`
}
type BlendoRegistryI interface {
	Setup()
}

type BlendoRegistry struct {
	currentSourceJSON backendconfig.SourcesT
	isConfigSet       bool
}

var (
	configBackendURL, workspaceToken string
	blendoRegistryUrl                string
	pollInterval                     time.Duration
	Http                             sysUtils.HttpI   = sysUtils.NewHttp()
	IoUtil                           sysUtils.IoUtilI = sysUtils.NewIoUtil()
	log                              logger.LoggerI   = logger.NewLogger()
)

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	blendoRegistryUrl = config.GetEnv("BLENDO_REGISTRY_URL", "http://localhost:8181")
	pollInterval = config.GetDuration("BlendoConfig.pollIntervalInS", 5) * time.Second
}

func init() {
	config.Initialize()
	loadConfig()
}

// RequestToRegistry sends request to registry
func RequestToRegistry(id string, method string, data interface{}) (response []byte, ok bool) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/%s", blendoRegistryUrl, id)
	var dataJSONReader *bytes.Buffer
	if data != nil {
		dataJSON, _ := json.Marshal(data)
		dataJSONReader = bytes.NewBuffer(dataJSON)
	}
	request, err := Http.NewRequest(method, url, dataJSONReader)
	if err != nil {
		log.Errorf("BLENDO Registry: Failed to make %s request: %s, Error: %s", method, url, err.Error())
		return []byte{}, false
	}

	request.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(request)
	if err != nil {
		log.Errorf("BLENDO Registry: Failed to execute %s request: %s, Error: %s", method, url, err.Error())
		return []byte{}, false
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		log.Errorf("BLENDO Registry: Got error response %d", resp.StatusCode)
	}

	body, err := IoUtil.ReadAll(resp.Body)
	defer resp.Body.Close()

	log.Debugf("BLENDO Registry: Successful %s", string(body))
	return body, true
}

// putConfigToRegistry puts config in blendo registry
func (br *BlendoRegistry) putConfigToRegistry(id string, data interface{}) (response []byte, ok bool) {
	return RequestToRegistry(id, "PUT", data)
}

// deleteConfigFromRegistry deletes config from blendo registry
func (br *BlendoRegistry) deleteConfigFromRegistry(id string) (response []byte, ok bool) {
	return RequestToRegistry(id, "DELETE", nil)
}

// shouldMakeActionInSource returns if a configuration has changed and what action need to be done in registry (put or delete)
func (br *BlendoRegistry) shouldMakeActionInSource(source backendconfig.SourceT) (shouldMakeAction bool, action string) {
	if !br.isConfigSet {
		if source.Deleted {
			return true, "delete"
		} else {
			return true, "put"
		}
	}
	prevSources := br.currentSourceJSON.Sources
	for _, prevSource := range prevSources {
		if prevSource.ID == source.ID {
			if prevSource.Deleted != source.Deleted {
				action = "delete"
				shouldMakeAction = true
			} else if prevSource.Enabled != source.Enabled {
				shouldMakeAction = true
				action = "put"
			}
			break
		}
	}
	return shouldMakeAction, action
}

// shouldMakeActionInDestination returns if a configuration has changed when possible and what action need to be done in registry (put or delete)
func (br *BlendoRegistry) shouldMakeActionInDestination(sourceId string, destination backendconfig.DestinationT) (shouldMakeAction bool, action string) {
	if !br.isConfigSet {
		if destination.Deleted {
			return true, "delete"
		} else {
			return true, "put"
		}
	}
	prevSources := br.currentSourceJSON.Sources
	for _, prevSource := range prevSources {
		if prevSource.ID == sourceId {
			for _, prevDestination := range prevSource.Destinations {
				if prevDestination.ID == destination.ID {
					if prevDestination.Deleted != destination.Deleted {
						action = "delete"
						shouldMakeAction = true
					} else if prevDestination.Enabled != destination.Enabled {
						shouldMakeAction = true
						action = "put"
					}
					break
				}
			}
			break
		}
	}
	return shouldMakeAction, action
}

// getResources returns the resources of a pipeline in form that registry understands
func (br *BlendoRegistry) getResources(resources []string) []BlendoResourcesT {
	resourcesArray := []BlendoResourcesT{}
	for _, role := range resources {
		resourcesArray = append(resourcesArray, BlendoResourcesT{Role: role})
	}
	return resourcesArray
}

// getPipelineId returns a pipeline id tha is constructed with a combination of source id and destination id
func (br *BlendoRegistry) getPipelineId(sourceId string, destinationId string) string {
	return fmt.Sprintf("%s.%s", sourceId, destinationId)
}

// getConfig returns the configuration of the pipeline
func (br *BlendoRegistry) getConfig(source backendconfig.SourceT, destination backendconfig.DestinationT) BlendoRegistryPipelineConfigT {
	resources := br.getResources(source.Config["resources"].([]string))
	return BlendoRegistryPipelineConfigT{
		Source: BlendoSourceT{
			Role:    source.SourceDefinition.Name,
			Options: source.Config,
		},
		Sink: BlendoDestinationT{
			Role:    destination.DestinationDefinition.Name,
			Options: destination.Config,
		},
		Schedule: BlendoScheduleT{
			Type: "once_per_hour",
		},
		Resources: resources,
		Paused:    !source.Enabled || !destination.Enabled,
	}
}

// handleSources updates the registry accordingly
func (br *BlendoRegistry) handleSources(sources []backendconfig.SourceT) {
	for _, source := range sources {
		shouldSourceTakeAction, sourceAction := br.shouldMakeActionInSource(source)
		for _, destination := range source.Destinations {
			var action string
			var shouldTakeAction bool
			pipelineId := br.getPipelineId(source.ID, destination.ID)
			if shouldSourceTakeAction {
				action = sourceAction
				shouldTakeAction = true
			} else {
				shouldDestinationTakeAction, destinationAction := br.shouldMakeActionInDestination(source.ID, destination)
				action = destinationAction
				shouldTakeAction = shouldDestinationTakeAction
			}
			if shouldTakeAction {
				if action == "delete" {
					br.deleteConfigFromRegistry(pipelineId)
				} else {
					br.putConfigToRegistry(pipelineId, br.getConfig(source, destination))
				}
			}
		}
	}
}

// backendConfigSubscriber subscribes to backend-config change and updates the registry accordingly
func (br *BlendoRegistry) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		sources := config.Data.(backendconfig.SourcesT)
		br.handleSources(sources.Sources)
		br.currentSourceJSON = config.Data.(backendconfig.SourcesT)
		br.isConfigSet = true
	}
}

func (br *BlendoRegistry) Setup() {
	rruntime.Go(br.backendConfigSubscriber)
}
