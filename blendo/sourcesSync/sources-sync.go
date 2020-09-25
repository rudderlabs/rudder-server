package sourcesSync

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
)

type SourceT struct {
	Role      string               `json:"role"`
	Options   interface{}          `json:"options"`
	WriteKey  string               `json:"write_key"`
	Resources map[string]ResourceT `json:"resources"`
}

type SourcesScheduleT struct {
	Every   int    `json:"every"`
	Unit    string `json:"unit"`
	StartAt string `json:"start_at"`
	EndAt   string `json:"end_at"`
}

type ResourceT struct {
	Role    string          `json:"role"`
	Options json.RawMessage `json:"options"`
}

type SourceConfigT struct {
	Source   SourceT          `json:"source"`
	Schedule SourcesScheduleT `json:"schedule"`
}
type SourcesClientI interface {
	DeleteConfigFromRegistry(id string) (response []byte, ok bool)
	PutConfigToRegistry(id string, data interface{}) (response []byte, ok bool)
}

type SourcesSync struct {
	currentSourceJSON backendconfig.SourcesT
	isConfigSet       bool
	SourcesClient     SourcesClientI
}

var (
	configBackendURL, workspaceToken string
	pollInterval                     time.Duration
	log                              logger.LoggerI = logger.NewLogger()
	sourcesURL                       string
)

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	sourcesURL = config.GetEnv("BLENDO_REGISTRY_URL", "http://localhost:8642")
	pollInterval = config.GetDuration("BlendoConfig.pollIntervalInS", 5) * time.Second
}

func init() {
	config.Initialize()
	loadConfig()
}

//NewSourcesSync Returns an instance of SourcesSync
func NewSourcesSync() *SourcesSync {
	return &SourcesSync{
		SourcesClient: &SourcesClient{SourcesURL: sourcesURL},
	}
}

// checkSourceDestinations check is all destinations are deleted or a destination added to the source
// returns true is an action has been made
func (br *SourcesSync) checkSourceDestinations(source backendconfig.SourceT, prevSource backendconfig.SourceT) bool {
	// if source deleted, delete all the pipelines from registry for this source
	destinationDeletedCount := 0
	prevDestinationDeletedCount := 0
	destinationNewCount := 0
	pipelineID := br.getPipelineID(source.ID)
	if !reflect.DeepEqual(source.Destinations, prevSource.Destinations) {
		for _, destination := range source.Destinations {
			destinationExists := false
			for _, prevDestination := range prevSource.Destinations {
				if prevDestination.Deleted || !prevDestination.IsConnectionEnabled || !prevDestination.Enabled {
					prevDestinationDeletedCount++
				}
				if prevDestination.ID == destination.ID {
					destinationExists = true
					// if destination deleted state has change (this means that deleted is true) remove the pipeline from registry
					if destination.Deleted != prevDestination.Deleted ||
						(destination.IsConnectionEnabled != prevDestination.IsConnectionEnabled &&
							!destination.IsConnectionEnabled) || (destination.Enabled != prevDestination.Enabled &&
						!destination.Enabled) {
						destinationDeletedCount++
						// if destination or source enabled or connection status or configuration has change then update registry configuration
					}
					if (destination.IsConnectionEnabled != prevDestination.IsConnectionEnabled &&
						destination.IsConnectionEnabled) || (destination.Enabled != prevDestination.Enabled &&
						destination.Enabled) {
						destinationNewCount++
						// if destination or source enabled or connection status or configuration has change then update registry configuration
					}
				}
			}
			// if destination not found in the previous destinations, means is new
			if !destinationExists && !destination.Deleted && destination.IsConnectionEnabled {
				destinationNewCount++
			}
		}
	}
	sourceDestinationsLength := len(source.Destinations)
	prevDestinationLength := len(prevSource.Destinations)
	if destinationDeletedCount == sourceDestinationsLength && destinationNewCount == 0 {
		br.SourcesClient.DeleteConfigFromRegistry(pipelineID)
		return true
	} else if prevDestinationLength == prevDestinationDeletedCount && destinationNewCount > 0 {
		br.SourcesClient.PutConfigToRegistry(pipelineID, br.getConfig(source))
		return true
	}
	return false
}

// calculateDifferencesAndUpdateRegistry calculate the differences from the previous config and make the coresponding actions
// we need to update blendo registry when the destination or source configuration or enabled status change
// and delete from blendo registry when the connection is no longer exists or a source or destination is deleted
func (br *SourcesSync) calculateDifferencesAndUpdateRegistry(sources []backendconfig.SourceT) {
	prevSources := br.currentSourceJSON.Sources
	for _, source := range sources {
		pipelineID := br.getPipelineID(source.ID)
		if source.SourceDefinition.Category != "cloud" {
			continue
		}
		sourceExists := false
		if br.isConfigSet {
			for _, prevSource := range prevSources {
				if prevSource.ID == source.ID {
					sourceExists = true
					if source.Deleted != prevSource.Deleted || (source.Enabled != prevSource.Enabled && !source.Enabled) {
						br.SourcesClient.DeleteConfigFromRegistry(pipelineID)
						continue
					} else if !br.checkSourceDestinations(source, prevSource) {
						if !reflect.DeepEqual(source.Config, prevSource.Config) || (source.Enabled != prevSource.Enabled && source.Enabled) {
							br.SourcesClient.PutConfigToRegistry(pipelineID, br.getConfig(source))
						}
					}
				}
			}
		}
		// if source not found in the previous sources mean is new
		if !sourceExists {
			destinationDeletedCount := 0
			for _, destination := range source.Destinations {
				if !destination.IsConnectionEnabled || destination.Deleted {
					destinationDeletedCount++
					// if destination or source enabled or connection status or configuration has change then update registry configuration
				}
			}
			if destinationDeletedCount == len(source.Destinations) {
				br.SourcesClient.DeleteConfigFromRegistry(pipelineID)
			} else {
				br.SourcesClient.PutConfigToRegistry(pipelineID, br.getConfig(source))
			}
		}
	}
}

// getResources returns the resources of a pipeline in form that registry understands
func (br *SourcesSync) getResources(resources []interface{}) map[string]ResourceT {
	resourcesMap := make(map[string]ResourceT)
	for _, role := range resources {
		resourcesMap[role.(string)] = ResourceT{Role: role.(string)}
	}
	return resourcesMap
}

// getPipelineID returns a pipeline id tha is constructed with a combination of source id and destination id
func (br *SourcesSync) getPipelineID(sourceID string) string {
	return sourceID
}

func (br *SourcesSync) mapDestinationconfig(config map[string]interface{}) map[string]interface{} {
	desConfog := make(map[string]interface{})
	for key, value := range config {
		if key == "user" {
			desConfog["username"] = value
		} else if key == "port" {
			port, _ := strconv.Atoi(value.(string))
			desConfog["port"] = port
		} else {
			desConfog[key] = value

		}
	}
	return desConfog
}

// getConfig returns the configuration of the pipeline
func (br *SourcesSync) getConfig(source backendconfig.SourceT) SourceConfigT {
	sourceResourcesList := source.Config["resources"]
	sourceSchedule := source.Config["schedule"]

	schedule := &SourcesScheduleT{}
	if sourceSchedule != nil {
		every := sourceSchedule.(map[string]interface{})["every"].(float64)
		unit := sourceSchedule.(map[string]interface{})["unit"].(string)

		schedule.Every = int(every)
		schedule.Unit = unit
		if sourceSchedule.(map[string]interface{})["start_at"] != nil {
			startAt := sourceSchedule.(map[string]interface{})["start_at"].(string)
			schedule.StartAt = startAt
		}
	}
	var resources map[string]ResourceT
	if sourceResourcesList != nil {
		resources = br.getResources(sourceResourcesList.([]interface{}))
	}
	return SourceConfigT{
		Source: SourceT{
			Role:      source.SourceDefinition.Name,
			Options:   source.Config,
			WriteKey:  source.WriteKey,
			Resources: resources,
		},
		Schedule: *schedule,
	}
}

// handleSources updates the registry accordingly
func (br *SourcesSync) handleSources(sources []backendconfig.SourceT) {
	br.calculateDifferencesAndUpdateRegistry(sources)
}

// backendConfigSubscriber subscribes to backend-config change and updates the registry accordingly
func (br *SourcesSync) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		sources := config.Data.(backendconfig.SourcesT)
		fmt.Println("Received Blendo Config")
		br.handleSources(sources.Sources)
		br.currentSourceJSON = config.Data.(backendconfig.SourcesT)
		br.isConfigSet = true
	}
}

//Setup make necessary actions before sourcesSync start
func (br *SourcesSync) Setup() {
	rruntime.Go(br.backendConfigSubscriber)
}
