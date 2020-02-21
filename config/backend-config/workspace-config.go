package backendconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

type WorkspaceConfig struct {
}

func (workspaceConfig *WorkspaceConfig) SetUp() {
}

func (workspaceConfig *WorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	return ""
}

func (WorkspaceConfig *WorkspaceConfig) filterOutNativeSDKDestinations(config SourcesT) SourcesT {
	var modifiedSources SourcesT
	modifiedSources.Sources = make([]SourceT, 0)
	for _, source := range config.Sources {
		destinations := make([]DestinationT, 0)
		for _, destination := range source.Destinations {
			isDeviceModeOnly := destination.DestinationDefinition.Config.(map[string]interface{})["deviceModeOnly"]
			isUsingNativeSDK := destination.Config.(map[string]interface{})["useNativeSDK"]
			isServerModeDestination := !(isDeviceModeOnly == true || isUsingNativeSDK == true)
			logger.Debug("Destination Name :", destination.Name, " deviceModeOnly: ", isDeviceModeOnly, " useNativeSDK: ", isUsingNativeSDK, " serverMode: ", isServerModeDestination)

			if isServerModeDestination {
				destinations = append(destinations, destination)
			}
		}
		source.Destinations = destinations
		modifiedSources.Sources = append(modifiedSources.Sources, source)
	}
	return modifiedSources
}

//Get returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) Get() (SourcesT, bool) {
	var config SourcesT
	var ok bool

	if configFromFile {
		config, ok = workspaceConfig.getFromFile()
	} else {
		config, ok = workspaceConfig.getFromAPI()
	}
	logger.Debug("Unfiltered complete config ", config)
	if !ok {
		return config, false
	}
	return workspaceConfig.filterOutNativeSDKDestinations(config), true
}

// getFromApi gets the workspace config from api
func (workspaceConfig *WorkspaceConfig) getFromAPI() (SourcesT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig", configBackendURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Error("Errored when sending request to the server", err)
		return SourcesT{}, false
	}

	req.SetBasicAuth(configBackendToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Errored when sending request to the server", err)
		return SourcesT{}, false
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	var sourcesJSON SourcesT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		logger.Error("Errored while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}
	return sourcesJSON, true
}

// getFromApi reads the workspace config from JSON file
func (workspaceConfig *WorkspaceConfig) getFromFile() (SourcesT, bool) {
	logger.Info("Reading workspace config from JSON file")
	data, err := ioutil.ReadFile(configJSONPath)
	if err != nil {
		logger.Error("Unable to read backend config from file.")
		return SourcesT{}, false
	}
	var configJSON SourcesT
	error := json.Unmarshal(data, &configJSON)
	if error != nil {
		logger.Error("Unable to parse backend config from file.")
		return SourcesT{}, false
	}
	return configJSON, true
}
