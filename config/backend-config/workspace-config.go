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

//Get returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) Get() (SourcesT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI()
	}
}

// getFromApi gets the workspace config from api
func (workspaceConfig *WorkspaceConfig) getFromAPI() (SourcesT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Error("Errored when sending request to the server", err)
		return SourcesT{}, false
	}

	req.SetBasicAuth(workspaceToken, "")
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

// getFromFile reads the workspace config from JSON file
func (workspaceConfig *WorkspaceConfig) getFromFile() (SourcesT, bool) {
	logger.Info("Reading workspace config from JSON file")
	data, err := ioutil.ReadFile(configJSONPath)
	if err != nil {
		logger.Errorf("Unable to read backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	var configJSON SourcesT
	error := json.Unmarshal(data, &configJSON)
	if error != nil {
		logger.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	return configJSON, true
}
