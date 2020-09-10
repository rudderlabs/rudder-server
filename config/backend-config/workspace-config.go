package backendconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
)

type WorkspaceConfig struct {
	CommonBackendConfig
	workspaceID     string
	workspaceIDLock sync.RWMutex
}

func (workspaceConfig *WorkspaceConfig) SetUp() {
}

func (workspaceConfig *WorkspaceConfig) GetWorkspaceIDForWriteKey(writeKey string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

//Get returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) Get() (SourcesT, bool) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI()
	}
}

//GetRegulations returns sources from the workspace
func (workspaceConfig *WorkspaceConfig) GetRegulations() (RegulationsT, bool) {
	if configFromFile {
		return workspaceConfig.getRegulationsFromFile()
	} else {
		return workspaceConfig.getRegulationsFromAPI()
	}
}

// getFromApi gets the workspace config from api
func (workspaceConfig *WorkspaceConfig) getFromAPI() (SourcesT, bool) {
	url := fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)

	var respBody []byte
	var statusCode int

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		log.Errorf("[[ Workspace-config ]] Failed to fetch config from API with error: %w, retrying after %v", err, t)
	})

	if err != nil {
		log.Error("Error sending request to the server", err)
		return SourcesT{}, false
	}

	configEnvHandler := workspaceConfig.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	fmt.Println("[CONFIG] ", string(respBody))

	var sourcesJSON SourcesT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		log.Error("Error while parsing request", err, string(respBody), statusCode)
		return SourcesT{}, false
	}

	workspaceConfig.workspaceIDLock.Lock()
	workspaceConfig.workspaceID = sourcesJSON.WorkspaceID
	workspaceConfig.workspaceIDLock.Unlock()

	return sourcesJSON, true
}

// getFromFile reads the workspace config from JSON file
func (workspaceConfig *WorkspaceConfig) getFromFile() (SourcesT, bool) {
	log.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(configJSONPath)
	if err != nil {
		log.Errorf("Unable to read backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	var configJSON SourcesT
	error := json.Unmarshal(data, &configJSON)
	if error != nil {
		log.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return SourcesT{}, false
	}
	return configJSON, true
}

func (workspaceConfig *WorkspaceConfig) getRegulationsFromAPI() (RegulationsT, bool) {
	regulationsJSON := RegulationsT{}
	wregulations, status := workspaceConfig.getWorkspaceRegulationsFromAPI()
	if !status {
		return RegulationsT{}, false
	}
	regulationsJSON.WorkspaceRegulations = wregulations

	var sregulations []SourceRegulationT
	sregulations, status = workspaceConfig.getSourceRegulationsFromAPI()
	if !status {
		return RegulationsT{}, false
	}
	regulationsJSON.SourceRegulations = sregulations

	return regulationsJSON, true
}

func (workspaceConfig *WorkspaceConfig) getWorkspaceRegulationsFromAPI() ([]WorkspaceRegulationT, bool) {
	start := 0

	totalWorkspaceRegulations := []WorkspaceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
			return fetchError
		}

		backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
		err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			log.Errorf("[[ Workspace-config ]] Failed to fetch workspace regulations from API with error: %w, retrying after %v", err, t)
		})

		if err != nil {
			log.Error("Error sending request to the server", err)
			return []WorkspaceRegulationT{}, false
		}

		var workspaceRegulationsJSON WRegulationsT
		err = json.Unmarshal(respBody, &workspaceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), statusCode)
			return []WorkspaceRegulationT{}, false
		}

		totalWorkspaceRegulations = append(totalWorkspaceRegulations, workspaceRegulationsJSON.WorkspaceRegulations...)

		if workspaceRegulationsJSON.End {
			break
		}

		start = workspaceRegulationsJSON.Next
	}

	return totalWorkspaceRegulations, true
}

func (workspaceConfig *WorkspaceConfig) getSourceRegulationsFromAPI() ([]SourceRegulationT, bool) {
	start := 0

	totalSourceRegulations := []SourceRegulationT{}
	for {
		url := fmt.Sprintf("%s/workspaces/sources/regulations?start=%d&limit=%d", configBackendURL, start, maxRegulationsPerRequest)

		var respBody []byte
		var statusCode int

		operation := func() error {
			var fetchError error
			respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(url)
			return fetchError
		}

		backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
		err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			log.Errorf("[[ Workspace-config ]] Failed to fetch source regulations from API with error: %w, retrying after %v", err, t)
		})
		if err != nil {
			log.Error("Error sending request to the server", err)
			return []SourceRegulationT{}, false
		}

		var sourceRegulationsJSON SRegulationsT
		err = json.Unmarshal(respBody, &sourceRegulationsJSON)
		if err != nil {
			log.Error("Error while parsing request", err, string(respBody), statusCode)
			return []SourceRegulationT{}, false
		}

		totalSourceRegulations = append(totalSourceRegulations, sourceRegulationsJSON.SourceRegulations...)

		if sourceRegulationsJSON.End {
			break
		}

		start = sourceRegulationsJSON.Next
	}

	return totalSourceRegulations, true
}

func (workspaceConfig *WorkspaceConfig) getRegulationsFromFile() (RegulationsT, bool) {
	regulations := RegulationsT{}
	regulations.WorkspaceRegulations = make([]WorkspaceRegulationT, 0)
	regulations.SourceRegulations = make([]SourceRegulationT, 0)
	return regulations, true
}

func (workspaceConfig *WorkspaceConfig) makeHTTPRequest(url string) ([]byte, int, error) {
	req, err := Http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = IoUtil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	return respBody, resp.StatusCode, nil
}
