package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/config"
)

type SingleWorkspaceConfig struct {
	CommonBackendConfig
	workspaceID string
	Token       string

	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (workspaceConfig *SingleWorkspaceConfig) SetUp() {
	if workspaceConfig.Token == "" {
		workspaceConfig.Token = config.GetWorkspaceToken()
	}
}

func (workspaceConfig *SingleWorkspaceConfig) AccessToken() string {
	return workspaceConfig.Token
}

func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceIDForWriteKey(_ string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceIDForSourceID(_ string) string {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()

	return workspaceConfig.workspaceID
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (workspaceConfig *SingleWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	workspaceConfig.workspaceIDLock.RLock()
	defer workspaceConfig.workspaceIDLock.RUnlock()
	if workspaceConfig.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return workspaceConfig.workspaceIDToLibrariesMap[workspaceID]
}

// Get returns sources from the workspace
func (workspaceConfig *SingleWorkspaceConfig) Get(ctx context.Context, workspace string) (ConfigT, error) {
	if configFromFile {
		return workspaceConfig.getFromFile()
	} else {
		return workspaceConfig.getFromAPI(ctx, workspace)
	}
}

// getFromApi gets the workspace config from api
func (workspaceConfig *SingleWorkspaceConfig) getFromAPI(ctx context.Context, workspace string) (ConfigT, error) {
	if workspace == "" {
		return ConfigT{}, newError(false, fmt.Errorf("no workspace token provided, skipping backend config fetch"))
	}

	var (
		respBody   []byte
		statusCode int
		url        = fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL)
	)

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = workspaceConfig.makeHTTPRequest(ctx, url, workspace)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[[ Workspace-config ]] Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server", err)
		return ConfigT{}, newError(true, err)
	}

	configEnvHandler := workspaceConfig.CommonBackendConfig.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var sourcesJSON ConfigT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, newError(true, err)
	}

	workspaceConfig.workspaceIDLock.Lock()
	workspaceConfig.workspaceID = sourcesJSON.WorkspaceID
	workspaceConfig.workspaceIDToLibrariesMap = make(map[string]LibrariesT)
	workspaceConfig.workspaceIDToLibrariesMap[sourcesJSON.WorkspaceID] = sourcesJSON.Libraries
	workspaceConfig.workspaceIDLock.Unlock()

	return sourcesJSON, nil
}

// getFromFile reads the workspace config from JSON file
func (*SingleWorkspaceConfig) getFromFile() (ConfigT, error) {
	pkgLogger.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(configJSONPath)
	if err != nil {
		pkgLogger.Errorf("Unable to read backend config from file: %s with error : %s", configJSONPath, err.Error())
		return ConfigT{}, newError(false, err)
	}
	var configJSON ConfigT
	if err = json.Unmarshal(data, &configJSON); err != nil {
		pkgLogger.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return ConfigT{}, newError(false, err)
	}
	return configJSON, nil
}

func (*SingleWorkspaceConfig) makeHTTPRequest(ctx context.Context, url, workspaceToken string) ([]byte, int, error) {
	req, err := Http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return []byte{}, 400, err
	}

	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	defer func() { _ = resp.Body.Close() }()

	respBody, err := IoUtil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, 400, err
	}

	return respBody, resp.StatusCode, nil
}

func (workspaceConfig *SingleWorkspaceConfig) IsConfigured() bool {
	if configFromFile {
		return true
	}
	return workspaceConfig.Token != ""
}
