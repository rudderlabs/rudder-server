package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type SingleWorkspaceConfig struct {
	Token            string
	workspaceID      string
	configBackendURL string
	configEnvHandler types.ConfigEnvI

	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (wc *SingleWorkspaceConfig) SetUp() error {
	if configFromFile {
		return nil
	}
	if wc.Token == "" {
		wc.Token = config.GetWorkspaceToken()
	}
	if wc.Token == "" {
		return fmt.Errorf("single workspace: empty workspace config token")
	}
	return nil
}

func (wc *SingleWorkspaceConfig) AccessToken() string {
	return wc.Token
}

func (wc *SingleWorkspaceConfig) GetWorkspaceIDForWriteKey(_ string) string {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()

	return wc.workspaceID
}

func (wc *SingleWorkspaceConfig) GetWorkspaceIDForSourceID(_ string) string {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()

	return wc.workspaceID
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (wc *SingleWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()
	if wc.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return wc.workspaceIDToLibrariesMap[workspaceID]
}

// Get returns sources from the workspace
func (wc *SingleWorkspaceConfig) Get(ctx context.Context, workspace string) (ConfigT, error) {
	if configFromFile {
		return wc.getFromFile()
	} else {
		return wc.getFromAPI(ctx, workspace)
	}
}

// getFromApi gets the workspace config from api
func (wc *SingleWorkspaceConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	var (
		respBody   []byte
		statusCode int
		url        = fmt.Sprintf("%s/workspaceConfig?fetchAll=true", wc.configBackendURL)
	)

	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = wc.makeHTTPRequest(ctx, url)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Warnf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Errorf("Error sending request to the server: %v", err)
		return ConfigT{}, err
	}

	configEnvHandler := wc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var sourcesJSON ConfigT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request [%d]: %v", statusCode, err)
		return ConfigT{}, err
	}

	wc.workspaceIDLock.Lock()
	wc.workspaceID = sourcesJSON.WorkspaceID
	wc.workspaceIDToLibrariesMap = make(map[string]LibrariesT)
	wc.workspaceIDToLibrariesMap[sourcesJSON.WorkspaceID] = sourcesJSON.Libraries
	wc.workspaceIDLock.Unlock()

	return sourcesJSON, nil
}

// getFromFile reads the workspace config from JSON file
func (*SingleWorkspaceConfig) getFromFile() (ConfigT, error) {
	pkgLogger.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(configJSONPath)
	if err != nil {
		pkgLogger.Errorf("Unable to read backend config from file: %s with error : %s", configJSONPath, err.Error())
		return ConfigT{}, err
	}
	var configJSON ConfigT
	if err = json.Unmarshal(data, &configJSON); err != nil {
		pkgLogger.Errorf("Unable to parse backend config from file: %s", configJSONPath)
		return ConfigT{}, err
	}
	return configJSON, nil
}

func (wc *SingleWorkspaceConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	req.SetBasicAuth(wc.Token, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, resp.StatusCode, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, resp.StatusCode, nil
}
