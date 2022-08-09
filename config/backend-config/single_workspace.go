package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type singleWorkspaceConfig struct {
	Token            string
	workspaceID      string
	configBackendURL *url.URL
	configJSONPath   string
	configEnvHandler types.ConfigEnvI

	workspaceIDToLibrariesMap map[string]LibrariesT
	workspaceIDLock           sync.RWMutex
}

func (wc *singleWorkspaceConfig) SetUp() error {
	if configFromFile {
		if wc.configJSONPath == "" {
			return fmt.Errorf("valid configJSONPath is required when configFromFile is set to true")
		}
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

func (wc *singleWorkspaceConfig) AccessToken() string {
	return wc.Token
}

func (wc *singleWorkspaceConfig) GetWorkspaceIDForWriteKey(_ string) string {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()

	return wc.workspaceID
}

func (wc *singleWorkspaceConfig) GetWorkspaceIDForSourceID(_ string) string {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()

	return wc.workspaceID
}

// GetWorkspaceLibrariesForWorkspaceID returns workspaceLibraries for workspaceID
func (wc *singleWorkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(workspaceID string) LibrariesT {
	wc.workspaceIDLock.RLock()
	defer wc.workspaceIDLock.RUnlock()
	if wc.workspaceIDToLibrariesMap[workspaceID] == nil {
		return LibrariesT{}
	}
	return wc.workspaceIDToLibrariesMap[workspaceID]
}

// Get returns sources from the workspace
func (wc *singleWorkspaceConfig) Get(ctx context.Context, workspace string) (ConfigT, error) {
	if configFromFile {
		return wc.getFromFile()
	} else {
		return wc.getFromAPI(ctx, workspace)
	}
}

// getFromApi gets the workspace config from api
func (wc *singleWorkspaceConfig) getFromAPI(ctx context.Context, _ string) (ConfigT, error) {
	if wc.configBackendURL == nil {
		return ConfigT{}, fmt.Errorf("single workspace: config backend url is nil")
	}

	var (
		respBody []byte
		u        = fmt.Sprintf("%s/workspaceConfig?fetchAll=true", wc.configBackendURL)
	)

	operation := func() error {
		var fetchError error
		respBody, fetchError = wc.makeHTTPRequest(ctx, u)
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
		pkgLogger.Errorf("Error while parsing request: %v", err)
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
func (wc *singleWorkspaceConfig) getFromFile() (ConfigT, error) {
	pkgLogger.Info("Reading workspace config from JSON file")
	data, err := IoUtil.ReadFile(wc.configJSONPath)
	if err != nil {
		pkgLogger.Errorf("Unable to read backend config from file: %s with error : %s", wc.configJSONPath, err.Error())
		return ConfigT{}, err
	}
	var configJSON ConfigT
	if err = json.Unmarshal(data, &configJSON); err != nil {
		pkgLogger.Errorf("Unable to parse backend config from file: %s", wc.configJSONPath)
		return ConfigT{}, err
	}
	return configJSON, nil
}

func (wc *singleWorkspaceConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(wc.Token, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, nil
}
