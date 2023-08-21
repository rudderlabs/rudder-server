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

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type singleWorkspaceConfig struct {
	token            string
	configBackendURL *url.URL
	configJSONPath   string
	configEnvHandler types.ConfigEnvI
	region           string

	workspaceIDOnce sync.Once
	workspaceID     string
}

func (wc *singleWorkspaceConfig) SetUp() error {
	if configFromFile {
		if wc.configJSONPath == "" {
			return fmt.Errorf("valid configJSONPath is required when configFromFile is set to true")
		}
		return nil
	}
	if wc.token == "" {
		wc.token = config.GetWorkspaceToken()
	}
	if wc.token == "" {
		return fmt.Errorf("single workspace: empty workspace config token")
	}
	return nil
}

func (wc *singleWorkspaceConfig) AccessToken() string {
	return wc.token
}

// Get returns sources from the workspace
func (wc *singleWorkspaceConfig) Get(ctx context.Context) (map[string]ConfigT, error) {
	if configFromFile {
		return wc.getFromFile()
	} else {
		return wc.getFromAPI(ctx)
	}
}

// getFromApi gets the workspace config from api
func (wc *singleWorkspaceConfig) getFromAPI(ctx context.Context) (map[string]ConfigT, error) {
	config := make(map[string]ConfigT)
	if wc.configBackendURL == nil {
		return config, fmt.Errorf("single workspace: config backend url is nil")
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

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Warnf("Failed to fetch config from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		if ctx.Err() == nil {
			pkgLogger.Errorf("Error sending request to the server: %v", err)
		}
		return config, err
	}

	configEnvHandler := wc.configEnvHandler
	if configEnvReplacementEnabled && configEnvHandler != nil {
		respBody = configEnvHandler.ReplaceConfigWithEnvVariables(respBody)
	}

	var sourcesJSON ConfigT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		pkgLogger.Errorf("Error while parsing request: %v", err)
		return config, err
	}
	sourcesJSON.ApplyReplaySources()
	workspaceID := sourcesJSON.WorkspaceID

	wc.workspaceIDOnce.Do(func() {
		wc.workspaceID = workspaceID
	})
	config[workspaceID] = sourcesJSON

	return config, nil
}

// getFromFile reads the workspace config from JSON file
func (wc *singleWorkspaceConfig) getFromFile() (map[string]ConfigT, error) {
	pkgLogger.Debug("Reading workspace config from JSON file")
	config := make(map[string]ConfigT)
	data, err := IoUtil.ReadFile(wc.configJSONPath)
	if err != nil {
		pkgLogger.Errorf("Unable to read backend config from file: %s with error : %s", wc.configJSONPath, err.Error())
		return config, err
	}
	var configJSON ConfigT
	if err = json.Unmarshal(data, &configJSON); err != nil {
		pkgLogger.Errorf("Unable to parse backend config from file: %s", wc.configJSONPath)
		return config, err
	}
	workspaceID := configJSON.WorkspaceID
	wc.workspaceIDOnce.Do(func() {
		pkgLogger.Info("Read workspace config from JSON file")
		wc.workspaceID = workspaceID
	})
	config[workspaceID] = configJSON
	return config, nil
}

func (wc *singleWorkspaceConfig) makeHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(wc.token, "")
	req.Header.Set("Content-Type", "application/json")
	if wc.region != "" {
		q := req.URL.Query()
		q.Add("region", wc.region)
		req.URL.RawQuery = q.Encode()
	}

	client := &http.Client{Timeout: config.GetDuration("HttpClient.backendConfig.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { kithttputil.CloseResponse(resp) }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 300 {
		return nil, getNotOKError(respBody, resp.StatusCode)
	}

	return respBody, nil
}

func (wc *singleWorkspaceConfig) Identity() identity.Identifier {
	return &identity.Workspace{
		WorkspaceID:    wc.workspaceID,
		WorkspaceToken: wc.token,
	}
}
