package backendconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//ManagedHosting is a struct to hold variables necessary for managed hosting.
type ManagedHosting struct {
	hostedWorkspaceIDToWriteKeysMap map[string][]string
	workspaceWriteKeysMapLock       sync.RWMutex
}

//HostedWorkspacesT holds hosted workspaces sources
type HostedWorkspacesT struct {
	HostedWorkspaces map[string][]SourceT `json:"-"`
}

//SetUp sets up the hosting config
func (hosting *ManagedHosting) SetUp() {
	hosting.hostedWorkspaceIDToWriteKeysMap = make(map[string][]string)
}

//GetWorkspaceIDForWriteKey return workspaceID for the given writeKey
func (hosting *ManagedHosting) GetWorkspaceIDForWriteKey(givenWriteKey string) string {
	hosting.workspaceWriteKeysMapLock.RLock()
	defer hosting.workspaceWriteKeysMapLock.RUnlock()
	for workspaceID, writeKeys := range hosting.hostedWorkspaceIDToWriteKeysMap {
		for _, writeKey := range writeKeys {
			if givenWriteKey == writeKey {
				return workspaceID
			}
		}
	}

	return ""
}

//GetBackendConfig returns sources from all hosted workspaces
func (hosting *ManagedHosting) GetBackendConfig() (SourcesT, bool) {
	url := fmt.Sprintf("%s/hostedWorkspaceConfig", configBackendURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Error("Errored when sending request to the server", err)
		return SourcesT{}, false
	}

	req.SetBasicAuth(hostedServiceSecret, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	var hostedWorspaces HostedWorkspacesT
	err = json.Unmarshal(respBody, &hostedWorspaces.HostedWorkspaces)
	if err != nil {
		logger.Error("Errored while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}

	hostedWorkspaceIDToWriteKeysMap := make(map[string][]string)
	var sourcesJSON SourcesT
	for workspaceID, sourceArr := range hostedWorspaces.HostedWorkspaces {
		for _, source := range sourceArr {
			hostedWorkspaceIDToWriteKeysMap[workspaceID] = append(hostedWorkspaceIDToWriteKeysMap[workspaceID], source.WriteKey)
		}
		sourcesJSON.Sources = append(sourcesJSON.Sources, sourceArr...)
	}

	hosting.workspaceWriteKeysMapLock.Lock()
	hosting.hostedWorkspaceIDToWriteKeysMap = hostedWorkspaceIDToWriteKeysMap
	hosting.workspaceWriteKeysMapLock.Unlock()

	return sourcesJSON, true
}
