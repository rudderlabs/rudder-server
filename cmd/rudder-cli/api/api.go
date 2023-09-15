package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/olekukonko/tablewriter"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
)

type SourceT struct {
	ID               string
	Name             string
	SourceDefinition SourceDefinitionT
	Config           interface{}
	Enabled          bool
	Destinations     []DestinationT
	WriteKey         string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                interface{}
	Enabled               bool
	Transformations       []TransformationT
}
type TransformationT struct {
	ID          string
	Name        string
	Description string
	VersionID   string
}
type SourcesT struct {
	Sources []SourceT `json:"sources"`
}

type DestinationDefinitionT struct {
	ID          string
	Name        string
	DisplayName string
}

type SourceDefinitionT struct {
	ID   string
	Name string
}

func getWorkspaceConfig() (SourcesT, bool) {
	baseUrl := config.GetEnv(config.ConfigBackendURLKey)
	configBackendToken := config.GetEnv(config.ConfigBackendWorkSpaceToken)

	client := &http.Client{}
	url := fmt.Sprintf("%s/workspace-config?workspaceToken=%s", baseUrl, configBackendToken)
	resp, err := client.Get(url)

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = io.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	if err != nil {
		fmt.Println("Errored when sending request to the server", err)
		return SourcesT{}, false
	}
	var sources SourcesT
	err = json.Unmarshal(respBody, &sources)
	if err != nil {
		fmt.Println("Errored while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}

	return sources, true
}

func DisplayConfig() {
	sourceList, ok := getWorkspaceConfig()
	if !ok {
		return
	}
	sources := sourceList.Sources

	table := tablewriter.NewWriter(os.Stdout)

	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Source", "Destination"})

	for i := 0; i < len(sources); i++ {
		for j := 0; j < len(sources[i].Destinations); j++ {
			sourceName := sources[i].Name
			if j > 0 {
				sourceName = ""
			}
			table.Append([]string{
				sourceName,
				sources[i].Destinations[j].Name,
			})
		}
	}

	table.Render()
}
