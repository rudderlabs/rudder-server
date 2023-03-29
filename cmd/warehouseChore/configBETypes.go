package warehouseChore

type DestinationDefinitionT struct {
	ID            string
	Name          string
	DisplayName   string
	Config        map[string]interface{}
	ResponseRules map[string]interface{}
}

type SourceDefinitionT struct {
	ID       string
	Name     string
	Category string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                map[string]interface{}
	Enabled               bool
	WorkspaceID           string
	IsProcessorEnabled    bool
	RevisionID            string
}

type SourceT struct {
	ID               string
	Name             string
	SourceDefinition SourceDefinitionT
	Config           map[string]interface{}
	Enabled          bool
	WorkspaceID      string
	Destinations     []DestinationT
	WriteKey         string
	Transient        bool
}

type ConfigT struct {
	EnableMetrics bool      `json:"enableMetrics"`
	WorkspaceID   string    `json:"workspaceId"`
	Sources       []SourceT `json:"sources"`
}
