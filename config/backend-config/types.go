package backendconfig

import (
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Topic refers to a subset of backend config's updates, received after subscribing using the backend config's Subscribe function.
type Topic string

type Regulation string

const (
	/*TopicBackendConfig topic provides updates on full backend config, via Subscribe function */
	TopicBackendConfig Topic = "backendConfig"

	/*TopicProcessConfig topic provides updates on backend config of processor enabled destinations, via Subscribe function */
	TopicProcessConfig Topic = "processConfig"

	/*RegulationSuppress refers to Suppress Regulation */
	RegulationSuppress Regulation = "Suppress"

	/*RegulationDelete refers to Suppress and Delete Regulation */
	RegulationDelete Regulation = "Delete" // TODO Will add support soon.

	/*RegulationSuppressAndDelete refers to Suppress and Delete Regulation */
	RegulationSuppressAndDelete Regulation = "Suppress_With_Delete"

	GlobalEventType = "global"
)

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
	Transformations       []TransformationT
	IsProcessorEnabled    bool
	RevisionID            string
}

type SourceT struct {
	ID                         string
	Name                       string
	SourceDefinition           SourceDefinitionT
	Config                     map[string]interface{}
	Enabled                    bool
	WorkspaceID                string
	Destinations               []DestinationT
	WriteKey                   string
	DgSourceTrackingPlanConfig DgSourceTrackingPlanConfigT
	Transient                  bool
}

type WorkspaceRegulationT struct {
	ID             string
	RegulationType string
	WorkspaceID    string
	UserID         string
}

type SourceRegulationT struct {
	ID             string
	RegulationType string
	WorkspaceID    string
	SourceID       string
	UserID         string
}

type ConfigT struct {
	EnableMetrics   bool            `json:"enableMetrics"`
	WorkspaceID     string          `json:"workspaceId"`
	Sources         []SourceT       `json:"sources"`
	Libraries       LibrariesT      `json:"libraries"`
	ConnectionFlags ConnectionFlags `json:"flags"`
}

type ConnectionFlags struct {
	URL      string          `json:"url"`
	Services map[string]bool `json:"services"`
}

type WRegulationsT struct {
	WorkspaceRegulations []WorkspaceRegulationT `json:"workspaceRegulations"`
	Start                int                    `json:"start"`
	Limit                int                    `json:"limit"`
	Size                 int                    `json:"size"`
	End                  bool                   `json:"end"`
	Next                 int                    `json:"next"`
}

type SRegulationsT struct {
	SourceRegulations []SourceRegulationT `json:"sourceRegulations"`
	Start             int                 `json:"start"`
	Limit             int                 `json:"limit"`
	Size              int                 `json:"size"`
	End               bool                `json:"end"`
	Next              int                 `json:"next"`
}

type TransformationT struct {
	VersionID string
	ID        string
	Config    map[string]interface{}
}

type LibraryT struct {
	VersionID string
}

type LibrariesT []LibraryT

type DgSourceTrackingPlanConfigT struct {
	SourceId            string                            `json:"sourceId"`
	SourceConfigVersion int                               `json:"version"`
	Config              map[string]map[string]interface{} `json:"config"`
	MergedConfig        map[string]interface{}            `json:"mergedConfig"`
	Deleted             bool                              `json:"deleted"`
	TrackingPlan        TrackingPlanT                     `json:"trackingPlan"`
}

func (dgSourceTPConfigT *DgSourceTrackingPlanConfigT) GetMergedConfig(eventType string) map[string]interface{} {
	if dgSourceTPConfigT.MergedConfig == nil {
		globalConfig := dgSourceTPConfigT.fetchEventConfig(GlobalEventType)
		eventSpecificConfig := dgSourceTPConfigT.fetchEventConfig(eventType)
		outputConfig := misc.MergeMaps(globalConfig, eventSpecificConfig)
		dgSourceTPConfigT.MergedConfig = outputConfig
	}
	return dgSourceTPConfigT.MergedConfig
}

func (dgSourceTPConfigT *DgSourceTrackingPlanConfigT) fetchEventConfig(eventType string) map[string]interface{} {
	emptyMap := map[string]interface{}{}
	_, eventSpecificConfigPresent := dgSourceTPConfigT.Config[eventType]
	if !eventSpecificConfigPresent {
		return emptyMap
	}
	return dgSourceTPConfigT.Config[eventType]
}

type TrackingPlanT struct {
	Id      string `json:"id"`
	Version int    `json:"version"`
}
