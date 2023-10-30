package backendconfig

import (
	"time"

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
	Type     string // // Indicates whether source is one of {cloud, web, flutter, android, ios, warehouse, cordova, amp, reactnative, unity}. This field is not present in sources table
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                map[string]interface{}
	Enabled               bool
	WorkspaceID           string
	Transformations       []TransformationT
	IsProcessorEnabled    bool
	RevisionID            string
}

type SourceT struct {
	ID                         string
	OriginalID                 string
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

func (s *SourceT) IsReplaySource() bool {
	return s.OriginalID != ""
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
	EnableMetrics   bool                         `json:"enableMetrics"`
	WorkspaceID     string                       `json:"workspaceId"`
	Sources         []SourceT                    `json:"sources"`
	EventReplays    map[string]EventReplayConfig `json:"eventReplays"`
	Libraries       LibrariesT                   `json:"libraries"`
	ConnectionFlags ConnectionFlags              `json:"flags"`
	Settings        Settings                     `json:"settings"`
	UpdatedAt       time.Time                    `json:"updatedAt"`
}

func (c *ConfigT) SourcesMap() map[string]*SourceT {
	sourcesMap := make(map[string]*SourceT)
	for i := range c.Sources {
		source := c.Sources[i]
		sourcesMap[source.ID] = &source
	}
	return sourcesMap
}

func (c *ConfigT) DestinationsMap() map[string]*DestinationT {
	destinationsMap := make(map[string]*DestinationT)
	for i := range c.Sources {
		source := c.Sources[i]
		for j := range source.Destinations {
			destination := source.Destinations[j]
			destinationsMap[destination.ID] = &destination
		}
	}
	return destinationsMap
}

type Settings struct {
	DataRetention     DataRetention `json:"dataRetention"`
	EventAuditEnabled bool          `json:"eventAuditEnabled"`
}

type DataRetention struct {
	DisableReportingPII bool               `json:"disableReportingPii"`
	UseSelfStorage      bool               `json:"useSelfStorage"`
	StorageBucket       StorageBucket      `json:"storageBucket"`
	StoragePreferences  StoragePreferences `json:"storagePreferences"`
	RetentionPeriod     string             `json:"retentionPeriod"`
}

type StorageBucket struct {
	Type   string `json:"type"`
	Config map[string]interface{}
}

type StoragePreferences struct {
	ProcErrors       bool `json:"procErrors"`
	GatewayDumps     bool `json:"gatewayDumps"`
	ProcErrorDumps   bool `json:"procErrorDumps"`
	RouterDumps      bool `json:"routerDumps"`
	BatchRouterDumps bool `json:"batchRouterDumps"`
}

func (sp StoragePreferences) Backup(tableprefix string) bool {
	switch tableprefix {
	case "gw":
		return sp.GatewayDumps
	case "rt":
		return sp.RouterDumps
	case "batch_rt":
		return sp.BatchRouterDumps
	case "proc_error":
		return sp.ProcErrorDumps
	default:
		return false
	}
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

func (d *DestinationT) IsOAuthDestination() bool {
	if authValue, err := misc.NestedMapLookup(d.Config, "auth", "type"); err == nil {
		if authType, ok := authValue.(string); ok {
			return authType == "OAuth"
		}
	}
	return false
}

/*
Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`GetAccountId(destDetail.Config, "rudderDeleteAccountId")` --> To be used when we make use of OAuth during regulation flow
`GetAccountId(destDetail.Config, "rudderAccountId")` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationT) GetAccountID(idKey string) string {
	rudderAccountIdInterface, found := d.Config[idKey]
	if !d.IsOAuthDestination() || !found || idKey == "" {
		return ""
	}
	rudderAccountId, ok := rudderAccountIdInterface.(string)
	if ok {
		return rudderAccountId
	}
	return ""
}
