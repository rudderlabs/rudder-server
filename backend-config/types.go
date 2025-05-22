package backendconfig

import (
	"encoding/json"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
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
	DeliveryAccount       *Account `json:"deliveryAccount,omitempty"`
	DeleteAccount         *Account `json:"deleteAccount,omitempty"`
	HasDynamicConfig      bool     `json:"hasDynamicConfig,omitempty"`
}

// UpdateHasDynamicConfig checks if the destination config contains dynamic config patterns
// and sets the HasDynamicConfig field accordingly.
// It uses a cache to avoid recomputing the flag for destinations that haven't changed.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
// When a destination's RevisionID changes, it indicates a config change, and we recompute the flag.
func (d *DestinationT) UpdateHasDynamicConfig(cache dynamicconfig.Cache) {
	// Check if we have a cached value for this destination
	cachedInfo, exists := cache.Get(d.ID)

	// If the destination's RevisionID matches the cached RevisionID,
	// use the cached HasDynamicConfig value to avoid recomputation
	if exists && d.RevisionID == cachedInfo.RevisionID {
		d.HasDynamicConfig = cachedInfo.HasDynamicConfig
		return
	}

	// RevisionID is not in cache or has changed, recompute the dynamic config flag
	d.HasDynamicConfig = dynamicconfig.ContainsPattern(d.Config)

	// Update the cache with the new value
	cache.Set(d.ID, &dynamicconfig.DestinationRevisionInfo{
		RevisionID:       d.RevisionID,
		HasDynamicConfig: d.HasDynamicConfig,
	})
}

type SourceT struct {
	ID                         string
	OriginalID                 string
	Name                       string
	SourceDefinition           SourceDefinitionT
	Config                     json.RawMessage
	Enabled                    bool
	WorkspaceID                string
	Destinations               []DestinationT
	WriteKey                   string
	DgSourceTrackingPlanConfig DgSourceTrackingPlanConfigT
	Transient                  bool
	GeoEnrichment              struct {
		Enabled bool
	}
}

type Credential struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	IsSecret bool   `json:"isSecret"`
}

func (s *SourceT) IsReplaySource() bool {
	return s.OriginalID != ""
}

type Account struct {
	ID                    string                 `json:"id"`
	AccountDefinitionName string                 `json:"accountDefinitionName"`
	Options               map[string]interface{} `json:"options"`
	Secret                map[string]interface{} `json:"secret"`
	AccountDefinition     *AccountDefinition     `json:"accountDefinition"`
}

type AccountDefinition struct {
	Name               string                 `json:"name"`
	Config             map[string]interface{} `json:"config"`
	AuthenticationType string                 `json:"authenticationType"`
}
type ConfigT struct {
	EnableMetrics      bool                         `json:"enableMetrics"`
	WorkspaceID        string                       `json:"workspaceId"`
	Sources            []SourceT                    `json:"sources"`
	EventReplays       map[string]EventReplayConfig `json:"eventReplays"`
	Libraries          LibrariesT                   `json:"libraries"`
	ConnectionFlags    ConnectionFlags              `json:"flags"`
	Settings           Settings                     `json:"settings"`
	UpdatedAt          time.Time                    `json:"updatedAt"`
	Credentials        map[string]Credential        `json:"credentials"`
	Connections        map[string]Connection        `json:"connections"`
	Accounts           map[string]Account           `json:"accounts"`
	AccountDefinitions map[string]AccountDefinition `json:"accountDefinitions"`
}

type Connection struct {
	SourceID         string                 `json:"sourceId"`
	DestinationID    string                 `json:"destinationId"`
	Enabled          bool                   `json:"enabled"`
	Config           map[string]interface{} `json:"config"`
	ProcessorEnabled bool                   `json:"processorEnabled"`
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
		outputConfig := lo.Assign(globalConfig, eventSpecificConfig)
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
