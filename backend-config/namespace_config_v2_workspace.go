package backendconfig

import (
	"encoding/json"
	"time"
)

// The workspace body of a v2 response. Only what feeds a declared field of ConfigT is typed:
// Other fields like whtProjects, destinationTransformations, resources, sqlModelVersions,
// secret versions and most timestamps are dropped, as they are also ignored by v1's json.Unmarshal today.
//
// Sources, destinations, connections and transformations are keyed maps here where v1 nests
// destinations under sources; the key is the entity's id and is hoisted into the struct by the
// mapper. This is the body v2Workspace keeps the raw bytes of.
type v2WorkspaceBody struct {
	Sources         map[string]v2Source          `json:"sources"`
	Destinations    map[string]v2Destination     `json:"destinations"`
	Connections     map[string]Connection        `json:"connections"`
	Transformations map[string]v2Transformation  `json:"transformations"`
	TrackingPlans   map[string]v2TrackingPlan    `json:"trackingPlans"`
	Accounts        map[string]Account           `json:"accounts"`
	Credentials     map[string]Credential        `json:"credentials"`
	EventReplays    map[string]EventReplayConfig `json:"eventReplays"`
	Libraries       LibrariesT                   `json:"libraries"`
	Settings        *Settings                    `json:"settings"`
	UpdatedAt       time.Time                    `json:"updatedAt"`
}

type v2Source struct {
	Name                       string                        `json:"name"`
	WriteKey                   string                        `json:"writeKey"`
	Enabled                    bool                          `json:"enabled"`
	Transient                  bool                          `json:"transient"`
	SourceDefinitionName       string                        `json:"sourceDefinitionName"`
	Config                     map[string]any                `json:"config"`
	LiveEventsConfig           map[string]any                `json:"liveEventsConfig"`
	DgSourceTrackingPlanConfig *v2DgSourceTrackingPlanConfig `json:"dgSourceTrackingPlanConfig"`
	GeoEnrichment              *struct {
		Enabled bool `json:"enabled"`
	} `json:"geoEnrichment"`
	InternalSecret json.RawMessage `json:"internalSecret"`
}

type v2Destination struct {
	Name                      string         `json:"name"`
	Enabled                   bool           `json:"enabled"`
	Deleted                   bool           `json:"deleted"`
	DestinationDefinitionName string         `json:"destinationDefinitionName"`
	Config                    map[string]any `json:"config"`
	LiveEventsConfig          map[string]any `json:"liveEventsConfig"`
	TransformationIDs         []string       `json:"transformationIds"`
	RevisionID                string         `json:"revisionId"`
	// Version is the integration major this destination is pinned to. Absent and null both mean
	// the default major, so the pointer distinguishes them from an explicit 0, which resolves to
	// no config at all - as it does in the control plane.
	Version *int `json:"version"`
}

type v2Transformation struct {
	VersionID        string         `json:"versionId"`
	Language         string         `json:"language"`
	LiveEventsConfig map[string]any `json:"liveEventsConfig"`
}

type v2TrackingPlan struct {
	Version int `json:"version"`
}

type v2DgSourceTrackingPlanConfig struct {
	TrackingPlanID string                    `json:"trackingPlanId"`
	Version        int                       `json:"version"`
	Config         map[string]map[string]any `json:"config"`
	Enabled        bool                      `json:"enabled"`
	Deleted        bool                      `json:"deleted"`
}
