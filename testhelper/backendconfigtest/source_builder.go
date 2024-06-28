package backendconfigtest

import (
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// NewSourceBuilder returns a new SourceBuilder
func NewSourceBuilder() *SourceBuilder {
	var b SourceBuilder

	b.v = &backendconfig.SourceT{
		ID:       rand.UniqueString(10),
		Name:     rand.String(5),
		Enabled:  true,
		WriteKey: rand.UniqueString(10),
		Config:   map[string]any{},
		SourceDefinition: backendconfig.SourceDefinitionT{
			ID:       rand.UniqueString(10),
			Name:     rand.String(5),
			Category: "eventStream",
			Type:     "type",
		},
	}
	return &b
}

// SourceBuilder is a builder for a source
type SourceBuilder struct {
	valueBuilder[backendconfig.SourceT]
}

// WithID sets the ID of the source
func (b *SourceBuilder) WithID(id string) *SourceBuilder {
	b.v.ID = id
	return b
}

// WithWriteKey sets the write key of the source
func (b *SourceBuilder) WithWriteKey(writeKey string) *SourceBuilder {
	b.v.WriteKey = writeKey
	return b
}

// WithConfigOption sets a config option for the source
func (b *SourceBuilder) WithConfigOption(key string, value any) *SourceBuilder {
	b.v.Config[key] = value
	return b
}

// WithConnection adds a destination to the source
func (b *SourceBuilder) WithConnection(destination backendconfig.DestinationT) *SourceBuilder {
	b.v.Destinations = append(b.v.Destinations, destination)
	return b
}

// Disabled disables the source
func (b *SourceBuilder) Disabled() *SourceBuilder {
	b.v.Enabled = false
	return b
}

// WithTrackingPlan adds a tracking plan to the source
func (b *SourceBuilder) WithTrackingPlan(id string, version int) *SourceBuilder {
	b.v.DgSourceTrackingPlanConfig.TrackingPlan.Id = id
	b.v.DgSourceTrackingPlanConfig.TrackingPlan.Version = version
	return b
}

// WithGeoenrichmentEnabled enables geoenrichment for the source
func (b *SourceBuilder) WithGeoenrichmentEnabled(enabled bool) *SourceBuilder {
	b.v.GeoEnrichment.Enabled = enabled
	return b
}

// WithSourceCategory sets the source definition category
func (b *SourceBuilder) WithSourceCategory(category string) *SourceBuilder {
	b.v.SourceDefinition.Category = category
	return b
}

// WithSourceType sets the source type
func (b *SourceBuilder) WithSourceType(sourceType string) *SourceBuilder {
	b.v.SourceDefinition.Name = sourceType
	return b
}

// WithSourceType sets the source type
func (b *SourceBuilder) WithWorkspaceID(workspaceID string) *SourceBuilder {
	b.v.WorkspaceID = workspaceID
	return b
}
