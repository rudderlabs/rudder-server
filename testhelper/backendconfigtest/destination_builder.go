package backendconfigtest

import (
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// NewDestinationBuilder returns a new DestinationBuilder
func NewDestinationBuilder(destType string) *DestinationBuilder {
	var b DestinationBuilder
	b.v = &backendconfig.DestinationT{
		ID:                 rand.UniqueString(10),
		Name:               rand.String(5),
		Enabled:            true,
		IsProcessorEnabled: true,
		Config:             map[string]any{},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:     rand.UniqueString(10),
			Name:   destType,
			Config: map[string]any{},
		},
	}
	return &b
}

// DestinationBuilder is a builder for a destination
type DestinationBuilder struct {
	valueBuilder[backendconfig.DestinationT]
}

// WithID sets the ID of the destination
func (b *DestinationBuilder) WithID(id string) *DestinationBuilder {
	b.v.ID = id
	return b
}

// WithRevisionID sets the revision ID of the destination
func (b *DestinationBuilder) WithRevisionID(revisionID string) *DestinationBuilder {
	b.v.RevisionID = revisionID
	return b
}

// WithConfigOption sets a config option for the destination
func (b *DestinationBuilder) WithConfigOption(key string, value any) *DestinationBuilder {
	b.v.Config[key] = value
	return b
}

// WithUserTransformation adds a user transformation to the destination
func (b *DestinationBuilder) WithUserTransformation(id, version string) *DestinationBuilder {
	b.v.Transformations = append(b.v.Transformations, backendconfig.TransformationT{
		ID:        id,
		VersionID: version,
	})
	return b
}

// WithDefinitionConfigOption adds a config option to the destination definition
func (b *DestinationBuilder) WithDefinitionConfigOption(key string, value any) *DestinationBuilder {
	b.v.DestinationDefinition.Config[key] = value
	return b
}
