package backendconfigtest

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// NewConfigBuilder returns a new ConfigBuilder
func NewConfigBuilder() *ConfigBuilder {
	var b ConfigBuilder
	b.v = &backendconfig.ConfigT{
		EnableMetrics: false,
		WorkspaceID:   rand.UniqueString(10),
		UpdatedAt:     time.Now(),
	}
	return &b
}

// ConfigBuilder is a builder for a backend config
type ConfigBuilder struct {
	valueBuilder[backendconfig.ConfigT]
}

// WithSource adds a source to the config
func (b *ConfigBuilder) WithSource(source backendconfig.SourceT) *ConfigBuilder {
	source.WorkspaceID = b.v.WorkspaceID
	for i := range source.Destinations {
		source.Destinations[i].WorkspaceID = b.v.WorkspaceID
	}
	b.v.Sources = append(b.v.Sources, source)
	return b
}

func (b *ConfigBuilder) WithCredentials(credentials map[string]backendconfig.Credential) *ConfigBuilder {
	b.v.Credentials = credentials
	return b
}
