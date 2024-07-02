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
	b.v.Sources = append(b.v.Sources, source)
	return b
}

// WithWorkspaceID adds a workspaceID to the config
func (b *ConfigBuilder) WithWorkspaceID(workspaceID string) *ConfigBuilder {
	b.v.WorkspaceID = workspaceID
	return b
}
