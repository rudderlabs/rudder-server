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

// WithConnection adds a source-to-destination connection (keyed by connection id)
// to the workspace config. Some pipelines (e.g. the router's reverse-ETL/warehouse
// path) require a top-level connection entry, not just a destination nested under
// the source via SourceBuilder.WithConnection.
func (b *ConfigBuilder) WithConnection(connectionID string, connection backendconfig.Connection) *ConfigBuilder {
	if b.v.Connections == nil {
		b.v.Connections = map[string]backendconfig.Connection{}
	}
	b.v.Connections[connectionID] = connection
	return b
}
