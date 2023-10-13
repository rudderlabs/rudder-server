package backendconfigtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// NewBuilder returns a new ServerBuilder
func NewBuilder() *ServerBuilder {
	return &ServerBuilder{
		settingsHandler: func(_ http.ResponseWriter, _ *http.Request) {},
	}
}

// ServerBuilder is a builder for a test server that returns backend configs
type ServerBuilder struct {
	namespace       string
	configs         map[string]backendconfig.ConfigT
	settingsHandler http.HandlerFunc
}

// WithNamespace sets the namespace for the server along with the configs for that namespace
func (b *ServerBuilder) WithNamespace(namespace string, configs ...backendconfig.ConfigT) *ServerBuilder {
	b.namespace = namespace
	b.configs = map[string]backendconfig.ConfigT{}
	for i := range configs {
		config := configs[i]
		b.configs[config.WorkspaceID] = config
	}
	return b
}

// WithWorkspaceConfig sets the workspace config for the server
func (b *ServerBuilder) WithWorkspaceConfig(config backendconfig.ConfigT) *ServerBuilder {
	b.configs = map[string]backendconfig.ConfigT{
		config.WorkspaceID: config,
	}
	return b
}

// Build builds the test server
func (b *ServerBuilder) Build() *httptest.Server {
	mux := http.NewServeMux()
	if b.namespace != "" {
		mux.HandleFunc(fmt.Sprintf("/data-plane/v1/namespaces/%s/config", b.namespace), func(w http.ResponseWriter, r *http.Request) {
			response, _ := json.Marshal(b.configs)
			_, _ = w.Write(response)
		})
	} else {
		mux.HandleFunc("/workspaceConfig", func(w http.ResponseWriter, r *http.Request) {
			response, _ := json.Marshal(lo.Values(b.configs)[0])
			_, _ = w.Write(response)
		})
	}

	return httptest.NewServer(mux)
}
