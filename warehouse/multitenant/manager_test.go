package multitenant_test

import (
	"context"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var _ backendconfig.BackendConfig = (*mockBackendConfig)(nil)

type mockBackendConfig struct {
	config map[string]backendconfig.ConfigT
}

func (m *mockBackendConfig) WaitForConfig(ctx context.Context)                   {}
func (m *mockBackendConfig) Stop()                                               {}
func (m *mockBackendConfig) StartWithIDs(ctx context.Context, workspaces string) {}
func (m *mockBackendConfig) SetUp() error                                        { return nil }
func (m *mockBackendConfig) AccessToken() string                                 { return "" }
func (m *mockBackendConfig) Identity() identity.Identifier                       { return nil }

func (m *mockBackendConfig) Get(context.Context, string) (map[string]backendconfig.ConfigT, error) {
	return m.config, nil
}

func (m *mockBackendConfig) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)
	go func() {
		ch <- pubsub.DataEvent{
			Data: m.config,
		}

		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func TestWatchConfig(t *testing.T) {
}

func TestSourceToWorkspace(t *testing.T) {
}

func TestDegradedWorkspace(t *testing.T) {
}
