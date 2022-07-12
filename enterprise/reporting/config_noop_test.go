package reporting

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var _ backendconfig.BackendConfig = &NOOPConfig{}

type NOOPConfig struct{}

func (noop *NOOPConfig) IsConfigured() bool {
	return true
}

func (noop *NOOPConfig) AccessToken() string {
	return ""
}

func (noop *NOOPConfig) SetUp() {
	return
}

func (noop *NOOPConfig) Get(workspace string) (backendconfig.ConfigT, bool) {
	return backendconfig.ConfigT{}, false
}

func (noop *NOOPConfig) GetWorkspaceIDForWriteKey(string) string {
	return ""
}

func (noop *NOOPConfig) GetWorkspaceIDForSourceID(sourceID string) string {
	return ""
}

func (noop *NOOPConfig) GetWorkspaceLibrariesForWorkspaceID(string) backendconfig.LibrariesT {
	return backendconfig.LibrariesT{}
}

func (noop *NOOPConfig) WaitForConfig(ctx context.Context) error {
	return nil
}

func (noop *NOOPConfig) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)

	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (noop *NOOPConfig) StartWithIDs(workspaces string) {
	return
}

func (noop *NOOPConfig) Stop() {
	return
}
