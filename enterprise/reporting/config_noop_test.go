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

func (noop *NOOPConfig) Get(_ context.Context, _ string) (backendconfig.ConfigT, error) {
	return backendconfig.ConfigT{}, nil
}

func (noop *NOOPConfig) GetWorkspaceIDForWriteKey(string) string {
	return ""
}

func (noop *NOOPConfig) GetWorkspaceIDForSourceID(_ string) string {
	return ""
}

func (noop *NOOPConfig) GetWorkspaceLibrariesForWorkspaceID(string) backendconfig.LibrariesT {
	return backendconfig.LibrariesT{}
}

func (noop *NOOPConfig) WaitForConfig(_ context.Context) error {
	return nil
}

func (noop *NOOPConfig) Subscribe(ctx context.Context, _ backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)

	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (noop *NOOPConfig) StartWithIDs(_ context.Context, _ string) {
	return
}

func (noop *NOOPConfig) Stop() {
	return
}
