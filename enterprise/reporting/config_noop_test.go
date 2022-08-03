package reporting

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var _ backendconfig.BackendConfig = &NOOPConfig{}

type NOOPConfig struct{}

func (*NOOPConfig) AccessToken() string {
	return ""
}

func (*NOOPConfig) SetUp() error {
	return nil
}

func (*NOOPConfig) Get(_ context.Context, _ string) (backendconfig.ConfigT, error) {
	return backendconfig.ConfigT{}, nil
}

func (*NOOPConfig) GetWorkspaceIDForWriteKey(string) string {
	return ""
}

func (noop *NOOPConfig) GetWorkspaceIDForSourceID(_ string) string {
	return ""
}

func (*NOOPConfig) GetWorkspaceLibrariesForWorkspaceID(string) backendconfig.LibrariesT {
	return backendconfig.LibrariesT{}
}

func (*NOOPConfig) WaitForConfig(_ context.Context) {}

func (*NOOPConfig) Subscribe(ctx context.Context, _ backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)

	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (*NOOPConfig) StartWithIDs(_ context.Context, _ string) {}

func (*NOOPConfig) Stop() {
	return
}
