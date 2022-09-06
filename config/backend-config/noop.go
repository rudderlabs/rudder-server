package backendconfig

import (
	"context"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var _ BackendConfig = &NOOP{}

type NOOP struct{}

func (*NOOP) AccessToken() string {
	return ""
}

func (*NOOP) Identity() identity.Identifier {
	return &identity.NOOP{}
}

func (*NOOP) SetUp() error {
	return nil
}

func (*NOOP) Get(_ context.Context, _ string) (ConfigT, error) {
	return ConfigT{}, nil
}

func (*NOOP) GetWorkspaceIDForWriteKey(string) string {
	return ""
}

func (*NOOP) GetWorkspaceIDForSourceID(_ string) string {
	return ""
}

func (*NOOP) GetWorkspaceLibrariesForWorkspaceID(string) LibrariesT {
	return LibrariesT{}
}

func (*NOOP) WaitForConfig(_ context.Context) {}

func (*NOOP) Subscribe(ctx context.Context, _ Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent)

	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (*NOOP) StartWithIDs(_ context.Context, _ string) {}

func (*NOOP) Stop() {
}
