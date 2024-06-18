package backendconfigtest

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var _ backendconfig.BackendConfig = &StaticLibrary{}

func NewStaticLibrary(configs map[string]backendconfig.ConfigT) *StaticLibrary {
	return &StaticLibrary{configs: configs}
}

type StaticLibrary struct {
	configs map[string]backendconfig.ConfigT
}

// AccessToken returns the access token for the backend config
func (l *StaticLibrary) AccessToken() string {
	panic("not implemented") // TODO: Implement
}

func (l *StaticLibrary) Get(context.Context) (map[string]backendconfig.ConfigT, error) {
	return l.configs, nil
}

func (l *StaticLibrary) Identity() identity.Identifier {
	panic("not implemented") // TODO: Implement
}

func (l *StaticLibrary) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	ch := make(chan pubsub.DataEvent, 1)
	ch <- pubsub.DataEvent{Data: l.configs, Topic: string(topic)}
	// on Subscribe, emulate a single backend configuration event
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (l *StaticLibrary) SetUp() error {
	return nil
}

func (l *StaticLibrary) WaitForConfig(context.Context) {}

func (l *StaticLibrary) Stop() {}

func (l *StaticLibrary) StartWithIDs(context.Context, string) {}
