package state

import (
	"context"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

var _ cluster.ChangeEventProvider = &StaticProvider{}

type StaticProvider struct {
	mode servermode.Mode
	// TODO: WorkspaceIDs
}

func NewStaticProvider(Mode servermode.Mode) *StaticProvider {
	return &StaticProvider{
		mode: Mode,
	}
}

// ServerMode returns a channel with a single message containing this static provider's mode.
func (s *StaticProvider) ServerMode(ctx context.Context) <-chan servermode.ChangeEvent {
	ch := make(chan servermode.ChangeEvent, 1)
	ch <- servermode.NewChangeEvent(s.mode, func(ctx context.Context) error {
		return nil
	})

	go func() {
		<-ctx.Done()
		close(ch)
	}()

	return ch
}

// WorkspaceIDs returns an empty channel, since we don't expect workspaceIDs updates with static provider
// TODO: This method should return proper workspaceIDs for backend config, even with static provide.
func (*StaticProvider) WorkspaceIDs(ctx context.Context) <-chan workspace.ChangeEvent {
	ch := make(chan workspace.ChangeEvent)

	go func() {
		<-ctx.Done()
		close(ch)
	}()

	return ch
}
