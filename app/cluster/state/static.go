package state

import (
	"context"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

var _ cluster.ModeProvider = &StaticProvider{}

type StaticProvider struct {
	Mode servermode.Mode
	// TODO: WorkspaceIDs
}

// ServerMode returns a channel with a single server mode, the one provided in the constructor.
func (s *StaticProvider) ServerMode(ctx context.Context) <-chan servermode.ModeRequest {
	ch := make(chan servermode.ModeRequest, 1)
	ch <- servermode.NewModeRequest(servermode.Mode(s.Mode), func() error {
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
func (s *StaticProvider) WorkspaceIDs(ctx context.Context) <-chan workspace.WorkspacesRequest {
	ch := make(chan workspace.WorkspacesRequest, 0)

	go func() {
		<-ctx.Done()
		close(ch)
	}()

	return ch
}
