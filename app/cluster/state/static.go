package state

import (
	"context"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

var _ cluster.ModeProvider = &StaticProvider{}

type StaticProvider struct {
	Mode       servermode.Mode
	Workspaces []string
}

func (s *StaticProvider) ServerMode(ctx context.Context) <-chan servermode.ModeRequest {
	ch := make(chan servermode.ModeRequest, 1)
	ch <- servermode.NewModeRequest(servermode.Mode(s.Mode), func() error {
		return nil
	})
	return ch
}

func (s *StaticProvider) WorkspaceIDs(ctx context.Context) <-chan workspace.WorkspacesRequest {
	ch := make(chan workspace.WorkspacesRequest, 1)

	ch <- workspace.NewWorkspacesRequest([]string{}, func() error {
		return nil
	})

	return ch
}
