package workspace

import "context"

type ChangeEvent struct {
	err          error
	ack          func(context.Context) error
	workspaceIDs []string
}

func NewWorkspacesRequest(workspaceIDs []string, ack func(context.Context) error) ChangeEvent {
	return ChangeEvent{
		workspaceIDs: workspaceIDs,
		ack:          ack,
	}
}

func ChangeEventError(err error) ChangeEvent {
	return ChangeEvent{
		err: err,
	}
}

func (m ChangeEvent) Ack(ctx context.Context) error {
	return m.ack(ctx)
}

func (m ChangeEvent) WorkspaceIDs() []string {
	return m.workspaceIDs
}

func (m ChangeEvent) Err() error {
	return m.err
}
