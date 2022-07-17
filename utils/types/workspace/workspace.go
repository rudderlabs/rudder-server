package workspace

import "context"

type ChangeEvent struct {
	err          error
	ack          func(context.Context, error) error
	workspaceIDs []string
}

func NewWorkspacesRequest(
	workspaceIDs []string,
	ack func(context.Context, error) error,
) ChangeEvent {
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

func (m ChangeEvent) Ack(ctx context.Context, err error) error {
	return m.ack(ctx, err)
}

func (m ChangeEvent) WorkspaceIDs() []string {
	return m.workspaceIDs
}

func (m ChangeEvent) Err() error {
	return m.err
}
