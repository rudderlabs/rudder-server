package workspace

type ChangeEvent struct {
	err          error
	ack          func() error
	workspaceIDs []string
}

func NewWorkspacesRequest(workspaceIDs []string, ack func() error) ChangeEvent {
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

func (m ChangeEvent) Ack() error {
	return m.ack()
}

func (m ChangeEvent) WorkspaceIDs() []string {
	return m.workspaceIDs
}

func (m ChangeEvent) Err() error {
	return m.err
}
