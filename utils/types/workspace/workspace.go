package workspace

type WorkspacesRequest struct {
	err          error
	ack          func() error
	workspaceIDs []string
}

func (m WorkspacesRequest) Ack() error {
	return m.ack()
}

func (m WorkspacesRequest) WorkspaceIDs() []string {
	return m.workspaceIDs
}

func (m WorkspacesRequest) Err() error {
	return m.err
}

func NewWorkspacesRequest(workspaceIDs []string, ack func() error) WorkspacesRequest {
	return WorkspacesRequest{
		workspaceIDs: workspaceIDs,
		ack:          ack,
	}
}

func WorkspacesError(err error) WorkspacesRequest {
	return WorkspacesRequest{
		err: err,
	}
}
