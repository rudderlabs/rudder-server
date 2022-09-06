package identity

import "net/http"

type Identifier interface {
	ID() string
	HTTPAuth(req *http.Request)
	Resource() string
}

var (
	_ Identifier = (*Namespace)(nil)
	_ Identifier = (*Workspace)(nil)
)

type Namespace struct {
	Namespace    string
	HostedSecret string
}

func (n *Namespace) ID() string {
	return n.Namespace
}

func (n *Namespace) HTTPAuth(req *http.Request) {
	req.SetBasicAuth(n.HostedSecret, "")
}

func (*Namespace) Resource() string {
	return "namespaces"
}

type Workspace struct {
	WorkspaceID    string
	WorkspaceToken string
}

func (w *Workspace) ID() string {
	return w.WorkspaceID
}

func (w *Workspace) HTTPAuth(req *http.Request) {
	req.SetBasicAuth(w.WorkspaceToken, "")
}

func (*Workspace) Resource() string {
	return "workspace"
}

type NOOP struct{}

func (*NOOP) ID() string {
	return ""
}

func (*NOOP) HTTPAuth(_ *http.Request) {
}

func (*NOOP) Resource() string {
	return ""
}
