package features

import "net/http"

type identity interface {
	ID() string
	HTTPAuth(req *http.Request)
	Resource() string
}

type Namespace struct {
	NamespaceID  string
	HostedSecret string
}

func (c *Namespace) ID() string {
	return c.NamespaceID
}
func (c *Namespace) HTTPAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.HostedSecret)
}
func (c *Namespace) Resource() string {
	return "namespaces"
}

type Workspace struct {
	WorkspaceID    string
	WorkspaceToken string
}

func (c *Workspace) ID() string {
	return c.WorkspaceID
}
func (c *Workspace) HTTPAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.WorkspaceToken)
}
func (c *Workspace) Resource() string {
	return "namespaces"
}
