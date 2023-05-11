package identity

import "github.com/rudderlabs/rudder-server/utils/types/deployment"

// Authorizer abstracts how data-plane can be authorized by the control-plane.
type Authorizer interface {
	BasicAuth() (string, string)
}

// Identifier abstracts how data-plane can be identified to the control-plane.
//
//	Including both a unique identifier and the authentication method.
type Identifier interface {
	Authorizer
	ID() string
	Type() deployment.Type
}

var (
	_ Identifier = (*Namespace)(nil)
	_ Identifier = (*Workspace)(nil)
	_ Identifier = (*NOOP)(nil)
)

// Workspace identifier represents a single customer's workspace.
// Less flexible than a namespace, it does not allow for multitenant.
type Workspace struct {
	WorkspaceID    string
	WorkspaceToken string
}

func (w *Workspace) ID() string {
	return w.WorkspaceID
}

func (w *Workspace) BasicAuth() (string, string) {
	return w.WorkspaceToken, ""
}

func (*Workspace) Type() deployment.Type {
	return deployment.DedicatedType
}

// Namespace identifier represents a group of workspaces that share a common resource.
//
//	Namespace is used but is not limited to implemented multi-tenancy.
//	It also allows for more complex entity relations.
type Namespace struct {
	Namespace    string
	HostedSecret string
}

func (n *Namespace) ID() string {
	return n.Namespace
}

func (n *Namespace) BasicAuth() (string, string) {
	return n.HostedSecret, ""
}

func (*Namespace) Type() deployment.Type {
	return deployment.MultiTenantType
}

// NOOP is a no-op implementation of the Identifier interface.
// Used only for testing purposes.
type NOOP struct{}

func (*NOOP) ID() string {
	return ""
}

func (*NOOP) BasicAuth() (string, string) {
	return "", ""
}

func (*NOOP) Type() deployment.Type {
	return ""
}

// Admin is an implementation of the Authorizer interface for data-plane admin endpoints.
type Admin struct {
	Username, Password string
}

func (a *Admin) BasicAuth() (string, string) {
	return a.Username, a.Password
}

type IdentifierDecorator struct {
	Identifier
	Id string
}

func (d *IdentifierDecorator) ID() string {
	return d.Id
}
