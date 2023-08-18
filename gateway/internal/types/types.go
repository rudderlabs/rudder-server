package types

import "github.com/rudderlabs/rudder-server/utils/misc"

type ContextKey string

const (
	// CtxParamCallType is the key for the call type in the request context.
	CtxParamCallType ContextKey = "rudder.gateway.callType"
	// CtxParamAuthRequestContext is the key for the auth request context in the request context.
	CtxParamAuthRequestContext ContextKey = "rudder.gateway.authRequestContext"
)

// AuthRequestContext contains the authenticated source information for a request.
type AuthRequestContext struct {
	SourceEnabled  bool
	SourceID       string
	WriteKey       string
	WorkspaceID    string
	SourceName     string
	SourceDefName  string
	SourceCategory string

	SourceJobRunID  string
	SourceTaskRunID string
}

func (arctx *AuthRequestContext) SourceTag() string {
	return misc.GetTagName(arctx.WriteKey, arctx.SourceName)
}
