package types

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type ContextKey string

const (
	// CtxParamCallType is the key for the call type in the request context.
	CtxParamCallType ContextKey = "rudder.gateway.callType"
	// CtxParamAuthRequestContext is the key for the auth request context in the request context.
	CtxParamAuthRequestContext ContextKey = "rudder.gateway.authRequestContext"
)

// AuthRequestContext contains the authenticated source information for a request.
type AuthRequestContext struct {
	SourceEnabled   bool
	SourceID        string
	WriteKey        string
	WorkspaceID     string
	SourceName      string
	SourceDefName   string
	SourceCategory  string
	ReplaySource    bool
	SourceJobRunID  string
	SourceTaskRunID string
	Source          backendconfig.SourceT
	// DestinationID is optional param, destination id will be present for rETL Request
	DestinationID string
}

func (arctx *AuthRequestContext) SourceTag() string {
	return misc.GetTagName(arctx.WriteKey, arctx.SourceName)
}
