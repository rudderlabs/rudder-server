package types

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
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
	// deprecated in favor of SourceDetails
	Source backendconfig.SourceT
	// DestinationID is optional param, destination id will be present for rETL Request
	DestinationID string
	SourceDetails struct {
		ID               string
		OriginalID       string
		Name             string
		SourceDefinition struct {
			ID       string
			Name     string
			Category string
			Type     string
		}
		Enabled     bool
		WorkspaceID string
		WriteKey    string
		Config      map[string]interface{}
	}
}

func (arctx *AuthRequestContext) SourceTag() string {
	return misc.GetTagName(arctx.WriteKey, arctx.SourceName)
}

type StatReporter interface {
	Report(s stats.Stats)
	RequestFailed(errMsg string)
	RequestDropped()
	RequestSucceeded()
}
