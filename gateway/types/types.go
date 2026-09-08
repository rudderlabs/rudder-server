package types

import (
	"encoding/json"

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
		Config      json.RawMessage
	}
}

func (arctx *AuthRequestContext) SourceTag() string {
	return misc.GetTagName(arctx.WriteKey, arctx.SourceName)
}

type StatReporter interface {
	Report(s stats.Stats)
	// RequestFailed reports a request that broke, with the reason it broke. The reason is a closed set (see
	// StatReason) so that an implementation can switch over it rather than match strings.
	RequestFailed(reason StatReason)
	// RequestDropped reports a request that was turned away deliberately, with the reason it was turned away - a rate
	// limit, say, which an implementation may want to tell apart from a failure.
	RequestDropped(reason StatReason)
	RequestSucceeded()
}
