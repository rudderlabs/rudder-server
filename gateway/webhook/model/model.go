package model

import gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"

type FailedWebhookPayload struct {
	RequestContext *gwtypes.AuthRequestContext
	Payload        []byte
	SourceType     string
	Reason         string
}
