package context

import (
	"context"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
)

// GetRequestTypeFromCtx : get request type from context
func GetRequestTypeFromCtx(ctx context.Context) (string, bool) {
	reqType, ok := ctx.Value(gwtypes.CtxParamCallType).(string)
	return reqType, ok
}

// GetAuthRequestFromCtx : get auth request from context
func GetAuthRequestFromCtx(ctx context.Context) (*gwtypes.AuthRequestContext, bool) {
	authReqCtx, ok := ctx.Value(gwtypes.CtxParamAuthRequestContext).(*gwtypes.AuthRequestContext)
	return authReqCtx, ok
}
