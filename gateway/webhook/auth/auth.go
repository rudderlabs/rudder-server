package auth

import (
	"context"
	"net/http"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

type WebhookAuth struct {
	onFailure             func(errorMessage, reqType string)
	authReqCtxForWriteKey func(writeKey string) *gwtypes.AuthRequestContext
}

func NewWebhookAuth(
	onFailure func(errorMessage, reqType string),
	authReqCtxForWriteKey func(writeKey string) *gwtypes.AuthRequestContext,
) *WebhookAuth {
	return &WebhookAuth{
		onFailure:             onFailure,
		authReqCtxForWriteKey: authReqCtxForWriteKey,
	}
}

func (wa *WebhookAuth) AuthHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
		var arctx *gwtypes.AuthRequestContext
		var errorMessage string
		defer func() {
			wa.onFailure(errorMessage, reqType)
		}()

		var writeKey string
		if writeKeys, found := r.URL.Query()["writeKey"]; found && writeKeys[0] != "" {
			writeKey = writeKeys[0]
		} else {
			writeKey, _, _ = r.BasicAuth()
		}
		if writeKey == "" {
			errorMessage = response.NoWriteKeyInQueryParams
			return
		}
		arctx = wa.authReqCtxForWriteKey(writeKey)
		if arctx == nil || arctx.SourceCategory != "webhook" {
			errorMessage = response.InvalidWriteKey
			return
		}
		if !arctx.SourceEnabled {
			errorMessage = response.SourceDisabled
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}
