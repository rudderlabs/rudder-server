package auth

import (
	"context"
	"net/http"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"

	"github.com/rudderlabs/rudder-server/gateway/response"
)

type WebhookAuth struct {
	onFailure             func(w http.ResponseWriter, r *http.Request, errorMessage string)
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error)
}

func NewWebhookAuth(
	onFailure func(w http.ResponseWriter, r *http.Request, errorMessage string),
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error),
) *WebhookAuth {
	return &WebhookAuth{
		onFailure:             onFailure,
		authReqCtxForWriteKey: authReqCtxForWriteKey,
	}
}

func (wa *WebhookAuth) AuthHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var arctx *gwtypes.AuthRequestContext
		var errorMessage string
		defer func() {
			if errorMessage != "" {
				wa.onFailure(w, r, errorMessage)
			}
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
		arctx, err := wa.authReqCtxForWriteKey(writeKey)
		if err != nil {
			errorMessage = response.ErrAuthenticatingWebhookRequest
			return
		}
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
