package auth

import (
	"context"
	"errors"
	"net/http"

	"github.com/rudderlabs/rudder-server/gateway/response"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
)

var ErrSourceNotFound = errors.New("source not found")

type WebhookAuth struct {
	onFailure             func(w http.ResponseWriter, r *http.Request, errorMessage string, reason gwtypes.StatReason, authCtx *gwtypes.AuthRequestContext)
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error)
}

func NewWebhookAuth(
	onFailure func(w http.ResponseWriter, r *http.Request, errorMessage string, reason gwtypes.StatReason, authCtx *gwtypes.AuthRequestContext),
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

		var writeKey string
		if writeKeys, found := r.URL.Query()["writeKey"]; found && writeKeys[0] != "" {
			writeKey = writeKeys[0]
		} else {
			writeKey, _, _ = r.BasicAuth()
		}
		if writeKey == "" {
			wa.onFailure(w, r, response.NoWriteKeyInQueryParams, gwtypes.ReasonNoWriteKeyInQueryParams, nil)
			return
		}
		arctx, err := wa.authReqCtxForWriteKey(writeKey)
		if err != nil {
			if errors.Is(err, ErrSourceNotFound) {
				wa.onFailure(w, r, response.InvalidWriteKey, gwtypes.ReasonInvalidWriteKey, arctx)
				return
			}
			wa.onFailure(w, r, response.ErrAuthenticatingWebhookRequest, gwtypes.ReasonAuthenticatingWebhookRequest, arctx)
			return
		}
		if arctx.SourceCategory != "webhook" {
			wa.onFailure(w, r, response.InvalidWriteKey, gwtypes.ReasonInvalidWriteKey, arctx)
			return
		}
		if !arctx.SourceEnabled {
			wa.onFailure(w, r, response.SourceDisabled, gwtypes.ReasonSourceDisabled, arctx)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}
