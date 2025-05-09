package auth

import (
	"context"
	"errors"
	"net/http"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"

	"github.com/rudderlabs/rudder-server/gateway/response"
)

var ErrSourceNotFound = errors.New("source not found")

type WebhookAuth struct {
	onFailure             func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext)
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error)
}

func NewWebhookAuth(
	onFailure func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext),
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
			wa.onFailure(w, r, response.NoWriteKeyInQueryParams, nil)
			return
		}
		arctx, err := wa.authReqCtxForWriteKey(writeKey)
		if err != nil {
			if errors.Is(err, ErrSourceNotFound) {
				wa.onFailure(w, r, response.InvalidWriteKey, arctx)
				return
			}
			wa.onFailure(w, r, response.ErrAuthenticatingWebhookRequest, arctx)
			return
		}
		if arctx.SourceCategory != "webhook" {
			wa.onFailure(w, r, response.InvalidWriteKey, arctx)
			return
		}
		if !arctx.SourceEnabled {
			wa.onFailure(w, r, response.SourceDisabled, arctx)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}
