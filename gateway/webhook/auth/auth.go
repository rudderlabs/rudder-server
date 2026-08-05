package auth

import (
	"context"
	"errors"
	"net/http"

	"github.com/rudderlabs/rudder-server/gateway/response"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
)

var (
	ErrSourceNotFound     = errors.New("source not found")
	ErrWorkspaceDisrupted = errors.New("workspace service disrupted")
)

type WebhookAuth struct {
	onFailure             func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext)
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error)
	isWorkspaceDisrupted  func(*gwtypes.AuthRequestContext) bool
}

func NewWebhookAuth(
	onFailure func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext),
	authReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error),
	isWorkspaceDisrupted ...func(*gwtypes.AuthRequestContext) bool,
) *WebhookAuth {
	wa := &WebhookAuth{
		onFailure:             onFailure,
		authReqCtxForWriteKey: authReqCtxForWriteKey,
	}
	if len(isWorkspaceDisrupted) > 0 {
		wa.isWorkspaceDisrupted = isWorkspaceDisrupted[0]
	}
	return wa
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
			switch {
			case errors.Is(err, ErrSourceNotFound):
				wa.onFailure(w, r, response.InvalidWriteKey, arctx)
			case errors.Is(err, ErrWorkspaceDisrupted):
				wa.onFailure(w, r, response.ServiceDisrupted, arctx)
			default:
				wa.onFailure(w, r, response.ErrAuthenticatingWebhookRequest, arctx)
			}
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
		if wa.isWorkspaceDisrupted != nil && wa.isWorkspaceDisrupted(arctx) {
			wa.onFailure(w, r, response.ServiceDisrupted, arctx)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamAuthRequestContext, arctx)))
	}
}
