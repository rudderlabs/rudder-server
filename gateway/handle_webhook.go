package gateway

import (
	"net/http"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
)

func (gw *Handle) webhookHandler() http.HandlerFunc {
	if gw.conf.webhookV2HandlerEnabled {
		return gw.callType("webhook", gw.webhookAuthMiddleware.AuthHandler(gw.webhook.RequestHandler))
	}
	return gw.callType("webhook", gw.webhookAuth(gw.webhook.RequestHandler))
}

// ProcessTransformedWebhookRequest is an interface wrapper for webhook
func (gw *Handle) ProcessTransformedWebhookRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, arctx *gwtypes.AuthRequestContext) string {
	return gw.rrh.ProcessRequest(w, r, reqType, payload, arctx)
}

func (gw *Handle) SaveWebhookFailures(reqs []*model.FailedWebhookPayload) error {
	// no-op
	return nil
}
