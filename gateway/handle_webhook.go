package gateway

import (
	"context"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/jobsdb"
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

// SaveWebhookFailures saves errors to the error db
func (gw *Handle) SaveWebhookFailures(reqs []*model.FailedWebhookPayload) error {
	if !gw.conf.errorDBEnabled.Load() {
		return nil
	}

	jobs := make([]*jobsdb.JobT, 0, len(reqs))
	for _, req := range reqs {
		params := map[string]any{
			"source_id":   req.RequestContext.SourceID,
			"stage":       "webhook",
			"source_type": req.SourceType,
			"reason":      req.Reason,
		}
		marshalledParams, err := jsonrs.Marshal(params)
		if err != nil {
			gw.logger.Errorn("[Gateway] Failed to marshal parameters map",
				obskit.SourceID(req.RequestContext.SourceID),
				obskit.SourceType(req.SourceType),
				logger.NewStringField("stage", "webhook"),
				logger.NewStringField("reason", req.Reason),
				obskit.Error(err),
			)
			marshalledParams = []byte(`{"error": "rudder-server gateway failed to marshal params"}`)
		}

		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       uuid.New().String(), // Using a random userid for these failures. There is no notion of user id for these events.
			Parameters:   marshalledParams,
			CustomVal:    "WEBHOOK",
			EventPayload: req.Payload,
			EventCount:   1,
			WorkspaceId:  req.RequestContext.WorkspaceID,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), gw.conf.WriteTimeout)
	defer cancel()
	return gw.errorDB.Store(ctx, jobs)
}
