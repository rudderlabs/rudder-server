package gateway

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func (gw *Handle) webhookHandler() http.HandlerFunc {
	return gw.callType("webhook", gw.webhookAuth(gw.webhook.RequestHandler))
}

// ProcessWebRequest is an interface wrapper for webhook
func (gw *Handle) ProcessWebRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, arctx *gwtypes.AuthRequestContext) string {
	return gw.rrh.ProcessRequest(w, r, reqType, payload, arctx)
}

// SaveWebhookFailures saves errors to the error db
func (gw *Handle) SaveWebhookFailures(reqs []*model.FailedWebhookPayload) error {
	jobs := make([]*jobsdb.JobT, 0, len(reqs))
	for _, req := range reqs {
		params := map[string]interface{}{
			"source_id":   req.RequestContext.SourceID,
			"stage":       "webhook",
			"source_type": req.SourceType,
			"reason":      req.Reason,
		}
		marshalledParams, err := json.Marshal(params)
		if err != nil {
			gw.logger.Errorf("[Gateway] Failed to marshal parameters map. Parameters: %+v", params)
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
	return gw.errDB.Store(ctx, jobs)
}
