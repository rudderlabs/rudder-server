package gateway

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

type ReplayRequestHandler struct {
	*Handle
}

func (rerh *ReplayRequestHandler) ProcessRequest(_ *http.ResponseWriter, r *http.Request, _ string, _ []byte, _ string) string {
	r.Header.Get(jobRunIDHeader)
	r.Header.Get(taskRunIDHeader)
	return ""
}

func (rerh *ReplayRequestHandler) AuthenticateRequest(r *http.Request, reqType string) (string, string, error) {
	sourceID := chi.URLParam(r, "sourceID")
	if sourceID == "" {
		err := errors.New(response.NoSourceIDInHeader)
		stat := gwstats.SourceStat{
			Source:   "noWriteKey",
			SourceID: "noSourceID",
			WriteKey: "noWriteKey",
			ReqType:  reqType,
		}
		stat.RequestFailed("noSourceIDInHeader")
		stat.Report(rerh.stats)
		return "", "", err
	}
	return sourceID, sourceID, nil
}
