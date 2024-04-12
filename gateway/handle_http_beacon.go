package gateway

import (
	"net/http"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

// beaconBatchHandler can handle beacon batch requests where writeKey is passed as a query param
func (gw *Handle) beaconBatchHandler() http.HandlerFunc {
	return gw.beaconInterceptor(gw.webBatchHandler())
}

// beaconInterceptor reads the writeKey from the query params and sets it in the request Authorization header
func (gw *Handle) beaconInterceptor(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queryParams := r.URL.Query()
		if writeKey, present := queryParams["writeKey"]; present && writeKey[0] != "" {
			// set basic auth header
			r.SetBasicAuth(writeKey[0], "")
			delete(queryParams, "writeKey")
			// send req to webHandler
			delegate(w, r)
		} else {
			status := http.StatusUnauthorized
			responseBody := response.NoWriteKeyInQueryParams
			stat := gwstats.SourceStat{
				Source:   "invalidWriteKey",
				SourceID: "invalidWriteKey",
				WriteKey: "invalidWriteKey",
				ReqType:  "beacon",
			}
			stat.RequestFailed("invalidWriteKey")
			stat.Report(gw.stats)
			gw.logger.Infow("response",
				"ip", kithttputil.GetRequestIP(r),
				"path", r.URL.Path,
				"status", status,
				"body", responseBody)
			http.Error(w, responseBody, status)
		}
	}
}
