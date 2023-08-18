package gateway

import (
	"context"
	"net/http"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// webAudienceListHandler - handler for audience list requests
func (gw *Handle) webAudienceListHandler() http.HandlerFunc {
	return gw.callType("audiencelist", gw.writeKeyAuth(gw.webHandler()))
}

// webExtractHandler - handler for extract requests
func (gw *Handle) webExtractHandler() http.HandlerFunc {
	return gw.callType("extract", gw.writeKeyAuth(gw.webHandler()))
}

// webBatchHandler - handler for batch requests
func (gw *Handle) webBatchHandler() http.HandlerFunc {
	return gw.callType("batch", gw.writeKeyAuth(gw.webHandler()))
}

// webIdentifyHandler - handler for identify requests
func (gw *Handle) webIdentifyHandler() http.HandlerFunc {
	return gw.callType("identify", gw.writeKeyAuth(gw.webHandler()))
}

// webTrackHandler - handler for track requests
func (gw *Handle) webTrackHandler() http.HandlerFunc {
	return gw.callType("track", gw.writeKeyAuth(gw.webHandler()))
}

// webPageHandler - handler for page requests
func (gw *Handle) webPageHandler() http.HandlerFunc {
	return gw.callType("page", gw.writeKeyAuth(gw.webHandler()))
}

// webScreenHandler - handler for screen requests
func (gw *Handle) webScreenHandler() http.HandlerFunc {
	return gw.callType("screen", gw.writeKeyAuth(gw.webHandler()))
}

// webAliasHandler - handler for alias requests
func (gw *Handle) webAliasHandler() http.HandlerFunc {
	return gw.callType("alias", gw.writeKeyAuth(gw.webHandler()))
}

// webMergeHandler - handler for merge requests
func (gw *Handle) webMergeHandler() http.HandlerFunc {
	return gw.callType("merge", gw.writeKeyAuth(gw.webHandler()))
}

// webGroupHandler - handler for group requests
func (gw *Handle) webGroupHandler() http.HandlerFunc {
	return gw.callType("group", gw.writeKeyAuth(gw.webHandler()))
}

// robotsHandler prevents robots from crawling the gateway endpoints
func (*Handle) robotsHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("User-agent: * \nDisallow: / \n"))
}

// eventSchemaController middleware checks if the event schemas feature is enabled. If not, it returns a 400 response
func (gw *Handle) eventSchemaController(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !gw.conf.enableEventSchemasFeature {
			status := http.StatusBadRequest
			responseBody := response.MakeResponse("EventSchemas feature is disabled")
			gw.logger.Infow("response",
				"ip", misc.GetIPFromReq(r),
				"path", r.URL.Path,
				"status", status,
				"body", responseBody)
			http.Error(w, responseBody, status)
			return
		}
		wrappedFunc(w, r)
	}
}

// webHandler - regular web request handler
func (gw *Handle) webHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gw.webRequestHandler(gw.rrh, w, r)
	}
}

// webRequestHandler - handles web requests containing rudder events as payload.
// It parses the payload and calls the request handler to process the request.
func (gw *Handle) webRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request) {
	reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
	arctx := r.Context().Value(gwtypes.CtxParamAuthRequestContext).(*gwtypes.AuthRequestContext)
	gw.logger.LogRequest(r)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			status := response.GetErrorStatusCode(errorMessage)
			responseBody := response.GetStatus(errorMessage)
			gw.logger.Infow("response",
				"ip", misc.GetIPFromReq(r),
				"path", r.URL.Path,
				"status", status,
				"body", responseBody)
			http.Error(w, responseBody, status)
			return
		}
	}()
	payload, err := gw.getPayload(arctx, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(&w, r, reqType, payload, arctx)
	gw.TrackRequestMetrics(errorMessage)
	if errorMessage != "" {
		return
	}

	responseBody := response.GetStatus(response.Ok)
	gw.logger.Debugw("response",
		"ip", misc.GetIPFromReq(r),
		"path", r.URL.Path,
		"status", http.StatusOK,
		"body", responseBody)
	_, _ = w.Write([]byte(responseBody))
}

// callType middleware sets the call type in the request context
func (gw *Handle) callType(callType string, delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), gwtypes.CtxParamCallType, callType))
		delegate(w, r)
	}
}

// withContentType sets the content type of the response to the given value
func withContentType(contentType string, delegate http.HandlerFunc) http.HandlerFunc { // nolint: unparam
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", contentType)
		delegate(w, r)
	}
}
