package gateway

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (gw *Handle) pixelWebHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gw.pixelWebRequestHandler(gw.rrh, w, r, reqType)
}

func (gw *Handle) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gw.webRequestHandler(gw.rrh, w, r, reqType)
}

func (gw *Handle) webRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	webReqHandlerTime := gw.stats.NewTaggedStat("gw.web_req_handler_time", stats.TimerType, stats.Tags{"reqType": reqType})
	webReqHandlerStartTime := time.Now()
	defer webReqHandlerTime.Since(webReqHandlerStartTime)

	gw.logger.LogRequest(r)
	atomic.AddUint64(&gw.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gw.logger.Infof("IP: %s -- %s -- Response: %d, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetErrorStatusCode(errorMessage), errorMessage)
			http.Error(w, response.GetStatus(errorMessage), response.GetErrorStatusCode(errorMessage))
			return
		}
	}()
	_, writeKey, err := rh.AuthenticateRequest(r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	payload, err := gw.getPayloadFromRequest(r)

	errorMessage = rh.ProcessRequest(&w, r, reqType, payload, writeKey)
	atomic.AddUint64(&gw.ackCount, 1)
	gw.TrackRequestMetrics(errorMessage)
	if errorMessage != "" {
		return
	}
	gw.logger.Debugf("IP: %s -- %s -- Response: 200, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetStatus(response.Ok))

	httpWriteTime := gw.stats.NewTaggedStat("gw.http_write_time", stats.TimerType, stats.Tags{"reqType": reqType})
	httpWriteStartTime := time.Now()
	_, _ = w.Write([]byte(response.GetStatus(response.Ok)))
	httpWriteTime.Since(httpWriteStartTime)
}

func (gw *Handle) pixelWebRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	gw.sendPixelResponse(w)
	gw.logger.LogRequest(r)
	atomic.AddUint64(&gw.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gw.logger.Infow("Error while handling request",
				"ip", misc.GetIPFromReq(r),
				"path", r.URL.Path,
				"error", errorMessage)
		}
	}()
	payload, writeKey, err := gw.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(&w, r, reqType, payload, writeKey)

	atomic.AddUint64(&gw.ackCount, 1)
	gw.TrackRequestMetrics(errorMessage)
}

func (gw *Handle) sendPixelResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "image/gif")
	_, err := w.Write([]byte(response.GetPixelResponse()))
	if err != nil {
		gw.logger.Warnf("Error while sending pixel response: %v", err)
		return
	}
}

func (gw *Handle) webImportHandler(w http.ResponseWriter, r *http.Request) {
	gw.webRequestHandler(gw.irh, w, r, "import")
}

func (gw *Handle) webAudienceListHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "audiencelist")
}

func (gw *Handle) webExtractHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "extract")
}

func (gw *Handle) replayHandler(w http.ResponseWriter, r *http.Request) {
	gw.webRequestHandler(gw.rerh, w, r, "replay")
}

func (gw *Handle) webBatchHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "batch")
}

func (gw *Handle) webIdentifyHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "identify")
}

func (gw *Handle) webTrackHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "track")
}

func (gw *Handle) webPageHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "page")
}

func (gw *Handle) webScreenHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "screen")
}

func (gw *Handle) webAliasHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "alias")
}

func (gw *Handle) webMergeHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "merge")
}

func (gw *Handle) webGroupHandler(w http.ResponseWriter, r *http.Request) {
	gw.webHandler(w, r, "group")
}

func (gw *Handle) pixelPageHandler(w http.ResponseWriter, r *http.Request) {
	gw.pixelHandler(w, r, "page")
}

func (gw *Handle) pixelTrackHandler(w http.ResponseWriter, r *http.Request) {
	gw.pixelHandler(w, r, "track")
}

func (gw *Handle) beaconBatchHandler(w http.ResponseWriter, r *http.Request) {
	gw.beaconHandler(w, r, "batch")
}

func (gw *Handle) pixelHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if queryParams["writeKey"] != nil {
		writeKey := queryParams["writeKey"]
		// make a new request
		req, err := http.NewRequest(http.MethodPost, "", http.NoBody)
		if err != nil {
			gw.sendPixelResponse(w)
			return
		}
		// set basic auth header
		req.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// set X-Forwarded-For header
		req.Header.Add("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))

		// convert the pixel request(r) to a web request(req)
		err = gw.setWebPayload(req, queryParams, reqType)
		if err == nil {
			gw.pixelWebHandler(w, req, reqType)
		} else {
			gw.sendPixelResponse(w)
		}
	} else {
		gw.logger.Info(fmt.Sprintf("IP: %s -- %s -- Error while handling request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path))
		gw.sendPixelResponse(w)
	}
}

func (gw *Handle) beaconHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if writeKey, present := queryParams["writeKey"]; present && writeKey[0] != "" {

		// set basic auth header
		r.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// send req to webHandler
		gw.webHandler(w, r, reqType)
	} else {
		gw.logger.Infof("IP: %s -- %s -- Error while handling beacon request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path)
		http.Error(w, response.NoWriteKeyInQueryParams, http.StatusUnauthorized)
	}
}

// Robots prevents robotsHandler from crawling the gateway endpoints
func (*Handle) robotsHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("User-agent: * \nDisallow: / \n"))
}

func (gw *Handle) eventSchemaController(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !gw.conf.enableEventSchemasFeature {
			gw.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 400, %s", misc.GetIPFromReq(r), r.URL.Path, response.MakeResponse("EventSchemas feature is disabled")))
			http.Error(w, response.MakeResponse("EventSchemas feature is disabled"), 400)
			return
		}
		wrappedFunc(w, r)
	}
}

func WithContentType(contentType string, delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", contentType)
		delegate(w, r)
	}
}
