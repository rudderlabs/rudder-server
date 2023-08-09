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

func (gateway *Handle) pixelWebHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.pixelWebRequestHandler(gateway.rrh, w, r, reqType)
}

func (gateway *Handle) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.webRequestHandler(gateway.rrh, w, r, reqType)
}

func (gateway *Handle) webRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	webReqHandlerTime := gateway.stats.NewTaggedStat("gateway.web_req_handler_time", stats.TimerType, stats.Tags{"reqType": reqType})
	webReqHandlerStartTime := time.Now()
	defer webReqHandlerTime.Since(webReqHandlerStartTime)

	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gateway.logger.Infof("IP: %s -- %s -- Response: %d, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetErrorStatusCode(errorMessage), errorMessage)
			http.Error(w, response.GetStatus(errorMessage), response.GetErrorStatusCode(errorMessage))
			return
		}
	}()
	payload, writeKey, err := gateway.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(&w, r, reqType, payload, writeKey)
	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.TrackRequestMetrics(errorMessage)
	if errorMessage != "" {
		return
	}
	gateway.logger.Debugf("IP: %s -- %s -- Response: 200, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetStatus(response.Ok))

	httpWriteTime := gateway.stats.NewTaggedStat("gateway.http_write_time", stats.TimerType, stats.Tags{"reqType": reqType})
	httpWriteStartTime := time.Now()
	_, _ = w.Write([]byte(response.GetStatus(response.Ok)))
	httpWriteTime.Since(httpWriteStartTime)
}

func (gateway *Handle) pixelWebRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.sendPixelResponse(w)
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gateway.logger.Infow("Error while handling request",
				"ip", misc.GetIPFromReq(r),
				"path", r.URL.Path,
				"error", errorMessage)
		}
	}()
	payload, writeKey, err := gateway.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(&w, r, reqType, payload, writeKey)

	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.TrackRequestMetrics(errorMessage)
}

func (gateway *Handle) sendPixelResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "image/gif")
	_, err := w.Write([]byte(response.GetPixelResponse()))
	if err != nil {
		gateway.logger.Warnf("Error while sending pixel response: %v", err)
		return
	}
}

func (gateway *Handle) webImportHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webRequestHandler(gateway.irh, w, r, "import")
}

func (gateway *Handle) webAudienceListHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "audiencelist")
}

func (gateway *Handle) webExtractHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "extract")
}

func (gateway *Handle) webBatchHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "batch")
}

func (gateway *Handle) webIdentifyHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "identify")
}

func (gateway *Handle) webTrackHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "track")
}

func (gateway *Handle) webPageHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "page")
}

func (gateway *Handle) webScreenHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "screen")
}

func (gateway *Handle) webAliasHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "alias")
}

func (gateway *Handle) webMergeHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "merge")
}

func (gateway *Handle) webGroupHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "group")
}

func (gateway *Handle) pixelPageHandler(w http.ResponseWriter, r *http.Request) {
	gateway.pixelHandler(w, r, "page")
}

func (gateway *Handle) pixelTrackHandler(w http.ResponseWriter, r *http.Request) {
	gateway.pixelHandler(w, r, "track")
}

func (gateway *Handle) beaconBatchHandler(w http.ResponseWriter, r *http.Request) {
	gateway.beaconHandler(w, r, "batch")
}

func (gateway *Handle) pixelHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if queryParams["writeKey"] != nil {
		writeKey := queryParams["writeKey"]
		// make a new request
		req, err := http.NewRequest(http.MethodPost, "", http.NoBody)
		if err != nil {
			gateway.sendPixelResponse(w)
			return
		}
		// set basic auth header
		req.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// set X-Forwarded-For header
		req.Header.Add("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))

		// convert the pixel request(r) to a web request(req)
		err = gateway.setWebPayload(req, queryParams, reqType)
		if err == nil {
			gateway.pixelWebHandler(w, req, reqType)
		} else {
			gateway.sendPixelResponse(w)
		}
	} else {
		gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Error while handling request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path))
		gateway.sendPixelResponse(w)
	}
}

func (gateway *Handle) beaconHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if writeKey, present := queryParams["writeKey"]; present && writeKey[0] != "" {

		// set basic auth header
		r.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// send req to webHandler
		gateway.webHandler(w, r, reqType)
	} else {
		gateway.logger.Infof("IP: %s -- %s -- Error while handling beacon request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path)
		http.Error(w, response.NoWriteKeyInQueryParams, http.StatusUnauthorized)
	}
}

// Robots prevents robotsHandler from crawling the gateway endpoints
func (*Handle) robotsHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("User-agent: * \nDisallow: / \n"))
}

func (gateway *Handle) eventSchemaController(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !gateway.conf.enableEventSchemasFeature {
			gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 400, %s", misc.GetIPFromReq(r), r.URL.Path, response.MakeResponse("EventSchemas feature is disabled")))
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
