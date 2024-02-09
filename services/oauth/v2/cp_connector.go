package v2

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type ControlPlaneConnectorI interface {
	CpApiCall(cpReq *ControlPlaneRequestT) (int, string)
}

type ControlPlaneConnector struct {
	client *http.Client
	logger logger.Logger
}

func NewControlPlaneConnector(options ...func(*ControlPlaneConnector)) ControlPlaneConnectorI {
	cpConnector := &ControlPlaneConnector{
		client: &http.Client{
			Transport: http.DefaultTransport,
		},
	}
	for _, opt := range options {
		opt(cpConnector)
	}
	if cpConnector.logger == nil {
		cpConnector.logger = logger.NewLogger().Child("ControlPlaneConnector")
	}
	return cpConnector
}

func WithParentLogger(parentLogger logger.Logger) func(*ControlPlaneConnector) {
	return func(cpConn *ControlPlaneConnector) {
		cpConn.logger = parentLogger
	}
}

func WithCpClientTimeout(timeout time.Duration) func(*ControlPlaneConnector) {
	return func(h *ControlPlaneConnector) {
		h.client.Timeout = timeout
	}
}

func processResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = io.ReadAll(resp.Body)
		defer httputil.CloseResponse(resp)
		if ioUtilReadErr != nil {
			return http.StatusInternalServerError, ioUtilReadErr.Error()
		}
	}
	// Detecting content type of the respData
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	// If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}

func (cpConn *ControlPlaneConnector) CpApiCall(cpReq *ControlPlaneRequestT) (int, string) {
	cpStatTags := stats.Tags{
		"url":         cpReq.Url,
		"requestType": cpReq.RequestType,
		"destType":    cpReq.destName,
		"method":      cpReq.Method,
		"flowType":    string(cpReq.rudderFlowType),
	}

	var reqBody *bytes.Buffer
	var req *http.Request
	var err error
	if cpReq.Body != "" {
		reqBody = bytes.NewBufferString(cpReq.Body)
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, reqBody)
	} else {
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, http.NoBody)
	}
	if err != nil {
		cpConn.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, err)
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	// Authorisation setting
	req.SetBasicAuth(cpReq.basicAuthUser.BasicAuth())

	// Set content-type in order to send the body in request correctly
	if cpReq.ContentType != "" {
		req.Header.Set("Content-Type", cpReq.ContentType)
	}

	cpApiDoTimeStart := time.Now()
	res, doErr := cpConn.client.Do(req)
	stats.Default.NewTaggedStat("cp_request_latency", stats.TimerType, cpStatTags).SendTiming(time.Since(cpApiDoTimeStart))
	cpConn.logger.Debugf("[%s request] :: destination request sent\n", loggerNm)
	if doErr != nil {
		// Abort on receiving an error
		cpConn.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, doErr)
		if os.IsTimeout(doErr) {
			stats.Default.NewTaggedStat("cp_request_timeout", stats.CountType, cpStatTags)
		}
		return http.StatusBadRequest, doErr.Error()
	}
	defer httputil.CloseResponse(res)
	statusCode, resp := processResponse(res)
	return statusCode, resp
}
