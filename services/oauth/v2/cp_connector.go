package v2

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type ControlPlaneConnectorI interface {
	CpApiCall(cpReq *ControlPlaneRequestT) (int, string)
}
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}
type ControlPlaneConnector struct {
	Client  HttpClient
	Logger  logger.Logger
	timeOut time.Duration
}

func NewControlPlaneConnector(options ...func(*ControlPlaneConnector)) ControlPlaneConnectorI {
	cpConnector := &ControlPlaneConnector{}
	for _, opt := range options {
		opt(cpConnector)
	}
	httpClient := &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   cpConnector.timeOut,
	}
	cpConnector.Client = httpClient
	if cpConnector.Logger == nil {
		cpConnector.Logger = logger.NewLogger().Child("ControlPlaneConnector")
	}
	return cpConnector
}

/*
WithParentLogger is a functional option to set the parent logger for the ControlPlaneConnector
*/
func WithParentLogger(parentLogger logger.Logger) func(*ControlPlaneConnector) {
	return func(cpConn *ControlPlaneConnector) {
		cpConn.Logger = parentLogger
	}
}

/*
WithCpClientTimeout is a functional option to set the timeout for the ControlPlaneConnector
*/
func WithCpClientTimeout(timeout time.Duration) func(*ControlPlaneConnector) {
	return func(h *ControlPlaneConnector) {
		h.timeOut = timeout
	}
}

/*
processResponse is a helper function to process the response from the control plane
*/
func processResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = io.ReadAll(resp.Body)
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

/*
CpApiCall is a function to make a call to the control plane, handle the response and return the status code and response body
*/
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
		cpConn.Logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, err)
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	// Authorisation setting
	req.SetBasicAuth(cpReq.BasicAuthUser.BasicAuth())

	// Set content-type in order to send the body in request correctly
	if cpReq.ContentType != "" {
		req.Header.Set("Content-Type", cpReq.ContentType)
	}

	cpApiDoTimeStart := time.Now()
	res, doErr := cpConn.Client.Do(req)
	defer func() { httputil.CloseResponse(res) }()
	stats.Default.NewTaggedStat("cp_request_latency", stats.TimerType, cpStatTags).SendTiming(time.Since(cpApiDoTimeStart))
	cpConn.Logger.Debugf("[%s request] :: destination request sent\n", loggerNm)
	if doErr != nil {
		// Abort on receiving an error
		cpConn.Logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, doErr)
		if os.IsTimeout(doErr) {
			stats.Default.NewTaggedStat("cp_request_timeout", stats.CountType, cpStatTags).Count(1)
		}
		if ok := errors.Is(doErr, syscall.ECONNRESET); ok {
			stats.Default.NewTaggedStat("cp_request_conn_reset", stats.CountType, cpStatTags).Count(1)
		}
		if _, ok := doErr.(net.Error); ok {
			resp := `{
				"error": "network_error",
				"message": 	"control plane service is not available or failed due to timeout."
			}`
			return http.StatusServiceUnavailable, resp
		}
		return http.StatusBadRequest, doErr.Error()
	}
	statusCode, resp := processResponse(res)
	return statusCode, resp
}
