package v2

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type ControlPlaneConnector interface {
	CpApiCall(cpReq *ControlPlaneRequest) (int, string)
}
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}
type controlPlaneConnector struct {
	client  HttpClient
	logger  logger.Logger
	timeOut time.Duration
}

func NewControlPlaneConnector(options ...func(*controlPlaneConnector)) ControlPlaneConnector {
	cpConnector := &controlPlaneConnector{}

	for _, opt := range options {
		opt(cpConnector)
	}

	if cpConnector.client == nil {
		cpConnector.client = &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   cpConnector.timeOut,
		}
	}
	if cpConnector.logger == nil {
		cpConnector.logger = logger.NewLogger().Child("ControlPlaneConnector")
	}
	return cpConnector
}

/*
WithClient is a functional option to set the client for the ControlPlaneConnector
*/
func WithClient(client HttpClient) func(*controlPlaneConnector) {
	return func(cpConn *controlPlaneConnector) {
		cpConn.client = client
	}
}

/*
WithParentLogger is a functional option to set the parent logger for the ControlPlaneConnector
*/
func WithParentLogger(parentLogger logger.Logger) func(*controlPlaneConnector) {
	return func(cpConn *controlPlaneConnector) {
		cpConn.logger = parentLogger
	}
}

/*
WithCpClientTimeout is a functional option to set the timeout for the ControlPlaneConnector
*/
func WithCpClientTimeout(timeout time.Duration) func(*controlPlaneConnector) {
	return func(h *controlPlaneConnector) {
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
func (c *controlPlaneConnector) CpApiCall(cpReq *ControlPlaneRequest) (int, string) {
	cpStatTags := stats.Tags{
		"url":          cpReq.Url,
		"requestType":  cpReq.RequestType,
		"destType":     cpReq.destName,
		"method":       cpReq.Method,
		"flowType":     string(cpReq.rudderFlowType),
		"oauthVersion": "v2",
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
		c.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, err)
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
	res, doErr := c.client.Do(req)
	defer func() { httputil.CloseResponse(res) }()
	stats.Default.NewTaggedStat("cp_request_latency", stats.TimerType, cpStatTags).SendTiming(time.Since(cpApiDoTimeStart))
	c.logger.Debugf("[%s request] :: destination request sent\n", loggerNm)
	if doErr != nil {
		// Abort on receiving an error
		c.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, doErr)

		errorType := GetErrorType(doErr)
		cpStatTags["errorType"] = errorType
		stats.Default.NewTaggedStat("oauth_v2_cp_request_error", stats.CountType, cpStatTags).Count(1)

		resp := doErr.Error()
		if errorType != "none" {
			resp = fmt.Sprintf(`{
				%q: %q,
				"message": 	%q
			}`, ErrorType, errorType, doErr.Error())
		}
		return http.StatusInternalServerError, resp
	}
	statusCode, resp := processResponse(res)
	return statusCode, resp
}
