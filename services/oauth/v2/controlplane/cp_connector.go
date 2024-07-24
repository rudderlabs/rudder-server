package controlplane

//go:generate mockgen -destination=../../../../mocks/services/oauthV2/mock_cp_connector.go -package=mock_oauthV2 github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane Connector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

var (
	errTypMap = map[syscall.Errno]string{
		syscall.ECONNRESET:   "econnreset",
		syscall.ECONNREFUSED: "econnrefused",
		syscall.ECONNABORTED: "econnaborted",
		syscall.ECANCELED:    "ecanceled",
	}
	contentTypePattern = regexp.MustCompile(`text|application/json|application/xml`)
)

type Connector interface {
	CpApiCall(cpReq *Request) (int, string)
}

type connector struct {
	client  HttpClient
	logger  logger.Logger
	timeout time.Duration
	stats   stats.Stats
}

func NewConnector(conf *config.Config, options ...func(*connector)) Connector {
	cpConnector := &connector{
		client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   conf.GetDuration("HttpClient.oauth.timeout", 30, time.Second),
		},
	}

	for _, opt := range options {
		opt(cpConnector)
	}
	if cpConnector.logger == nil {
		cpConnector.logger = logger.NewLogger()
	}
	cpConnector.logger = cpConnector.logger.Child("ControlPlaneConnector")
	return cpConnector
}

func WithStats(stats stats.Stats) func(*connector) {
	return func(c *connector) {
		c.stats = stats
	}
}

// WithClient is a functional option to set the client for the Connector
func WithClient(client HttpClient) func(*connector) {
	return func(c *connector) {
		c.client = client
	}
}

// WithLogger is a functional option to set the parent logger for the Connector
func WithLogger(parentLogger logger.Logger) func(*connector) {
	return func(c *connector) {
		c.logger = parentLogger
	}
}

// WithCpClientTimeout is a functional option to set the timeout for the Connector
func WithCpClientTimeout(timeout time.Duration) func(*connector) {
	return func(c *connector) {
		c.timeout = timeout
	}
}

// processResponse is a helper function to process the response from the control plane
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
	if !contentTypePattern.MatchString(contentTypeHeader) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}

// CpApiCall is a function to make a call to the control plane, handle the response and return the status code and response body
func (c *connector) CpApiCall(cpReq *Request) (int, string) {
	cpStatTags := stats.Tags{
		"url":          cpReq.URL,
		"requestType":  cpReq.RequestType,
		"destType":     cpReq.DestName,
		"method":       cpReq.Method,
		"flowType":     string(cpReq.rudderFlowType),
		"oauthVersion": "v2",
	}

	var reqBody *bytes.Buffer
	var req *http.Request
	var err error
	if cpReq.Body != "" {
		reqBody = bytes.NewBufferString(cpReq.Body)
		req, err = http.NewRequest(cpReq.Method, cpReq.URL, reqBody)
	} else {
		req, err = http.NewRequest(cpReq.Method, cpReq.URL, http.NoBody)
	}
	if err != nil {
		c.logger.Errorn("[request] :: destination request failed",
			logger.NewErrorField(err))
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
	c.stats.NewTaggedStat("oauth_v2_cp_request_latency", stats.TimerType, cpStatTags).SendTiming(time.Since(cpApiDoTimeStart))
	c.logger.Debugn("[request] :: destination request sent")
	if doErr != nil {
		// Abort on receiving an error
		c.logger.Errorn("[request] :: destination request failed",
			logger.NewErrorField(doErr))
		errorType := GetErrorType(doErr)
		cpStatTags["error"] = errorType
		c.stats.NewTaggedStat("oauth_v2_cp_requests", stats.CountType, cpStatTags).Count(1)

		resp := doErr.Error()
		if errorType != common.None {
			resp = fmt.Sprintf(`{
				%q: %q,
				"message": 	%q
			}`, common.ErrorType, errorType, doErr.Error())
		}
		return http.StatusInternalServerError, resp
	}
	cpStatTags["statusCode"] = strconv.Itoa(res.StatusCode)
	cpStatTags["error"] = "" // got some valid response from cp
	c.stats.NewTaggedStat("oauth_v2_cp_requests", stats.CountType, cpStatTags).Count(1)
	statusCode, resp := processResponse(res)
	return statusCode, resp
}

func GetErrorType(err error) string {
	if os.IsTimeout(err) {
		return common.TimeOutError
	}
	for errno, errTyp := range errTypMap {
		if ok := errors.Is(err, errno); ok {
			return errTyp
		}
	}
	var e net.Error
	if errors.As(err, &e) {
		return common.NetworkError
	}
	return common.None
}
