//go:generate mockgen -destination=../mocks/router/mock_network.go -package mock_network github.com/rudderlabs/rudder-server/router NetHandle

package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/netutil"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var (
	contentTypeRegex = regexp.MustCompile(`^(text/[a-z0-9.-]+)|(application/([a-z0-9.-]+\+)?(json|xml))$`)
	ErrDenyPrivateIP = errors.New("access to private IPs is blocked")
)

// netHandle is the wrapper holding private variables
type netHandle struct {
	disableEgress         bool
	httpClient            sysUtils.HTTPClientI
	logger                logger.Logger
	blockPrivateIPsDryRun bool
	blockPrivateIPs       bool
	blockPrivateIPsCIDRs  netutil.CIDRs
	destType              string
}

// NetHandle interface
type NetHandle interface {
	SendPost(ctx context.Context, structData integrations.PostParametersT) *utils.SendPostResponse
}

// temp solution for handling complex query params
func handleQueryParam(param interface{}) string {
	switch p := param.(type) {
	case string:
		return p
	case map[string]interface{}:
		temp, err := jsonrs.Marshal(p)
		if err != nil {
			return fmt.Sprint(p)
		}

		jsonParam := string(temp)
		return jsonParam
	default:
		return fmt.Sprint(param)
	}
}

// SendPost takes the EventPayload of a transformed job, gets the necessary values from the payload and makes a call to destination to push the event to it
// this returns the statusCode, status and response body from the response of the destination call
func (network *netHandle) SendPost(ctx context.Context, structData integrations.PostParametersT) *utils.SendPostResponse {
	if network.disableEgress {
		return &utils.SendPostResponse{
			StatusCode:   200,
			ResponseBody: []byte("200: outgoing disabled"),
		}
	}

	client := network.httpClient
	postInfo := structData
	isRest := postInfo.Type == "REST"

	isMultipart := len(postInfo.Files) > 0

	// going forward we may want to support GraphQL and multipart requests
	// the files key in the response is specifically to handle the multipart use case
	// for type GraphQL may need to support more keys like expected response format etc.
	// in future it's expected that we will build on top of this response type
	// so, code addition should be done here instead of version bumping of response.
	if isRest && !isMultipart {
		requestMethod := postInfo.RequestMethod
		requestBody := postInfo.Body
		requestQueryParams := postInfo.QueryParams
		var bodyFormat string
		var bodyValue map[string]interface{}

		for format, value := range requestBody {
			bodyData, ok := value.(map[string]interface{})
			if !ok {
				stats.Default.NewTaggedStat("router_invalid_payload", stats.CountType, stats.Tags{
					"destType": network.destType,
				})
				return &utils.SendPostResponse{
					StatusCode:   500,
					ResponseBody: []byte("500 Invalid Router Payload: body value must be a map"),
				}
			}

			if len(bodyData) > 0 {
				bodyFormat = format
				bodyValue = bodyData
				break
			}
		}

		var payload io.Reader
		headers := map[string]string{"User-Agent": "RudderLabs"}
		// support for JSON and FORM body type
		if len(bodyValue) > 0 {
			switch bodyFormat {
			case "JSON":
				jsonValue, err := jsonrs.Marshal(bodyValue)
				if err != nil {
					panic(err)
				}
				payload = strings.NewReader(string(jsonValue))
			case "JSON_ARRAY":
				// support for JSON ARRAY
				jsonListStr, ok := bodyValue["batch"].(string)
				if !ok {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte("400 Unable to parse json list. Unexpected transformer response"),
					}
				}
				payload = strings.NewReader(jsonListStr)
			case "XML":
				strValue, ok := bodyValue["payload"].(string)
				if !ok {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte("400 Unable to construct xml payload. Unexpected transformer response"),
					}
				}
				payload = strings.NewReader(strValue)
			case "FORM":
				formValues := url.Values{}
				for key, val := range bodyValue {
					formValues.Set(key, fmt.Sprint(val)) // transformer ensures top level string values, still val.(string) would be restrictive
				}
				payload = strings.NewReader(formValues.Encode())
			case "GZIP":
				strValue, ok := bodyValue["payload"].(string)
				if !ok {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte("400 Unable to parse json list. Unexpected transformer response"),
					}
				}
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				defer func() { _ = zw.Close() }()

				if _, err := zw.Write([]byte(strValue)); err != nil {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte("400 Unable to compress data. Unexpected response"),
					}
				}

				if err := zw.Close(); err != nil {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte("400 Unable to flush compressed data. Unexpected response"),
					}
				}

				headers["Content-Encoding"] = "gzip"
				payload = &buf
			default:
				stats.Default.NewTaggedStat("router_invalid_payload", stats.CountType, stats.Tags{
					"destType": network.destType,
				})
				return &utils.SendPostResponse{
					StatusCode:   500,
					ResponseBody: []byte(fmt.Sprintf("500 Invalid Router Payload: body format must be a map found format %s", bodyFormat)),
				}
			}
		}

		req, err := http.NewRequestWithContext(ctx, requestMethod, postInfo.URL, payload)
		if err != nil {
			network.logger.Errorn("400 Unable to construct request",
				logger.NewStringField("requestMethod", requestMethod),
				logger.NewStringField("url", postInfo.URL),
				obskit.Error(err),
			)
			return &utils.SendPostResponse{
				StatusCode:   400,
				ResponseBody: []byte(fmt.Sprintf(`400 Unable to construct %q request for URL : %q`, requestMethod, postInfo.URL)),
			}
		}

		// add query params to the url
		// support of array type in params is handled if the
		// response from transformers are "," separated
		queryParams := req.URL.Query()
		for key, val := range requestQueryParams {
			formattedVal := handleQueryParam(val)
			queryParams.Add(key, formattedVal)
		}

		req.URL.RawQuery = queryParams.Encode()
		headerKV := postInfo.Headers
		for key, val := range headerKV {
			req.Header.Add(key, val.(string))
		}

		for key, val := range headers {
			req.Header.Add(key, val)
		}

		resp, err := client.Do(req)
		if errors.Is(err, ErrDenyPrivateIP) {
			return &utils.SendPostResponse{
				StatusCode:   403,
				ResponseBody: []byte("403: access to private IPs is blocked"),
			}
		}

		if err != nil {
			return &utils.SendPostResponse{
				StatusCode:   http.StatusGatewayTimeout,
				ResponseBody: []byte(fmt.Sprintf(`504 Unable to make %q request for URL : %q. Error: %v`, requestMethod, postInfo.URL, err)),
			}
		}

		defer func() { httputil.CloseResponse(resp) }()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return &utils.SendPostResponse{
				StatusCode:   resp.StatusCode,
				ResponseBody: []byte(fmt.Sprintf(`Failed to read response body for request for URL : %q. Error: %v`, postInfo.URL, err)),
			}
		}
		network.logger.Debugn("SendPost",
			logger.NewStringField("url", postInfo.URL),
			logger.NewStringField("reqProto", req.Proto),
			logger.NewStringField("respProto", resp.Proto),
			logger.NewIntField("respProtoMajor", int64(resp.ProtoMajor)),
			logger.NewIntField("respProtoMinor", int64(resp.ProtoMinor)),
		)

		var contentTypeHeader string
		if resp.Header != nil {
			contentTypeHeader = resp.Header.Get("Content-Type")
		}
		if contentTypeHeader == "" {
			// Detecting content type of the respBody
			contentTypeHeader = http.DetectContentType(respBody)
		}
		mediaType, _, _ := mime.ParseMediaType(contentTypeHeader)

		// If media type is not in some human-readable format (text,json,xml), override the response with an empty string
		// https://www.iana.org/assignments/media-types/media-types.xhtml
		isHumanReadable := contentTypeRegex.MatchString(mediaType)
		if !isHumanReadable {
			respBody = []byte("redacted due to unsupported content-type")
		}

		return &utils.SendPostResponse{
			StatusCode:          resp.StatusCode,
			ResponseBody:        respBody,
			ResponseContentType: contentTypeHeader,
		}
	}

	// returning 200 with a message in case of unsupported processing
	// so that we don't process again. can change this code to anything
	// to be not picked up by router again
	return &utils.SendPostResponse{
		StatusCode:   200,
		ResponseBody: []byte{},
	}
}

// Setup initializes the module
func (network *netHandle) Setup(config *config.Config, netClientTimeout time.Duration) error {
	network.logger.Infon("Network Handler Startup")

	network.blockPrivateIPsDryRun = getRouterConfigBool("dryRunMode", network.destType, false)
	network.blockPrivateIPs = getRouterConfigBool("blockPrivateIPs", network.destType, false)
	network.logger.Infon("blockPrivateIPsDryRun", logger.NewBoolField("blockPrivateIPsDryRun", network.blockPrivateIPsDryRun))
	network.logger.Infon("blockPrivateIPs", logger.NewBoolField("blockPrivateIPs", network.blockPrivateIPs))

	privateIPRanges, err := netutil.NewCidrRanges(strings.Split(config.GetString("privateIPRanges", netutil.DefaultPrivateIPRanges), ","))
	if err != nil {
		network.logger.Errorn("Error loading private IP ranges", obskit.Error(err))
		return err
	}
	network.blockPrivateIPsCIDRs = privateIPRanges

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		return fmt.Errorf("typecast of defaultRoundTripper to *http.Transport failed")
	}
	var defaultTransportCopy http.Transport
	misc.Copy(&defaultTransportCopy, defaultTransportPointer)

	originalDialContext := defaultTransportCopy.DialContext

	dialContext := func(ctx context.Context, networkType, address string) (net.Conn, error) {
		if network.blockPrivateIPsDryRun || network.blockPrivateIPs {
			if networkType == "tcp" || networkType == "tcp4" || networkType == "tcp6" {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				ips, err := net.LookupIP(host)
				if err != nil {
					return nil, err
				}
				for _, ip := range ips {
					if network.blockPrivateIPsCIDRs.Contains(ip) {
						// In dry run mode, just log and allow the connection
						if network.blockPrivateIPsDryRun {
							network.logger.Warnn("Connection to private ip detected in dry run mode", logger.NewStringField("ip", ip.String()))
							return originalDialContext(ctx, networkType, address)
						}
						// In block mode, reject the connection
						if network.blockPrivateIPs {
							return nil, ErrDenyPrivateIP
						}
					}
				}
			}
		}
		return originalDialContext(ctx, networkType, address)
	}

	defaultTransportCopy.DialContext = dialContext

	forceHTTP1 := getRouterConfigBool("forceHTTP1", network.destType, false)
	network.logger.Infon("forceHTTP1", logger.NewBoolField("forceHTTP1", forceHTTP1))
	if forceHTTP1 {
		network.logger.Infon("Forcing HTTP1 connection", logger.NewStringField("destType", network.destType))
		defaultTransportCopy.ForceAttemptHTTP2 = false
		var tlsClientConfig tls.Config
		if defaultTransportCopy.TLSClientConfig != nil {
			misc.Copy(&tlsClientConfig, defaultTransportCopy.TLSClientConfig)
		}
		tlsClientConfig.NextProtos = []string{"http/1.1"}
		defaultTransportCopy.TLSClientConfig = &tlsClientConfig
		network.logger.Infon(network.destType+" protos",
			logger.NewStringField("tlsNextProtos", strings.Join(tlsClientConfig.NextProtos, ",")),
		)
	}

	defaultTransportCopy.MaxIdleConns = getHierarchicalRouterConfigInt(network.destType, 64, "httpMaxIdleConns", "noOfWorkers")
	defaultTransportCopy.MaxIdleConnsPerHost = getHierarchicalRouterConfigInt(network.destType, 64, "httpMaxIdleConnsPerHost", "noOfWorkers")
	network.logger.Infon(network.destType,
		logger.NewIntField("maxIdleConns", int64(defaultTransportCopy.MaxIdleConns)),
		logger.NewIntField("maxIdleConnsPerHost", int64(defaultTransportCopy.MaxIdleConnsPerHost)),
		logger.NewDurationField("timeout", netClientTimeout),
	)
	network.httpClient = &http.Client{Transport: &defaultTransportCopy, Timeout: netClientTimeout}
	return nil
}
