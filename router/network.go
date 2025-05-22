//go:generate mockgen -destination=../mocks/router/mock_network.go -package mock_network github.com/rudderlabs/rudder-server/router NetHandle

package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
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
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var contentTypeRegex = regexp.MustCompile(`^(text/[a-z0-9.-]+)|(application/([a-z0-9.-]+\+)?(json|xml))$`)

// Default private IP ranges in CIDR notation
const defaultPrivateIPRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,127.0.0.0/8,169.254.0.0/16,fc00::/7,fe80::/10"

// IPRange represents a range of IP addresses
type IPRange struct {
	start net.IP
	end   net.IP
}

// parseCIDRToRange converts a CIDR block to an IP range
func parseCIDRToRange(cidr string) (IPRange, error) {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return IPRange{}, fmt.Errorf("invalid CIDR block %s: %w", cidr, err)
	}

	// Get the network address (start of range)
	start := ipnet.IP

	// Calculate the broadcast address (end of range)
	mask := ipnet.Mask
	end := make(net.IP, len(start))
	for i := 0; i < len(start); i++ {
		end[i] = start[i] | ^mask[i]
	}

	return IPRange{start: start, end: end}, nil
}

// loadPrivateIPRanges loads private IP ranges from environment variable or uses defaults
func loadPrivateIPRanges(config *config.Config) []IPRange {
	cidrBlocks := config.GetString("Router.privateIPRanges", defaultPrivateIPRanges)
	if cidrBlocks == "" {
		cidrBlocks = defaultPrivateIPRanges
	}

	var ranges []IPRange
	for _, cidr := range strings.Split(cidrBlocks, ",") {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}

		ipRange, err := parseCIDRToRange(cidr)
		if err != nil {
			// Log error but continue with other ranges
			fmt.Printf("Error parsing CIDR block %s: %v\n", cidr, err)
			continue
		}
		ranges = append(ranges, ipRange)
	}

	return ranges
}

// netHandle is the wrapper holding private variables
type netHandle struct {
	disableEgress   bool
	httpClient      sysUtils.HTTPClientI
	logger          logger.Logger
	dryRunMode      bool
	blockPrivateIPs bool
	privateIPRanges []IPRange
}

// NetHandle interface
type NetHandle interface {
	SendPost(ctx context.Context, structData integrations.PostParametersT) *utils.SendPostResponse
}

// isPrivateIP checks if the given IP is in a private range
func (network *netHandle) isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}

	for _, r := range network.privateIPRanges {
		if bytes.Compare(ip, r.start) >= 0 && bytes.Compare(ip, r.end) <= 0 {
			return true
		}
	}
	return false
}

// validateURL checks if the URL resolves to a private IP
func (network *netHandle) validateURL(urlStr string) (bool, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return false, fmt.Errorf("invalid URL: %w", err)
	}

	host := parsedURL.Hostname()
	ips, err := net.LookupIP(host)
	if err != nil {
		return false, fmt.Errorf("failed to resolve host: %w", err)
	}

	for _, ip := range ips {
		if network.isPrivateIP(ip) {
			return true, nil
		}
	}
	return false, nil
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

// validateURLAndHandlePrivateIP checks if the URL resolves to a private IP and handles it based on mode
func (network *netHandle) validateURLAndHandlePrivateIP(urlStr string) (bool, error) {
	isPrivate, err := network.validateURL(urlStr)
	if err != nil {
		return false, fmt.Errorf("URL validation failed: %w", err)
	}

	if isPrivate {
		if network.dryRunMode {
			network.logger.Warnf("URL %s resolves to private IP in dry run mode", urlStr)
			return false, nil
		}
		if network.blockPrivateIPs {
			return true, fmt.Errorf("access to private IPs is blocked")
		}
	}
	return false, nil
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

	isBlocked, err := network.validateURLAndHandlePrivateIP(structData.URL)
	if err != nil {
		return &utils.SendPostResponse{
			StatusCode:   403,
			ResponseBody: []byte(fmt.Sprintf("403: %v", err)),
		}
	}
	if isBlocked {
		return &utils.SendPostResponse{
			StatusCode:   403,
			ResponseBody: []byte("403: Access to private IPs is blocked"),
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
		for k, v := range requestBody {
			if len(v.(map[string]interface{})) > 0 {
				bodyFormat = k
				bodyValue = v.(map[string]interface{})
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
				panic(fmt.Errorf("bodyFormat: %s is not supported", bodyFormat))
			}
		}

		req, err := http.NewRequestWithContext(ctx, requestMethod, postInfo.URL, payload)
		if err != nil {
			network.logger.Error(fmt.Sprintf(`400 Unable to construct %q request for URL : %q`, requestMethod, postInfo.URL))
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
		network.logger.Debug(postInfo.URL, " : ", req.Proto, " : ", resp.Proto, resp.ProtoMajor, resp.ProtoMinor, resp.ProtoAtLeast)

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
func (network *netHandle) Setup(config *config.Config, destType string, netClientTimeout time.Duration) {
	network.logger.Info("Network Handler Startup")

	network.privateIPRanges = loadPrivateIPRanges(config)
	network.logger.Info("Loaded private IP ranges:", network.privateIPRanges)

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Errorf("typecast of defaultRoundTripper to *http.Transport failed"))
	}
	var defaultTransportCopy http.Transport
	misc.Copy(&defaultTransportCopy, defaultTransportPointer)

	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	dialContext := func(ctx context.Context, networkType, address string) (net.Conn, error) {
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
				if network.isPrivateIP(ip) {
					// In dry run mode, just log and allow the connection
					if network.dryRunMode {
						network.logger.Warnf("Connection to private IP %s detected in dry run mode", ip)
						return dialer.DialContext(ctx, networkType, address)
					}
					// In block mode, reject the connection
					if network.blockPrivateIPs {
						return nil, fmt.Errorf("access to private IP %s is not allowed", ip)
					}
				}
			}
		}
		return dialer.DialContext(ctx, networkType, address)
	}

	defaultTransportCopy.DialContext = dialContext

	forceHTTP1 := getRouterConfigBool("forceHTTP1", destType, false)
	network.logger.Info("forceHTTP1: ", forceHTTP1)
	if forceHTTP1 {
		network.logger.Info("Forcing HTTP1 connection for ", destType)
		defaultTransportCopy.ForceAttemptHTTP2 = false
		var tlsClientConfig tls.Config
		if defaultTransportCopy.TLSClientConfig != nil {
			misc.Copy(&tlsClientConfig, defaultTransportCopy.TLSClientConfig)
		}
		tlsClientConfig.NextProtos = []string{"http/1.1"}
		defaultTransportCopy.TLSClientConfig = &tlsClientConfig
		network.logger.Info(destType, defaultTransportCopy.TLSClientConfig.NextProtos)
	}

	network.dryRunMode = getRouterConfigBool("dryRunMode", destType, false)
	network.blockPrivateIPs = getRouterConfigBool("blockPrivateIPs", destType, false)
	network.logger.Info("dryRunMode: ", network.dryRunMode)
	network.logger.Info("blockPrivateIPs: ", network.blockPrivateIPs)

	defaultTransportCopy.MaxIdleConns = getHierarchicalRouterConfigInt(destType, 64, "httpMaxIdleConns", "noOfWorkers")
	defaultTransportCopy.MaxIdleConnsPerHost = getHierarchicalRouterConfigInt(destType, 64, "httpMaxIdleConnsPerHost", "noOfWorkers")
	network.logger.Info(destType, ":   defaultTransportCopy.MaxIdleConns: ", defaultTransportCopy.MaxIdleConns)
	network.logger.Info("defaultTransportCopy.MaxIdleConnsPerHost: ", defaultTransportCopy.MaxIdleConnsPerHost)
	network.logger.Info("netClientTimeout: ", netClientTimeout)
	network.httpClient = &http.Client{Transport: &defaultTransportCopy, Timeout: netClientTimeout}
}
