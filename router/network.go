//go:generate mockgen -destination=../mocks/router/mock_network.go -package mock_network github.com/rudderlabs/rudder-server/router NetHandleI

package router

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var contentTypeRegex *regexp.Regexp

func init() {
	contentTypeRegex = regexp.MustCompile(`^(text/[a-z0-9.-]+)|(application/([a-z0-9.-]+\+)?(json|xml))$`)
}

//NetHandleT is the wrapper holding private variables
type NetHandleT struct {
	httpClient sysUtils.HTTPClientI
	logger     logger.LoggerI
}

//Network interface
type NetHandleI interface {
	SendPost(ctx context.Context, structData integrations.PostParametersT, destName string, jobId int64, transformerProxy bool) *utils.SendPostResponse
}

//temp solution for handling complex query params
func handleQueryParam(param interface{}) string {
	switch p := param.(type) {
	case string:
		return p
	case map[string]interface{}:
		temp, err := json.Marshal(p)
		if err != nil {
			return fmt.Sprint(p)
		}

		jsonParam := string(temp)
		return jsonParam
	default:
		return fmt.Sprint(param)
	}
}

//SendPost takes the EventPayload of a transformed job, gets the necessary values from the payload and makes a call to destination to push the event to it
//this returns the statusCode, status and response body from the response of the destination call
func (network *NetHandleT) SendPost(ctx context.Context, structData integrations.PostParametersT, destName string, jobId int64, transformerProxy bool) *utils.SendPostResponse {
	if disableEgress {
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
	// the files key in the response is specifically to handle the multipart usecase
	// for type GraphQL may need to support more keys like expected response format etc
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
		// support for JSON and FORM body type
		if len(bodyValue) > 0 {
			switch bodyFormat {
			case "JSON":
				jsonValue, err := json.Marshal(bodyValue)
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
			default:
				panic(fmt.Errorf("bodyFormat: %s is not supported", bodyFormat))
			}
		}

		req, err := http.NewRequestWithContext(ctx, requestMethod, postInfo.URL, payload)
		if err != nil {
			network.logger.Error(fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL))
			return &utils.SendPostResponse{
				StatusCode:   400,
				ResponseBody: []byte(fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL)),
			}
		}

		// add queryparams to the url
		// support of array type in params is handled if the
		// response from transformers are "," seperated
		queryParams := req.URL.Query()
		for key, val := range requestQueryParams {

			// list := strings.Split(valString, ",")
			// for _, listItem := range list {
			// 	queryParams.Add(key, fmt.Sprint(listItem))
			// }
			formattedVal := handleQueryParam(val)
			queryParams.Add(key, formattedVal)
		}

		req.URL.RawQuery = queryParams.Encode()
		headerKV := postInfo.Headers
		for key, val := range headerKV {
			req.Header[key] = []string{val.(string)}
		}

		req.Header["User-Agent"] = []string{"RudderLabs"}

		// We will change this to `transformerProxy`
		if transformerProxy {
			rtPayload := transformer.RouterDelPayload{
				Method:   requestMethod,
				Endpoint: postInfo.URL,
			}

			// payload will be "nil" when params or files is set during destination transformation
			if payload != nil {
				var payloadBytes []byte
				payloadBytes, err = ioutil.ReadAll(payload)
				if err != nil {
					return &utils.SendPostResponse{
						StatusCode:   400,
						ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} 400 Unable to read payload "%[3]v" request for URL : "%[4]v"`, destName, jobId, requestMethod, postInfo.URL)),
					}
				}

				tempJson := make(map[string]interface{})
				err := json.Unmarshal(payloadBytes, &tempJson)
				if err != nil {
					// For FORM and JSON body formats, the expected value is JSON type
					// Hence, declare it an error if we couldn't unmarshal
					// Note: For FORM, the final payload is a string
					if bodyFormat == "JSON" {
						return &utils.SendPostResponse{
							StatusCode:   400,
							ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} 400 Unable to unmarshal payload "%[3]v" request for URL : "%[4]v"`, destName, jobId, requestMethod, postInfo.URL)),
						}
					} else {
						rtPayload.Data = string(payloadBytes)
					}
				} else {
					rtPayload.Data = tempJson
				}
			}

			// This is being done to facilitate compatible comparison
			// As map[string][]string is the data-type for headers in golang
			// But headers is an object in Javascript, hence we need to level the plane for effective comparison
			headersMap := make(map[string]string)
			for k, v := range req.Header {
				headersMap[k] = string(v[0])
			}
			rtPayload.Headers = headersMap

			// This is being done to facilitate compatible comparison
			// As map[string][]string is the data-type for url.Values in golang
			// But params is an object in Javascript, hence we need to level the plane for effective comparison
			// queryParamsMap := make(map[string]string)
			// for k, v := range req.URL.Query() {
			// 	queryParamsMap[strings.ToLower(k)] = string(v[0])
			// }

			/**
			Sending params as the original "params" is required
			For example:
			When a URL is set as http://endpoint.com?someKey=someVal & "params" is not set
			If we send queryParamsMap to transformer (/proxyTest)
			We'd be including someKey in "params" object of the routerPayload but infact params is still "{}"
			Due to which there will be a mis-match
			Hence we should just send what is sent as part of "params" here(requestQueryParams)
			**/
			rtPayload.Params = requestQueryParams

			proxyReqBody := transformer.ProxyTestRequestPayload{
				RouterDeliveryPayload: rtPayload,
				ProxyRequestPayload:   postInfo,
			}

			proxyPayload, marshErr := json.Marshal(proxyReqBody)
			if marshErr != nil {
				network.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} ProxyReqBody Marshal Error, with %[3]v`, destName, jobId, marshErr.Error())
				return &utils.SendPostResponse{
					StatusCode:   400,
					ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} 400 Unable to read payload "%[3]v" request for URL : "%[4]v" with error: %[5]v`, destName, jobId, requestMethod, postInfo.URL, marshErr.Error())),
				}
			}
			// ProxyTest Url
			url := getProxyTestURL(destName)
			// New Request for ProxyTest endpoint
			proxyTestReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(proxyPayload))
			if err != nil {
				network.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} NewRequestWithContext Failed for %[1]v, with %[3]v`, destName, jobId, err.Error())
				return &utils.SendPostResponse{
					StatusCode:   400,
					ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} 400 Unable to read payload "%[3]v" request for URL : "%[4]v" with error: %[5]v`, destName, jobId, requestMethod, postInfo.URL, err.Error())),
				}
			}
			proxyTestReq.Header.Set("Content-Type", "application/json")

			var proxyResp *http.Response
			proxyResp, err = client.Do(proxyTestReq)
			if err != nil {
				network.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} Error in response or request to proxyTest with Error: %[3]v`, destName, jobId, err.Error())
				return &utils.SendPostResponse{
					StatusCode:   400,
					ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} 400 Unable to read payload "%[3]v" request for URL : "%[4]v" with error: %[5]v`, destName, jobId, requestMethod, postInfo.URL, err.Error())),
				}
			}

			defer proxyResp.Body.Close()

			respBody, err := io.ReadAll(proxyResp.Body)
			if err != nil {
				return &utils.SendPostResponse{
					StatusCode:   proxyResp.StatusCode,
					ResponseBody: []byte(fmt.Sprintf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} Failed to read response body for request for URL : "%[3]v"`, destName, jobId, postInfo.URL)),
				}
			}

			return &utils.SendPostResponse{
				StatusCode:          proxyResp.StatusCode,
				ResponseBody:        respBody,
				ResponseContentType: proxyResp.Header.Get("Content-Type"),
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			return &utils.SendPostResponse{
				StatusCode:   http.StatusGatewayTimeout,
				ResponseBody: []byte(fmt.Sprintf(`504 Unable to make "%s" request for URL : "%s"`, requestMethod, postInfo.URL)),
			}
		}

		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return &utils.SendPostResponse{
				StatusCode:   resp.StatusCode,
				ResponseBody: []byte(fmt.Sprintf(`Failed to read response body for request for URL : "%s"`, postInfo.URL)),
			}
		}
		network.logger.Debug(postInfo.URL, " : ", req.Proto, " : ", resp.Proto, resp.ProtoMajor, resp.ProtoMinor, resp.ProtoAtLeast)

		var contentTypeHeader string
		if resp != nil && resp.Header != nil {
			contentTypeHeader = resp.Header.Get("Content-Type")
		}
		if contentTypeHeader == "" {
			//Detecting content type of the respBody
			contentTypeHeader = http.DetectContentType(respBody)
		}
		mediaType, _, _ := mime.ParseMediaType(contentTypeHeader)

		// If media type is not in some human readable format (text,json,xml), override the response with an empty string
		// https://www.iana.org/assignments/media-types/media-types.xhtml
		isHumanReadable := contentTypeRegex.MatchString(mediaType)
		if !isHumanReadable {
			respBody = []byte("redacted due to unsupported content-type")
		}

		if err != nil {
			network.logger.Error("Errored when sending request to the server", err)
			return &utils.SendPostResponse{
				StatusCode:          http.StatusGatewayTimeout,
				ResponseBody:        respBody,
				ResponseContentType: contentTypeHeader,
			}
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

//Setup initializes the module
func (network *NetHandleT) Setup(destID string, netClientTimeout time.Duration) {
	network.logger.Info("Network Handler Startup")
	//Reference http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Errorf("typecast of defaultRoundTripper to *http.Transport failed")) //TODO: Handle error
	}
	var defaultTransportCopy http.Transport
	//Not safe to copy DefaultTransport
	//https://groups.google.com/forum/#!topic/golang-nuts/JmpHoAd76aU
	//Solved in go1.8 https://github.com/golang/go/issues/26013
	misc.Copy(&defaultTransportCopy, defaultTransportPointer)
	network.logger.Info("forceHTTP1: ", getRouterConfigBool("forceHTTP1", destID, false))
	if getRouterConfigBool("forceHTTP1", destID, false) {
		network.logger.Info("Forcing HTTP1 connection for ", destID)
		defaultTransportCopy.ForceAttemptHTTP2 = false
		var tlsClientConfig tls.Config
		if defaultTransportCopy.TLSClientConfig != nil {
			misc.Copy(&tlsClientConfig, defaultTransportCopy.TLSClientConfig)
		}
		tlsClientConfig.NextProtos = []string{"http/1.1"}
		defaultTransportCopy.TLSClientConfig = &tlsClientConfig
		network.logger.Info(destID, defaultTransportCopy.TLSClientConfig.NextProtos)
	}
	defaultTransportCopy.MaxIdleConns = getRouterConfigInt("httpMaxIdleConns", destID, 100)
	defaultTransportCopy.MaxIdleConnsPerHost = getRouterConfigInt("httpMaxIdleConnsPerHost", destID, 100)
	network.logger.Info(destID, ":   defaultTransportCopy.MaxIdleConns: ", defaultTransportCopy.MaxIdleConns)
	network.logger.Info("defaultTransportCopy.MaxIdleConnsPerHost: ", defaultTransportCopy.MaxIdleConnsPerHost)
	network.logger.Info("netClientTimeout: ", netClientTimeout)
	network.httpClient = &http.Client{Transport: &defaultTransportCopy, Timeout: netClientTimeout}
}

func getProxyTestURL(destName string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/destinations/" + strings.ToLower(destName) + "/proxyTest"
}
