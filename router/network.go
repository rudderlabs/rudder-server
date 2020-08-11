package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//NetHandleT is the wrapper holding private variables
type NetHandleT struct {
	httpClient *http.Client
}

//sendPost takes the EventPayload of a transformed job, gets the necessary values from the payload and makes a call to destination to push the event to it
//this returns the statusCode, status and response body from the response of the destination call
func (network *NetHandleT) sendPost(jsonData []byte) (statusCode int, status string, respBody string) {
	client := network.httpClient
	//Parse the response to get parameters
	postInfo, err := integrations.GetPostInfo(jsonData)
	if err != nil {
		return 500, "", fmt.Sprintf(`500 GetPostInfoFailed with error: %s`, err.Error())
	}
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

		req, err := http.NewRequest(requestMethod, postInfo.URL, nil)
		if err != nil {
			logger.Error(fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL))
			return 400, "", fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL)
		}

		// add queryparams to the url
		// support of array type in params is handled if the
		// response from transformers are "," seperated
		queryParams := req.URL.Query()
		for key, val := range requestQueryParams {
			valString := fmt.Sprint(val)
			// list := strings.Split(valString, ",")
			// for _, listItem := range list {
			// 	queryParams.Add(key, fmt.Sprint(listItem))
			// }
			queryParams.Add(key, fmt.Sprint(valString))
		}

		req.URL.RawQuery = queryParams.Encode()

		// support for JSON and FORM body type
		if len(bodyValue) > 0 {
			switch bodyFormat {
			case "JSON":
				jsonValue, err := json.Marshal(bodyValue)
				if err != nil {
					panic(err)
				}
				req.Body = ioutil.NopCloser(bytes.NewReader(jsonValue))

			case "FORM":
				formValues := url.Values{}
				for key, val := range bodyValue {
					formValues.Set(key, fmt.Sprint(val)) // transformer ensures top level string values, still val.(string) would be restrictive
				}
				req.Body = ioutil.NopCloser(strings.NewReader(formValues.Encode()))

			default:
				panic(fmt.Errorf("bodyFormat: %s is not supported", bodyFormat))

			}
		}

		headerKV := postInfo.Headers
		for key, val := range headerKV {
			req.Header.Add(key, val.(string))
		}

		req.Header.Add("User-Agent", "RudderLabs")

		resp, err := client.Do(req)

		var respBody []byte

		if resp != nil && resp.Body != nil {
			respBody, _ = ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()
		}

		if err != nil {
			logger.Error("Errored when sending request to the server", err)
			return http.StatusGatewayTimeout, "", string(respBody)
		}

		return resp.StatusCode, resp.Status, string(respBody)

	}

	// returning 200 with a message in case of unsupported processing
	// so that we don't process again. can change this code to anything
	// to be not picked up by router again
	return 200, "method not implemented", ""

}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	logger.Info("Network Handler Startup")
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
	defaultTransportCopy.MaxIdleConns = 100
	defaultTransportCopy.MaxIdleConnsPerHost = 100
	network.httpClient = &http.Client{Transport: &defaultTransportCopy}
	//network.httpClient = &http.Client{}
}
