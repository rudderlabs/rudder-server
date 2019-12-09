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

func (network *NetHandleT) processOldResponseType(jsonData []byte) (int, string, string) {
	client := network.httpClient
	//Parse the response to get parameters
	postInfo := integrations.GetPostInfo(jsonData)

	requestConfig, ok := postInfo.RequestConfig.(map[string]interface{})
	misc.Assert(ok)
	requestMethod, ok := requestConfig["requestMethod"].(string)
	misc.Assert(ok && (requestMethod == "POST" || requestMethod == "GET" || requestMethod == "PUT"))
	requestFormat := requestConfig["requestFormat"].(string)
	misc.Assert(ok)

	switch requestFormat {
	case "PARAMS":
		postInfo.Type = integrations.PostDataKV
	case "JSON":
		postInfo.Type = integrations.PostDataJSON
	case "FORM":
		postInfo.Type = integrations.PostDataFORM
	default:
		misc.Assert(false)
	}

	var req *http.Request
	var err error
	if useTestSink {
		req, err = http.NewRequest(requestMethod, testSinkURL, nil)
		misc.AssertError(err)
	} else {
		req, err = http.NewRequest(requestMethod, postInfo.URL, nil)
		misc.AssertError(err)
	}

	queryParams := req.URL.Query()
	if postInfo.Type == integrations.PostDataKV {
		payloadKV, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		for key, val := range payloadKV {
			queryParams.Add(key, fmt.Sprint(val))
		}
	} else if postInfo.Type == integrations.PostDataJSON {
		payloadJSON, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		jsonValue, err := json.Marshal(payloadJSON)
		misc.AssertError(err)
		req.Body = ioutil.NopCloser(bytes.NewReader(jsonValue))
	} else if postInfo.Type == integrations.PostDataFORM {
		payloadFormKV, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		formValues := url.Values{}
		for key, val := range payloadFormKV {
			fmt.Println(" === key , ==val=== ", key, " ", val)
			formValues.Set(key, fmt.Sprint(val)) // transformer ensures top level string values, still val.(string) would be restrictive
		}
		req.Body = ioutil.NopCloser(strings.NewReader(formValues.Encode()))
	} else {
		//Not implemented yet
		misc.Assert(false)
	}

	req.URL.RawQuery = queryParams.Encode()

	headerKV, ok := postInfo.Header.(map[string]interface{})
	misc.Assert(ok)
	for key, val := range headerKV {
		req.Header.Add(key, val.(string))
	}

	req.Header.Add("User-Agent", "RudderLabs")

	resp, err := client.Do(req)

	var respBody []byte

	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		fmt.Println("== resp body=== ", string(respBody))
		defer resp.Body.Close()
	}

	if err != nil {
		logger.Error("Errored when sending request to the server", err)
		fmt.Println("== resp body=== ", string(respBody))
		return http.StatusGatewayTimeout, "", string(respBody)
	}

	return resp.StatusCode, resp.Status, string(respBody)
}

func (network *NetHandleT) processNewResponseType(jsonData []byte) (int, string, string) {
	client := network.httpClient
	//Parse the response to get parameters
	postInfo := integrations.GetPostInfoNew(jsonData)

	isRest := postInfo.Type == "REST"

	isMultipart := len(postInfo.Files.(map[string]interface{})) > 0

	if isRest && !isMultipart {
		requestMethod := postInfo.RequestMethod
		requestBody := postInfo.Body.(map[string]interface{})
		requestQueryParams := postInfo.QueryParams.(map[string]interface{})
		var bodyFormat string
		var bodyValue map[string]interface{}
		for k, v := range requestBody {
			bodyFormat = k
			bodyValue = v.(map[string]interface{})
		}

		var req *http.Request
		var err error
		if useTestSink {
			req, err = http.NewRequest(requestMethod, testSinkURL, nil)
			misc.AssertError(err)
		} else {
			req, err = http.NewRequest(requestMethod, postInfo.URL, nil)
			misc.AssertError(err)
		}

		queryParams := req.URL.Query()
		for key, val := range requestQueryParams {
			queryParams.Add(key, fmt.Sprint(val))
		}

		req.URL.RawQuery = queryParams.Encode()

		switch bodyFormat {
		case integrations.PostDataJSON:
			payloadJSON, ok := bodyValue.(map[string]interface{})
			misc.Assert(ok)
			jsonValue, err := json.Marshal(payloadJSON)
			misc.AssertError(err)
			req.Body = ioutil.NopCloser(bytes.NewReader(jsonValue))

		case integrations.PostDataFORM:
			payloadFormKV, ok := bodyValue.(map[string]interface{})
			misc.Assert(ok)
			formValues := url.Values{}
			for key, val := range payloadFormKV {
				fmt.Println(" === key , ==val=== ", key, " ", val)
				formValues.Set(key, fmt.Sprint(val)) // transformer ensures top level string values, still val.(string) would be restrictive
			}
			req.Body = ioutil.NopCloser(strings.NewReader(formValues.Encode()))

		default:
			misc.Assert(false)

		}

		headerKV, ok := postInfo.Headers.(map[string]interface{})
		misc.Assert(ok)
		for key, val := range headerKV {
			req.Header.Add(key, val.(string))
		}

		req.Header.Add("User-Agent", "RudderLabs")

		resp, err := client.Do(req)

		var respBody []byte

		if resp != nil && resp.Body != nil {
			respBody, _ = ioutil.ReadAll(resp.Body)
			fmt.Println("== resp body=== ", string(respBody))
			defer resp.Body.Close()
		}

		if err != nil {
			logger.Error("Errored when sending request to the server", err)
			fmt.Println("== resp body=== ", string(respBody))
			return http.StatusGatewayTimeout, "", string(respBody)
		}

		return resp.StatusCode, resp.Status, string(respBody)

	}

	return 200, "method not implemented", ""

}

func (network *NetHandleT) sendPost(jsonData []byte) (int, string, string) {
	versionToFunc := map[int]func([]byte) (int, string, string){
		0: network.processOldResponseType,
		1: network.processNewResponseType,
	}
	// Get response version
	version := integrations.GetResponseVersion(jsonData)

	return versionToFunc[version](jsonData)

}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	logger.Info("Network Handler Startup")
	//Reference http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	misc.Assert(ok)
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
