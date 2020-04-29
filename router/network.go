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
	if !ok {
		panic(fmt.Errorf("typecast of postInfo.RequestConfig to map[string]interface{} failed"))
	}
	requestMethod, ok := requestConfig["requestMethod"].(string)
	if !(ok && (requestMethod == "POST" || requestMethod == "GET")) {
		panic(fmt.Errorf("typecast of requestConfig[\"requestMethod\"] to string failed. or requestMethod:%s is neither POST nor GET", requestMethod))
	}
	requestFormat := requestConfig["requestFormat"].(string)
	if !ok {
		panic(fmt.Errorf("typecast of requestConfig[\"requestFormat\"] to string failed"))
	}

	switch requestFormat {
	case "PARAMS":
		postInfo.Type = integrations.PostDataKV
	case "JSON":
		postInfo.Type = integrations.PostDataJSON
	default:
		panic(fmt.Errorf("requestFormat:%s is neither PARAMS nor JSON", requestFormat))
	}

	var req *http.Request
	var err error
	if useTestSink {
		req, err = http.NewRequest(requestMethod, testSinkURL, nil)
		if err != nil {
			panic(err)
		}
	} else {
		req, err = http.NewRequest(requestMethod, postInfo.URL, nil)
		if err != nil {
			panic(err)
		}
	}

	queryParams := req.URL.Query()
	if postInfo.Type == integrations.PostDataKV {
		payloadKV, ok := postInfo.Payload.(map[string]interface{})
		if !ok {
			panic(fmt.Errorf("typecast of postInfo.Payload to map[string]interface{} failed"))
		}
		for key, val := range payloadKV {
			queryParams.Add(key, fmt.Sprint(val))
		}
	} else if postInfo.Type == integrations.PostDataJSON {
		payloadJSON, ok := postInfo.Payload.(map[string]interface{})
		if !ok {
			panic(fmt.Errorf("typecast of postInfo.Payload to map[string]interface{} failed"))
		}
		jsonValue, err := json.Marshal(payloadJSON)
		if err != nil {
			panic(err)
		}
		req.Body = ioutil.NopCloser(bytes.NewReader(jsonValue))
	} else {
		//Not implemented yet
		panic(fmt.Errorf("postInfo.Type : %d is not implemented", postInfo.Type))
	}

	req.URL.RawQuery = queryParams.Encode()

	headerKV, ok := postInfo.Header.(map[string]interface{})
	if !ok {
		panic(fmt.Errorf("typecast of postInfo.Header to map[string]interface{} failed"))
	}
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

func (network *NetHandleT) processNewResponseType(jsonData []byte) (int, string, string) {
	client := network.httpClient
	//Parse the response to get parameters
	postInfo := integrations.GetPostInfoNew(jsonData)

	isRest := postInfo.Type == "REST"

	isMultipart := len(postInfo.Files.(map[string]interface{})) > 0

	// going forward we may want to support GraphQL and multipart requests
	// the files key in the response is specifically to handle the multipart usecase
	// for type GraphQL may need to support more keys like expected response format etc
	// in future it's expected that we will build on top of this response type
	// so, code addition should be done here instead of version bumping of response.
	if isRest && !isMultipart {
		requestMethod := postInfo.RequestMethod
		requestBody := postInfo.Body.(map[string]interface{})
		requestQueryParams := postInfo.QueryParams.(map[string]interface{})
		var bodyFormat string
		var bodyValue map[string]interface{}
		for k, v := range requestBody {
			if len(v.(map[string]interface{})) > 0 {
				bodyFormat = k
				bodyValue = v.(map[string]interface{})
				break
			}

		}

		var req *http.Request
		var err error
		if useTestSink {
			req, err = http.NewRequest(requestMethod, testSinkURL, nil)
			if err != nil {
				panic(err)
			}
		} else {
			req, err = http.NewRequest(requestMethod, postInfo.URL, nil)
			if err != nil {
				panic(err)
			}
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

		headerKV, ok := postInfo.Headers.(map[string]interface{})
		if !ok {
			panic(fmt.Errorf("typecast of postInfo.Headers to map[string]interface{} failed"))
		}
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

func (network *NetHandleT) sendPost(jsonData []byte) (int, string, string) {
	// Get response version
	version := integrations.GetResponseVersion(jsonData)

	if version == "0" {
		return network.processOldResponseType(jsonData)
	}
	return network.processNewResponseType(jsonData)
}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	logger.Info("Network Handler Startup")
	//Reference http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Errorf("typecast of defaultRoundTripper to *http.Transport failed"))
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
