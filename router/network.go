package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/misc"
)

//NetHandleT is the wrapper holding private variables
type NetHandleT struct {
	httpClient *http.Client
}

func (network *NetHandleT) sendPost(jsonData []byte) (int, string, string) {

	client := network.httpClient

	//Parse the response to get parameters
	postInfo := integrations.GetPostInfo(jsonData)

	requestConfig, ok := postInfo.RequestConfig.(map[string]interface{})
	misc.Assert(ok)
	requestMethod := requestConfig["request_method"].(string)
	requestFormat := requestConfig["request-format"].(string)

	switch requestFormat {
	case "PARAMS":
		postInfo.Type = integrations.PostDataKV
		break
	case "JSON":
		postInfo.Type = integrations.PostDataJSON
		break
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
			queryParams.Add(key, val.(string))
		}
	} else if postInfo.Type == integrations.PostDataJSON {
		payloadJSON, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		log.Println("--------------", payloadJSON)
		jsonValue, err := json.Marshal(payloadJSON)
		misc.AssertError(err)
		log.Println("--------------", jsonValue)
		req.Body = ioutil.NopCloser(bytes.NewReader(jsonValue))
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

	log.Println("making sink request")
	log.Println("url===", req.URL.String())
	resp, err := client.Do(req)

	var respBody []byte

	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		log.Println("===body===", string(respBody))
		defer resp.Body.Close()
	}

	if err != nil {
		log.Println("Errored when sending request to the server", err)
		log.Println("===body===", string(respBody))
		return http.StatusGatewayTimeout, "", string(respBody)
	}

	log.Printf("===status %v=====body=== %v", resp.StatusCode, string(respBody))
	return resp.StatusCode, resp.Status, string(respBody)
}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	fmt.Println("Network Handler Startup")
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
