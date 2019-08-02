package router

import (
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

	var req *http.Request
	var err error
	if useTestSink {
		req, err = http.NewRequest("GET", testSinkURL, nil)
		misc.AssertError(err)
	} else {
		req, err = http.NewRequest("GET", postInfo.URL, nil)
		misc.AssertError(err)
	}

	queryParams := req.URL.Query()
	if postInfo.Type == integrations.PostDataKV {
		payloadKV, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		for key, val := range payloadKV {
			queryParams.Add(key, val.(string))
		}
	} else {
		//Not implemented yet
		misc.Assert(false)
	}

	req.URL.RawQuery = queryParams.Encode()
	req.Header.Add("User-Agent", "RudderLabs")

	log.Println("making sink request")
	resp, err := client.Do(req)

	var respBody []byte

	if resp != nil && resp.Body != nil {
		respBody, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	if err != nil {
		log.Println("Errored when sending request to the server", err)
		return http.StatusGatewayTimeout, "", string(respBody)
	}

	return resp.StatusCode, resp.Status, string(respBody)
}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	fmt.Println("Network Handler Startup")
	//Reference http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	misc.Assert(ok)
	defaultTransport := *defaultTransportPointer
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100
	network.httpClient = &http.Client{Transport: &defaultTransport}
	//network.httpClient = &http.Client{}
}
