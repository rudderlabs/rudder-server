package router

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/misc"
)

//NetHandleT is the wrapper holding private variables
type NetHandleT struct {
	postInfo   integrations.PostParameterT
	httpClient *http.Client
}

func (network *NetHandleT) sendPost(jsonData []byte) (int, string, string) {

	client := network.httpClient
	req, err := http.NewRequest("GET", network.postInfo.URL, nil)
	misc.AssertError(err)

	var eventMap map[string]interface{}

	err = json.Unmarshal(jsonData, &eventMap)
	misc.AssertError(err)

	queryParams := req.URL.Query()
	if network.postInfo.Payload == integrations.PostDataKV {
		for key, val := range eventMap {
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

	if err != nil {
		log.Println("Errored when sending request to the server", err)
		return http.StatusGatewayTimeout, "", "" // sending generic status code
	}

	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)

	log.Println("respBody: ", respBody)

	//return resp.StatusCode, resp.Status, "`{}`"
	return resp.StatusCode, resp.Status, string(respBody) // need to check if respBody is not a json as job status need it to be one
}

//Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	network.postInfo = integrations.GetPostInfo(destID)
	network.httpClient = &http.Client{}
}
