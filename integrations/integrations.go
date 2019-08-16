package integrations

import (
	"encoding/json"
	"fmt"
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
)

var (
	destTransformURL, userTransformURL string
)

func init() {
	loadConfig()
}

func loadConfig() {
	destTransformURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	userTransformURL = config.GetEnv("USER_TRANSFORM_URL", "http://localhost:9191")
}

//destJSTransformerMap keeps a mapping between the destinationID and
//the NodeJS URL end point where the transformation function is hosted
//This should be coming from the config when that's ready
var destJSTransformerMap = map[string]string{
	"GA": "/v0/ga",
	"AM": "/v0/amplitude",
}

const (
	//PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	//PostDataJSON means post data is sent as JSON
	PostDataJSON
	//PostDataXML means post data is sent as XML
	PostDataXML
)

//PostParameterT  has post related parameters, the URL and the data type
type PostParameterT struct {
	URL           string
	Type          int
	UserID        string
	Payload       interface{} //PostDataKV or PostDataJSON or PostDataXML
	Header        interface{}
	RequestConfig interface{}
}

//GetPostInfo provides the post parameters for this destination
func GetPostInfo(transformRaw json.RawMessage) PostParameterT {

	var transformMap map[string]interface{}
	err := json.Unmarshal(transformRaw, &transformMap)
	misc.AssertError(err)

	var postInfo PostParameterT
	var ok bool
	postInfo.URL, ok = transformMap["endpoint"].(string)
	misc.Assert(ok)
	postInfo.Payload, ok = transformMap["payload"]
	misc.Assert(ok)
	postInfo.UserID, ok = transformMap["user_id"].(string)
	misc.Assert(ok)
	postInfo.Header, ok = transformMap["header"]
	misc.Assert(ok)
	postInfo.RequestConfig, ok = transformMap["request_config"]
	return postInfo
}

//GetDestinationIDs parses the destination names from the
//input JSON, matches them with enabled destinations from controle plane and returns the IDSs
func GetDestinationIDs(clientEvent interface{}, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	clientIntgs, ok := misc.GetRudderEventVal("rl_integrations", clientEvent)
	if !ok {
		return
	}

	clientIntgsList, ok := clientIntgs.(map[string]interface{})
	if !ok {
		return
	}
	var outVal []string
	for dest := range destNameIDMap {
		if clientIntgsList[dest] == false {
			continue
		}
		if (clientIntgsList["All"] != false) || clientIntgsList[dest] == true {
			outVal = append(outVal, dest)
		}
	}
	retVal = outVal
	return
}

//GetDestinationURL returns node URL
func GetDestinationURL(destID string) string {
	return fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destID))
}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL() string {
	return userTransformURL
}
