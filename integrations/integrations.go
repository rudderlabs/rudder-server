package integrations

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
)

//our internal ID for that destination. We save this ID in the customval field
//in JobsDB
var destNameIDMap = map[string]string{
	"google_analytics": "GA",
	"rudderlabs":       "GA",
	"amplitude":        "AM",
}

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
	URL     string
	Type    int
	UserID  string
	Payload interface{} //PostDataKV or PostDataJSON or PostDataXML
}

//GetPostInfo provides the post parameters for this destination
func GetPostInfo(transformRaw json.RawMessage) PostParameterT {

	var transformMap map[string]interface{}
	err := json.Unmarshal(transformRaw, &transformMap)
	misc.AssertError(err)

	var postInfo PostParameterT
	pType, ok := transformMap["request-format"].(string)
	misc.Assert(ok)
	switch pType {
	case "PARAMS":
		postInfo.Type = PostDataKV
	default:
		misc.Assert(false)
	}
	postInfo.URL, ok = transformMap["endpoint"].(string)
	misc.Assert(ok)
	postInfo.Payload, ok = transformMap["payload"]
	misc.Assert(ok)
	postInfo.UserID, ok = transformMap["user_id"].(string)
	misc.Assert(ok)
	return postInfo
}

//GetAllDestinations returns all the destinations that have been
//enabled
func GetAllDestinations() []string {
	allDest := make([]string, 0)
	for dest := range destJSTransformerMap {
		allDest = append(allDest, dest)
	}
	return allDest
}

//GetDestinationIDs parses the destination names from the
//input JSON and returns the IDSs
func GetDestinationIDs(clientEvent interface{}) (retVal []string) {
	clientIntgs, ok := misc.GetRudderEventVal("rl_integrations", clientEvent)
	if !ok {
		return
	}

	clientIntgsList, ok := clientIntgs.([]interface{})
	if !ok {
		return
	}
	var outVal []string
	for _, integ := range clientIntgsList {
		customVal, ok := destNameIDMap[strings.ToLower(integ.(string))]
		if ok {
			outVal = append(outVal, customVal)
		}
	}
	retVal = outVal
	return
}

//GetDestinationURL returns node URL
func GetDestinationURL(destID string) (string, bool) {
	path, ok := destJSTransformerMap[destID]
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%s/%s", destTransformURL, path), true
}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL() string {
	return userTransformURL
}
