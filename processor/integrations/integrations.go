package integrations

import (
	"encoding/json"
	"fmt"
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

var (
	destTransformURL, userTransformURL string
)

func init() {
	loadConfig()
}

func loadConfig() {
	destTransformURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
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
	Payload       interface{}
	Header        interface{}
	RequestConfig interface{}
}

// PostParameterNewT emulates parameters needed tp make a request
type PostParameterNewT struct {
	Type          string
	URL           string
	RequestMethod string
	UserID        string
	Headers       interface{}
	QueryParams   interface{}
	Body          interface{}
	Files         interface{}
}

// GetResponseVersion Get version of the transformer response
func GetResponseVersion(response json.RawMessage) string {
	parsedResponse := gjson.ParseBytes(response)
	if parsedResponse.Get("output").Exists() {
		return "-1"
	}
	if !parsedResponse.Get("version").Exists() {
		return "0"
	}
	version, ok := parsedResponse.Get("version").Value().(string)
	misc.Assert(ok)
	return version
}

// GetPostInfoNew parses the transformer response
func GetPostInfoNew(transformRaw json.RawMessage) PostParameterNewT {
	var postInfo PostParameterNewT
	var ok bool
	parsedJSON := gjson.ParseBytes(transformRaw)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	postInfo.Type, ok = parsedJSON.Get("type").Value().(string)
	misc.Assert(ok)
	postInfo.URL, ok = parsedJSON.Get("endpoint").Value().(string)
	misc.Assert(ok)
	postInfo.RequestMethod, ok = parsedJSON.Get("method").Value().(string)
	misc.Assert(ok)
	postInfo.UserID, ok = parsedJSON.Get("userId").Value().(string)
	misc.Assert(ok)
	postInfo.Body, ok = parsedJSON.Get("body").Value().(interface{})
	misc.Assert(ok)
	postInfo.Headers, ok = parsedJSON.Get("headers").Value().(interface{})
	misc.Assert(ok)
	postInfo.QueryParams, ok = parsedJSON.Get("params").Value().(interface{})
	misc.Assert(ok)
	postInfo.Files, ok = parsedJSON.Get("files").Value().(interface{})
	misc.Assert(ok)
	return postInfo
}

//GetPostInfo provides the post parameters for this destination
func GetPostInfo(transformRaw json.RawMessage) PostParameterT {
	var postInfo PostParameterT
	var ok bool
	parsedJSON := gjson.ParseBytes(transformRaw)
	postInfo.URL, ok = parsedJSON.Get("endpoint").Value().(string)
	misc.Assert(ok)
	postInfo.UserID, ok = parsedJSON.Get("userId").Value().(string)
	misc.Assert(ok)
	postInfo.Payload, ok = parsedJSON.Get("payload").Value().(interface{})
	misc.Assert(ok)
	postInfo.Header, ok = parsedJSON.Get("header").Value().(interface{})
	misc.Assert(ok)
	postInfo.RequestConfig, ok = parsedJSON.Get("requestConfig").Value().(interface{})
	misc.Assert(ok)
	return postInfo
}

// GetUserIDFromTransformerResponse parses the payload to get userId
func GetUserIDFromTransformerResponse(transformRaw json.RawMessage) string {
	// Get response version
	version := GetResponseVersion(transformRaw)
	var userID string
	switch version {
	case "0":
		response := GetPostInfo(transformRaw)
		userID = response.UserID
	case "-1", "1":
		response := GetPostInfoNew(transformRaw)
		userID = response.UserID
	default:
		misc.Assert(false)
	}
	return userID
}

//GetDestinationIDs parses the destination names from the
//input JSON, matches them with enabled destinations from controle plane and returns the IDSs
func GetDestinationIDs(clientEvent interface{}, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	clientIntgs, ok := misc.GetRudderEventVal("integrations", clientEvent)
	if !ok {
		clientIntgs = make(map[string]interface{})
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
			outVal = append(outVal, destNameIDMap[dest].Name)
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
	return destTransformURL + "/customTransform"
}
