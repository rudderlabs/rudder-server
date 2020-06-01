package integrations

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

var (
	destTransformURL, userTransformURL string
	customDestination                  []string
	whSchemaVersion                    string
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
	if !ok {
		panic(fmt.Errorf(""))
	}
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
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"type\") to string failed"))
	}
	postInfo.URL, ok = parsedJSON.Get("endpoint").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"endpoint\") to string failed"))
	}
	postInfo.RequestMethod, ok = parsedJSON.Get("method").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"method\") to string failed"))
	}
	postInfo.UserID, ok = parsedJSON.Get("userId").Value().(string)
	if !ok {
		postInfo.UserID = fmt.Sprintf("%v", parsedJSON.Get("userId").Value())
	}
	postInfo.Body, ok = parsedJSON.Get("body").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"body\") to interface{} failed"))
	}
	postInfo.Headers, ok = parsedJSON.Get("headers").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"headers\") to interface{} failed"))
	}
	postInfo.QueryParams, ok = parsedJSON.Get("params").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"params\") to interface{} failed"))
	}
	postInfo.Files, ok = parsedJSON.Get("files").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"files\") to interface{} failed"))
	}
	return postInfo
}

//GetPostInfo provides the post parameters for this destination
func GetPostInfo(transformRaw json.RawMessage) PostParameterT {
	var postInfo PostParameterT
	var ok bool
	parsedJSON := gjson.ParseBytes(transformRaw)
	postInfo.URL, ok = parsedJSON.Get("endpoint").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"endpoint\") to string failed"))
	}
	postInfo.UserID, ok = parsedJSON.Get("userId").Value().(string)
	if !ok {
		postInfo.UserID = fmt.Sprintf("%v", parsedJSON.Get("userId").Value())
	}
	postInfo.Payload, ok = parsedJSON.Get("payload").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"payload\") to interface{} failed"))
	}
	postInfo.Header, ok = parsedJSON.Get("header").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"header\") to interface{} failed"))
	}
	postInfo.RequestConfig, ok = parsedJSON.Get("requestConfig").Value().(interface{})
	if !ok {
		panic(fmt.Errorf("typecast of parsedJSON.Get(\"requestConfig\") to interface{} failed"))
	}
	return postInfo
}

// GetUserIDFromTransformerResponse parses the payload to get userId
func GetUserIDFromTransformerResponse(transformRaw json.RawMessage) string {

	var userID string
	parsedJSON := gjson.ParseBytes(transformRaw)
	var ok bool
	if userID, ok = parsedJSON.Get("userId").Value().(string); !ok {
		userID = fmt.Sprintf("%v", parsedJSON.Get("userId").Value())
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
	return fmt.Sprintf("%s/v0/%s?whSchemaVersion=%s", destTransformURL, strings.ToLower(destID), getWHSchemaVersion())
}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL(processSessions bool) string {
	if processSessions {
		return destTransformURL + "/customTransform?processSessions=true"
	}
	return destTransformURL + "/customTransform"
}

func createDBConnection() *sql.DB {
	var err error
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		panic(err)
	}

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}
	return dbHandle
}

func getWHSchemaVersion() string {
	if whSchemaVersion != "" {
		return whSchemaVersion
	}

	// get from db
	dbHandle := createDBConnection()

	var version string
	sqlStatememnt := fmt.Sprintf(`SELECT parameters ->> 'wh_schema_version' as version FROM workspace`)
	err := dbHandle.QueryRow(sqlStatememnt).Scan(&version)
	if err != nil {
		panic(err)
	}
	whSchemaVersion = version
	dbHandle.Close()
	return version
}
