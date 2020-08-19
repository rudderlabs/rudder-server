package integrations

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var (
	destTransformURL, userTransformURL string
	customDestination                  []string
	whSchemaVersion                    string
	postParametersTFields              []string
)

func init() {
	loadConfig()
	populatePostParameterFields()
}

func loadConfig() {
	destTransformURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
}

// This is called in init and it should be a one time call. Having reflect calls happening during runtime is not a great idea.
// We unmarshal json response from transformer into PostParametersT struct.
// Since unmarshal doesn't check if the fields are present in the json or not and instead just initialze to zero value, we have to manually do this check on all fields before unmarshaling
// This function gets a list of fields tagged as json from the struct and populates in postParametersTFields

func populatePostParameterFields() {
	v := reflect.TypeOf(PostParametersT{})
	postParametersTFields = make([]string, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		postParametersTFields[i] = strings.Split(v.Field(i).Tag.Get("json"), ",")[0]
	}
}

const (
	//PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	//PostDataJSON means post data is sent as JSON
	PostDataJSON
	//PostDataXML means post data is sent as XML
	PostDataXML
)

// PostParametersT is a struct for holding all the values from transformerResponse and use them to publish an event to a destination
type PostParametersT struct {
	Type          string                 `json:"type"`
	URL           string                 `json:"endpoint"`
	RequestMethod string                 `json:"method"`
	UserID        string                 `json:"userId"`
	Headers       map[string]interface{} `json:"headers"`
	QueryParams   map[string]interface{} `json:"params"`
	Body          map[string]interface{} `json:"body"`
	Files         map[string]interface{} `json:"files"`
}

// GetPostInfo parses the transformer response
func GetPostInfo(transformRaw json.RawMessage) (postInfo PostParametersT, err error) {
	parsedJSON := gjson.ParseBytes(transformRaw)
	errorMessages := make([]string, 0)
	for _, v := range postParametersTFields {
		if !parsedJSON.Get(v).Exists() {
			errMessage := fmt.Sprintf("missing expected field : %s", v)
			errorMessages = append(errorMessages, errMessage)
		}
	}
	if len(errorMessages) > 0 {
		errorMessages = append(errorMessages, fmt.Sprintf("in transformer response : %v", parsedJSON))
		err = errors.New(strings.Join(errorMessages, "\n"))
		return postInfo, err
	}
	unMarshalError := json.Unmarshal(transformRaw, &postInfo)
	if unMarshalError != nil {
		err = fmt.Errorf("Error while unmarshalling response from transformer : %s, Error: %w", transformRaw, unMarshalError)
	}
	return postInfo, err
}

// GetUserIDFromTransformerResponse parses the payload to get userId
func GetUserIDFromTransformerResponse(transformRaw json.RawMessage) (userID string, found bool) {
	parsedJSON := gjson.ParseBytes(transformRaw)
	userIDVal := parsedJSON.Get("userId").Value()

	if userIDVal != nil {
		return fmt.Sprintf("%v", userIDVal), true
	}
	return
}

//FilterClientIntegrations parses the destination names from the
//input JSON, matches them with enabled destinations from controle plane and returns the IDSs
func FilterClientIntegrations(clientEvent types.SingularEventT, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
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
func GetDestinationURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destType))
	if misc.Contains(warehouse.WarehouseDestinations, destType) {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s", config.GetWHSchemaVersion())
		if destType == "RS" {
			rsAlterStringToTextQueryParam := fmt.Sprintf("rsAlterStringToText=%s", fmt.Sprintf("%v", config.GetVarCharMaxForRS()))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + rsAlterStringToTextQueryParam
		}
		return destinationEndPoint + "?" + whSchemaVersionQueryParam
	}
	return destinationEndPoint

}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL(processSessions bool) string {
	if processSessions {
		return destTransformURL + "/customTransform?processSessions=true"
	}
	return destTransformURL + "/customTransform"
}
