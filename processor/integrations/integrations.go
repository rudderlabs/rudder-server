package integrations

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var (
	destTransformURL      string
	postParametersTFields []string
)

func init() {
	loadConfig()

	// This is called in init and it should be a one time call. Making reflect calls during runtime is not a great idea.
	// We unmarshal json response from transformer into PostParametersT struct.
	// Since unmarshal doesn't check if the fields are present in the json or not and instead just initialze to zero value, we have to manually do this check on all fields before unmarshaling
	// This function gets a list of fields tagged as json from the struct and populates in postParametersTFields
	postParametersTFields = misc.GetMandatoryJSONFieldNames(PostParametersT{})
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

// PostParametersT is a struct for holding all the values from transformerResponse and use them to publish an event to a destination
// optional is a custom tag introduced by us and is handled by GetMandatoryJSONFieldNames. Its intentionally added
// after two commas because the tag that comes after the first comma should be known by json parser
type PostParametersT struct {
	Type          string `json:"type"`
	URL           string `json:"endpoint"`
	RequestMethod string `json:"method"`
	//Invalid tag used in struct. skipcq: SCC-SA5008
	UserID      string                 `json:"userId,,optional"`
	Headers     map[string]interface{} `json:"headers"`
	QueryParams map[string]interface{} `json:"params"`
	Body        map[string]interface{} `json:"body"`
	Files       map[string]interface{} `json:"files"`
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
	// All is by default true, if not present make it true
	allVal, found := clientIntgsList["All"]
	if !found {
		allVal = true
	}
	_, isAllBoolean := allVal.(bool)
	if !isAllBoolean {
		return
	}
	var outVal []string
	for dest := range destNameIDMap {
		_, isBoolean := clientIntgsList[dest].(bool)
		// if dest is bool and is present in clientIntgretaion list, check if true/false
		if isBoolean {
			if clientIntgsList[dest] == true {
				outVal = append(outVal, destNameIDMap[dest].Name)
			}
			continue
		}
		// Always add for syntax dest:{...}
		_, isMap := clientIntgsList[dest].(map[string]interface{})
		if isMap {
			outVal = append(outVal, destNameIDMap[dest].Name)
			continue
		}
		// if dest  not present in clientIntgretaion list, add based on All flag
		if allVal.(bool) {
			outVal = append(outVal, destNameIDMap[dest].Name)
		}
	}
	retVal = outVal
	return
}

//GetTransformerURL gets the transfomer base url endpoint
func GetTransformerURL() string {
	return destTransformURL
}

//GetDestinationURL returns node URL
func GetDestinationURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destType))
	if misc.Contains(warehouse.WarehouseDestinations, destType) {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s&whIDResolve=%v", config.GetWHSchemaVersion(), warehouseutils.IDResolutionEnabled())
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
