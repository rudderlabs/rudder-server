package integrations

import (
	"errors"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
)

var (
	jsonfast              = jsoniter.ConfigCompatibleWithStandardLibrary
	destTransformURL      string
	userTransformURL      string
	postParametersTFields []string
)

func Init() {
	loadConfig()
	// This is called in init and it should be a one time call. Making reflect calls during runtime is not a great idea.
	// We unmarshal json response from transformer into PostParametersT struct.
	// Since unmarshal doesn't check if the fields are present in the json or not and instead just initialze to zero value, we have to manually do this check on all fields before unmarshaling
	// This function gets a list of fields tagged as json from the struct and populates in postParametersTFields
	postParametersTFields = misc.GetMandatoryJSONFieldNames(PostParametersT{})
}

func loadConfig() {
	destTransformURL = config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	userTransformURL = config.GetString("USER_TRANSFORM_URL", destTransformURL)
}

const (
	// PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	// PostDataJSON means post data is sent as JSON
	PostDataJSON
	// PostDataXML means post data is sent as XML
	PostDataXML
)

// PostParametersT is a struct for holding all the values from transformerResponse and use them to publish an event to a destination
// optional is a custom tag introduced by us and is handled by GetMandatoryJSONFieldNames. Its intentionally added
// after two commas because the tag that comes after the first comma should be known by json parser
type PostParametersT struct {
	Type          string `json:"type"`
	URL           string `json:"endpoint"`
	RequestMethod string `json:"method"`
	// Invalid tag used in struct. skipcq: SCC-SA5008
	UserID      string                 `json:"userId,,optional"` //nolint:staticcheck
	Headers     map[string]interface{} `json:"headers"`
	QueryParams map[string]interface{} `json:"params"`
	Body        map[string]interface{} `json:"body"`
	Files       map[string]interface{} `json:"files"`
}

type TransStatsT struct {
	StatTags map[string]string `json:"statTags"`
}
type TransResponseT struct {
	Message             string      `json:"message"`
	DestinationResponse interface{} `json:"destinationResponse"`
	AuthErrorCategory   string      `json:"authErrorCategory"`
}

func CollectDestErrorStats(input []byte) {
	var integrationStat TransStatsT
	err := jsonfast.Unmarshal(input, &integrationStat)
	if err == nil {
		if len(integrationStat.StatTags) > 0 {
			stats.Default.NewTaggedStat("integration.failure_detailed", stats.CountType, integrationStat.StatTags).Increment()
		}
	}
}

func CollectIntgTransformErrorStats(input []byte) {
	var integrationStats []TransStatsT
	err := jsonfast.Unmarshal(input, &integrationStats)
	if err == nil {
		for _, integrationStat := range integrationStats {
			if len(integrationStat.StatTags) > 0 {
				stats.Default.NewTaggedStat("integration.failure_detailed", stats.CountType, integrationStat.StatTags).Increment()
			}
		}
	}
}

// GetPostInfo parses the transformer response
func ValidatePostInfo(transformRawParams PostParametersT) error {
	transformRaw, err := jsonfast.Marshal(transformRawParams)
	if err != nil {
		return err
	}
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
		err := errors.New(strings.Join(errorMessages, "\n"))
		return err
	}

	return nil
}

// FilterClientIntegrations parses the destination names from the
// input JSON, matches them with enabled destinations from controle plane and returns the IDSs
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

// GetTransformerURL gets the transfomer base url endpoint
func GetTransformerURL() string {
	return destTransformURL
}

// GetDestinationURL returns node URL
func GetDestinationURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/destinations/%s", destTransformURL, strings.ToLower(destType))
	if misc.Contains(warehouseutils.WarehouseDestinations, destType) {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s&whIDResolve=%v", config.GetString("Warehouse.schemaVersion", "v1"), warehouseutils.IDResolutionEnabled())
		if destType == "RS" {
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		}
		if destType == "CLICKHOUSE" {
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", config.GetBool("Warehouse.clickhouse.enableArraySupport", false)))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		}
		return destinationEndPoint + "?" + whSchemaVersionQueryParam
	}
	return destinationEndPoint
}

// GetUserTransformURL returns the port of running user transform
func GetUserTransformURL() string {
	return userTransformURL + "/customTransform"
}

// GetTrackingPlanValidationURL returns the port of running tracking plan validation
func GetTrackingPlanValidationURL() string {
	return destTransformURL + "/v0/validate"
}
