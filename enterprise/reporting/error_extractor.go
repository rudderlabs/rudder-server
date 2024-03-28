package reporting

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	responseKey = "response"
	errorKey    = "error"
	spaceStr    = " "

	errorsKey = "errors"
)

var (
	urlRegex         = regexp.MustCompile(`\b((?:https?://|www\.)\S+)\b`)
	ipRegex          = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	emailRegex       = regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`)
	notWordRegex     = regexp.MustCompile(`\W+`)
	idRegex          = regexp.MustCompile(`\b([a-zA-Z0-9-]*\d[a-zA-Z0-9-]*)\b`)
	spaceRegex       = regexp.MustCompile(`\s+`)
	whitespacesRegex = regexp.MustCompile("[ \t\n\r]*") // used in checking if string is a valid json to remove extra-spaces

	defaultErrorMessageKeys = []string{"message", "description", "detail", "title", errorKey, "error_message"}

	deprecationKeywords = map[string]float64{
		"end-of-life":                 3,
		"end of life":                 3,
		"no longer supported":         3,
		"end-of-support":              3,
		"deprecated functionality":    3,
		"unsupported version":         3,
		"version upgrade recommended": 3,
		"retired":                     2,
		"API retirement":              2,
		"feature removal":             2,
		"migration required":          2,
		"upgrade mandatory":           2,
		"security vulnerability":      2,
		"deprecated":                  2,
		"deprecation":                 2,
		"deprecated software":         2,
		"upgrade required":            2,
		"deprecated feature":          2,
		"version no longer valid":     2,
		"deprecated library":          2,
		"version upgrade":             2,
		"deprecated component":        2,
		"upgrade recommended":         2,
		"discontinued":                1,
		"obsolete":                    1,
		"outdated":                    1,
		"old version":                 1,
		"discontinued product":        1,
		"version obsolescence":        1,
		"deprecated module":           1,
		"incompatible version":        1,
		"version":                     0.5,
		"upgrade":                     0.5,
		"update":                      0.5,
		"new version":                 0.5,
		"latest version":              0.5,
		"improved version":            0.5,
		// Negation terms
		"not deprecated":           -1,
		"still supported":          -1,
		"actively maintained":      -1,
		"compatible version":       -1,
		"no migration required":    -1,
		"security patch available": -1,
	}
)

var lowercasedDeprecationKeywords = lo.MapKeys(deprecationKeywords, func(_ float64, key string) string {
	return strings.ToLower(key)
})

type ExtractorHandle struct {
	log                              logger.Logger
	ErrorMessageKeys                 []string // the keys where in we may have error message
	versionDeprecationThresholdScore misc.ValueLoader[float64]
}

func NewErrorDetailExtractor(log logger.Logger) *ExtractorHandle {
	errMsgKeys := config.GetStringSlice("Reporting.ErrorDetail.ErrorMessageKeys", []string{})
	versionDepThreshold := config.GetReloadableFloat64Var(1.0, "Reporting.ErrorDetail.versionDeprecationThresholdScore")
	// adding to default message keys
	defaultErrorMessageKeys = append(defaultErrorMessageKeys, errMsgKeys...)

	extractor := &ExtractorHandle{
		ErrorMessageKeys:                 defaultErrorMessageKeys,
		log:                              log.Child("ErrorDetailExtractor"),
		versionDeprecationThresholdScore: versionDepThreshold,
	}
	return extractor
}

// Functions used for error message extraction -- STARTS
func checkForGoMapOrList(value interface{}) bool {
	switch value.(type) {
	case map[string]interface{}, []interface{}:
		return true
	}
	return false
}

func (ext *ExtractorHandle) getSimpleMessage(jsonStr string) string {
	if !IsJSON(jsonStr) {
		return jsonStr
	}

	var jsonMap map[string]interface{}
	er := json.Unmarshal([]byte(jsonStr), &jsonMap)
	if er != nil {
		ext.log.Debugf("%v is not a unmarshallable into interface{}", jsonStr)
		return jsonStr
	}

	for key, erRes := range jsonMap {
		erResStr, isString := erRes.(string)
		if !isString {
			ext.log.Debugf("Type-assertion failed for %v with value %v: not a string", key, erRes)
		}
		switch key {
		case "reason":
			return erResStr
		case "Error":
			if !IsJSON(erResStr) {
				return strings.Split(erResStr, "\n")[0]
			}
			return ""
		case responseKey, errorKey:
			if IsJSON(erResStr) {
				var unmarshalledJson interface{}
				unmarshalledErr := json.Unmarshal([]byte(erResStr), &unmarshalledJson)
				if unmarshalledErr != nil {
					return erResStr
				}
				return getErrorMessageFromResponse(unmarshalledJson, ext.ErrorMessageKeys)
			}
			if len(erResStr) == 0 {
				return ""
			}
			return erResStr
		// Warehouse related errors
		case "internal_processing_failed", "fetching_remote_schema_failed", "exporting_data_failed":
			valAsMap, isMap := erRes.(map[string]interface{})
			if !isMap {
				ext.log.Debugf("Failed while type asserting to map[string]interface{} warehouse error with whKey:%s", key)
				return ""
			}
			return getErrorFromWarehouse(valAsMap)
		}
	}

	return ""
}

func (ext *ExtractorHandle) GetErrorMessage(sampleResponse string) string {
	return ext.getSimpleMessage(sampleResponse)
}

func findKeys(keys []string, jsonObj interface{}) map[string]interface{} {
	values := make(map[string]interface{})
	if len(keys) == 0 {
		return values
	}
	// recursively search for keys in nested JSON objects
	switch jsonObj := jsonObj.(type) {
	case map[string]interface{}: // if jsonObj is a map
		for _, key := range keys {
			if value, ok := jsonObj[key]; ok && value != nil {
				values[key] = value
			}
		}
		for _, value := range jsonObj {
			subResults := findKeys(keys, value)
			for k, v := range subResults {
				values[k] = v
			}
		}
	case []interface{}: // if jsonObj is a slice
		for _, item := range jsonObj {
			subResults := findKeys(keys, item)
			for k, v := range subResults {
				values[k] = v
			}
		}
	}
	return values // return the map of keys and values
}

// This function takes a list of keys and a JSON object as input, and returns the value of the first key that exists in the JSON object.
func findFirstExistingKey(keys []string, jsonObj interface{}) interface{} {
	keyValues := findKeys(keys, jsonObj)
	result := getFirstNonNilValue(keys, keyValues)
	if checkForGoMapOrList(result) {
		return findFirstExistingKey(keys, result)
	}
	return result
}

func getFirstNonNilValue(keys []string, jsonObj map[string]interface{}) interface{} {
	for _, key := range keys {
		if value := jsonObj[key]; value != nil {
			return value
		}
	}
	return nil
}

func convertInterfaceArrToStrArrWithDelimitter(arrI []interface{}, delimitter string) string {
	s := make([]string, len(arrI))
	for i, v := range arrI {
		s[i] = fmt.Sprint(v)
	}
	return strings.Join(s, delimitter)
}

func getErrorMessageFromResponse(resp interface{}, messageKeys []string) string {
	var respMap map[string]interface{}
	respMap, isMap := resp.(map[string]interface{})

	getMessage := func(msgKeys []string, response interface{}) string {
		if result := findFirstExistingKey(msgKeys, response); result != nil {
			if s, ok := result.(string); ok {
				return strings.TrimSpace(s)
			}
		}
		return ""
	}

	var msg string

	if !isMap {
		goto errorsBlock
	}
	if _, ok := respMap["msg"]; ok {
		return respMap["msg"].(string)
	}

	if destinationResponse, ok := respMap["destinationResponse"].(map[string]interface{}); ok {
		msg = getMessage(messageKeys, destinationResponse)
		if msg != "" {
			return msg
		}
	}
	msg = getMessage(messageKeys, resp)
	if len(strings.TrimSpace(msg)) != 0 {
		return msg
	}

errorsBlock:
	errors, ok := getFirstNonNilValue([]string{errorsKey}, findKeys([]string{errorsKey}, resp)).([]interface{})
	if ok && len(errors) > 0 {
		return convertInterfaceArrToStrArrWithDelimitter(errors, ".")
	}

	return ""
}

func getErrorFromWarehouse(resp map[string]interface{}) string {
	errorsI, ok := resp[errorsKey]
	if !ok {
		return ""
	}
	arrOfErrs, isIntfArr := errorsI.([]interface{})
	if !isIntfArr {
		return ""
	}
	errors := lo.Uniq(arrOfErrs)
	return convertInterfaceArrToStrArrWithDelimitter(errors, ".")
}

func IsJSON(s string) bool {
	parsedBytesResult := gjson.ParseBytes([]byte(s))
	// Scenarios where we might have problems if the below logic is not included
	// 1. Parsing of a string which contains { or [ at the start of the string could be parsed successfully
	// 2. A valid with spacing before { or [ can also be deemed as not valid string

	// We are making sure we remove white-spaces when we check the string for being an array or an object (scenario-2 is covered)
	s = string(whitespacesRegex.ReplaceAllLiteral([]byte(parsedBytesResult.String()), []byte("")))
	var isEndingFlowerBrace, isEndingArrBrace bool
	// We are making sure we check for end-braces for array or object(scenario-1 is covered)
	if len(s) > 0 {
		isEndingFlowerBrace = s[len(s)-1] == '}'
		isEndingArrBrace = s[len(s)-1] == ']'
	}
	return (parsedBytesResult.IsObject() && isEndingFlowerBrace) || (parsedBytesResult.IsArray() && isEndingArrBrace)
}

func (ext *ExtractorHandle) CleanUpErrorMessage(errMsg string) string {
	var regexdMsg string
	regexdMsg = urlRegex.ReplaceAllLiteralString(errMsg, spaceStr)
	regexdMsg = ipRegex.ReplaceAllLiteralString(regexdMsg, spaceStr)
	regexdMsg = emailRegex.ReplaceAllLiteralString(regexdMsg, spaceStr)
	regexdMsg = notWordRegex.ReplaceAllLiteralString(regexdMsg, spaceStr)
	regexdMsg = idRegex.ReplaceAllLiteralString(regexdMsg, spaceStr)
	regexdMsg = spaceRegex.ReplaceAllLiteralString(regexdMsg, spaceStr)

	return regexdMsg
}

func (ext *ExtractorHandle) GetErrorCode(errorMessage string) string {
	// version deprecation logic
	var score float64
	var errorCode string

	errorMessage = strings.ToLower(errorMessage)
	for keyword, s := range lowercasedDeprecationKeywords {
		if strings.Contains(errorMessage, keyword) {
			score += s
		}
	}

	if score > ext.versionDeprecationThresholdScore.Load() {
		errorCode = "deprecation"
	}
	return errorCode
}
