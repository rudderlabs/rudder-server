package reporting

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
)

const (
	destinationResponseKey = "destinationResponse"
	errorsKey              = "errors"
)

var (
	urlRegex   = regexp.MustCompile(`\b((?:https?://|www\.)\S+)\b`)
	ipRegex    = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	emailRegex = regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`)
	numRegex   = regexp.MustCompile(`\d+`)
	// notWordRegex = regexp.MustCompile(`\W+`)
	// idRegex                 = regexp.MustCompile(`\b(?\=\w*\d)[\w-]+\b`)
	defaultErrorMessageKeys = []string{"message", "description", "detail", "title", "Error", "error", "error_message", "errors", "response"}
)

type ExtractorT struct {
	log              logger.Logger
	ErrorMessageKeys []string // the keys where in we may have error message
}

func NewErrorDetailExtractor(log logger.Logger) *ExtractorT {
	errMsgKeys := config.GetStringSlice("Reporting.ErrorDetail.ErrorMessageKeys", []string{})
	defaultErrorMessageKeys = append(defaultErrorMessageKeys, errMsgKeys...)
	extractor := &ExtractorT{
		ErrorMessageKeys: defaultErrorMessageKeys,
		log:              log.Child("ErrorDetailExtractor"),
	}
	return extractor
}

// Functions used for error message extraction -- STARTS
func isMapOrArray(value interface{}) bool {
	switch value.(type) {
	case map[string]interface{}, []interface{}:
		return true
	}
	return false
}

func findKeys(keys []string, jsonObj interface{}) map[string]interface{} {
	values := make(map[string]interface{})
	if len(keys) == 0 {
		return values
	}
	// recursively search for keys in nested JSON objects
	switch jsonObject := jsonObj.(type) {
	case map[string]interface{}: // if jsonObj is a map
		for _, key := range keys {
			if value, ok := jsonObject[key]; ok && value != nil {
				values[key] = value
			}
		}
		for _, value := range jsonObject {
			subResults := findKeys(keys, value)
			for k, v := range subResults {
				values[k] = v
			}
		}
	case []interface{}: // if jsonObj is a slice
		for _, item := range jsonObject {
			subResults := findKeys(keys, item)
			for k, v := range subResults {
				values[k] = v
			}
		}
	}
	// if jsonObj is not a map or slice, return an empty map
	return values // return the map of keys and values
}

// This function takes a list of keys and a JSON object as input, and returns the value of the first key that exists in the JSON object.
func findFirstExistingKey(keys []string, jsonObj interface{}) interface{} {
	keyValues := findKeys(keys, jsonObj)
	result := getValue(keys, keyValues)
	if isMapOrArray(result) {
		return findFirstExistingKey(keys, result)
	}
	return result
}

// get value
func getValue(keys []string, jsonObj map[string]interface{}) interface{} {
	for _, key := range keys {
		if value, ok := jsonObj[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

// Functions used for error message extraction -- ENDS

func (ext *ExtractorT) getErrorMessageFromResponse(resp map[string]interface{}) string {
	if _, ok := resp["msg"]; ok {
		return resp["msg"].(string)
	}

	// warehouseKeys := []string{"internal_processing_failed", "fetching_remote_schema_failed", "exporting_data_failed"}

	if destinationResponse, ok := resp[destinationResponseKey].(map[string]interface{}); ok {
		if result := findFirstExistingKey(ext.ErrorMessageKeys, destinationResponse); result != nil {
			return result.(string)
		}
	}
	if message := findFirstExistingKey(ext.ErrorMessageKeys, resp); message != nil {
		if s, ok := message.(string); ok {
			return s
		}
	}

	if errors, ok := getValue([]string{errorsKey}, findKeys([]string{errorsKey}, resp)).([]interface{}); ok && len(errors) > 0 {
		if message := findFirstExistingKey(ext.ErrorMessageKeys, resp); message != nil {
			if s, ok := message.(string); ok {
				return s
			}
		}
	}
	// split the error message by newline and take the first entry

	// for _, whKey := range warehouseKeys {
	//  if val, ok := row["json"].(map[string]interface{})[whKey]; ok {
	//      return getErrorFromWarehouse(val)
	//  }
	// }

	return ""
}

func (ext *ExtractorT) GetErrorMessage(jsonObject string) string {
	var m map[string]interface{}
	e := json.Unmarshal([]byte(jsonObject), &m)
	if e != nil {
		ext.log.Errorf("Problem in unmarshalling: %v", e)
		// TODO: Check with team if this is desirable
		return jsonObject
	}
	return ext.getErrorMessageFromResponse(m)
}

func (ext *ExtractorT) CleanUpErrorMessage(errMsg string) string {
	replacedStr := urlRegex.ReplaceAllString(errMsg, "")
	replacedStr = ipRegex.ReplaceAllString(replacedStr, "")
	replacedStr = emailRegex.ReplaceAllString(replacedStr, "")
	replacedStr = numRegex.ReplaceAllString(replacedStr, "")
	// replacedStr = notWordRegex.ReplaceAllString(replacedStr, "")
	// replacedStr = idRegex.ReplaceAllString(replacedStr, "")

	return strings.TrimSpace(replacedStr)
}

func IsJSON(s string) bool {
	parsedBytes := gjson.ParseBytes([]byte(s))

	s = strings.TrimSpace(s)
	var isEndingFlowerBrace, isEndingArrBrace bool
	if len(s) > 0 {
		isEndingFlowerBrace = s[len(s)-1] == '}'
		isEndingArrBrace = s[len(s)-1] == ']'
	}
	return ((parsedBytes.IsObject() && isEndingFlowerBrace) || (parsedBytes.IsArray() && isEndingArrBrace))
}

// New Logic introduced as previous logic had problems
// Still in testing phase
func GetValFromJSON(errorMsgKeys []string, jsonStr, parentKey string) (string, string) {
	parsedResult := gjson.ParseBytes([]byte(jsonStr))
	var resStr string = jsonStr

	if strings.TrimSpace(resStr) != "" && !IsJSON(resStr) {
		return resStr, parentKey
	}

	if parsedResult.IsObject() {
		// Transformer proxy case
		// "destinationResponse" is always included in "response" object itself
		// Example: { "response": "{\"destinationResponse\": \"...\"}"}
		destRespResult := parsedResult.Get("destinationResponse")
		if destRespResult.Exists() {
			parsedResult = destRespResult
		}
		// Traverse through the map
		for k, parsedRes := range parsedResult.Map() {
			r := parsedRes.String()

			computedErrMsgKey := k
			if strings.TrimSpace(parentKey) != "" {
				computedErrMsgKey = fmt.Sprintf("%s.%v", parentKey, k)
			}

			if IsJSON(r) {
				r, computedErrMsgKey = GetValFromJSON(errorMsgKeys, r, computedErrMsgKey)
			}
			// verify whether evaluated is from right set of probable error message keys
			keyArr := strings.Split(computedErrMsgKey, ".")
			// fmt.Printf("Object keyArr:%#v\n", keyArr)
			isValidErrMsgKey := lo.Contains(errorMsgKeys, keyArr[len(keyArr)-1])
			if isValidErrMsgKey {
				return r, keyArr[len(keyArr)-1]
			}
		}
	}

	if parsedResult.IsArray() {
		for i, res := range parsedResult.Array() {
			arrElStr := res.String()
			computedErrMsgKey := strconv.Itoa(i)
			if strings.TrimSpace(parentKey) != "" {
				computedErrMsgKey = fmt.Sprintf("%s.%v", parentKey, i)
			}
			if !IsJSON(arrElStr) {
				if strings.TrimSpace(arrElStr) != "" {
					// fmt.Println("computedErrMsgKey: ", computedErrMsgKey)
					// fmt.Println("parentKey: ", parentKey)
					return arrElStr, parentKey
				}
			}

			var r string
			r, computedErrMsgKey = GetValFromJSON(errorMsgKeys, res.String(), computedErrMsgKey)

			// verify whether evaluated is from right set of probable error message keys
			keyArr := strings.Split(computedErrMsgKey, ".")
			fmt.Printf("Array keyArr:%#v\n", keyArr)
			isValidErrMsgKey := lo.Contains(errorMsgKeys, keyArr[len(keyArr)-1])
			if isValidErrMsgKey {
				return r, keyArr[len(keyArr)-1]
			}
		}
	}

	// if code reaches here, it means resStr is JSON string
	return "", parentKey
}

func (ext *ExtractorT) GetErrorMessageFromResponse(jsonStr string) string {
	responseJson := gjson.Get(jsonStr, "response")
	var jsonString string = jsonStr
	if responseJson.Exists() {
		jsonString = responseJson.String()
	}
	errMsg, _ := GetValFromJSON(ext.ErrorMessageKeys, jsonString, "")
	return errMsg
}
