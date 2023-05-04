package reporting

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
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
	defaultErrorMessageKeys = []string{"message", "description", "detail", "title", "Error", "error", "error_message"}
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
