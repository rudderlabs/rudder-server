package reporting

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	responseKey = "response"

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

	WhitespacesRegex          = regexp.MustCompile("[ \t\n\r]*") // used in checking if string is a valid json to remove extra-spaces
	defaultErrorMessageKeys   = []string{"message", "description", "detail", "title", "error", "error_message"}
	defaultWhErrorMessageKeys = []string{"internal_processing_failed", "fetching_remote_schema_failed", "exporting_data_failed"}
)

type ExtractorT struct {
	log                logger.Logger
	ErrorMessageKeys   []string // the keys where in we may have error message
	WhErrorMessageKeys []string // the keys where in we may have error message for warehouse destinations
}

func NewErrorDetailExtractor(log logger.Logger) *ExtractorT {
	errMsgKeys := config.GetStringSlice("Reporting.ErrorDetail.ErrorMessageKeys", []string{})
	whErrMsgKeys := config.GetStringSlice("Reporting.ErrorDetail.WhErrorMessageKeys", []string{})
	// adding to default message keys
	defaultErrorMessageKeys = append(defaultErrorMessageKeys, errMsgKeys...)
	defaultWhErrorMessageKeys = append(defaultWhErrorMessageKeys, whErrMsgKeys...)

	extractor := &ExtractorT{
		ErrorMessageKeys:   defaultErrorMessageKeys,
		WhErrorMessageKeys: defaultWhErrorMessageKeys,
		log:                log.Child("ErrorDetailExtractor"),
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

func unmarshalJsonToMap(jsonStr string, logger *logger.Logger) map[string]interface{} {
	var j map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &j)
	if err != nil {
		errStr := fmt.Sprintf("Unmarshal Err: %v\nJsonStr:%v\n", err, jsonStr)
		// For proper logging
		if logger != nil {
			(*logger).Errorf(errStr)
		} else {
			fmt.Println(errStr)
		}

		return nil
	}
	return j
}

func (ext *ExtractorT) getJsonResponse(sampleResponse string) string {
	// json_response
	// parses the []byte json
	if !IsJSON(sampleResponse) {
		return sampleResponse
	}
	// if sampleResponse is indeed json
	sampleResp := gjson.Parse(sampleResponse)
	respRes := gjson.GetBytes([]byte(sampleResp.String()), responseKey)
	if respRes.Exists() && IsJSON(respRes.Str) {
		var j json.RawMessage
		e := json.Unmarshal([]byte(respRes.String()), &j)
		if e != nil {
			// panic(e)
			ext.log.Errorf("(GetJsonResponse)UnmarshalErr: %v\tResponse:%v\n", e, respRes.String())
			return respRes.String()
		}
		byteArr, setErr := sjson.SetBytes([]byte(sampleResp.String()), responseKey, string(j))
		if setErr != nil {
			ext.log.Errorf("SetBytesError: %v\tWill return empty string as error response\n", setErr)
			return ""
		}
		return string(byteArr)
	}

	return sampleResp.String()
}

func (ext *ExtractorT) getSimpleMessage(jsonStr string) string {
	if !IsJSON(jsonStr) {
		return jsonStr
	}
	error_reason_result := gjson.GetMany(jsonStr, "reason", "Error", responseKey)
	for _, erRes := range error_reason_result {
		if erRes.Exists() {
			erResStr := erRes.String()
			switch erRes.Path(jsonStr) {
			case "reason":
				return erResStr
			case "Error":
				if !IsJSON(erResStr) {
					return strings.Split(erResStr, "\n")[0]
				}
				return ""
			case responseKey:
				if len(erResStr) == 0 {
					return "EMPTY"
				}
				if !IsJSON(erResStr) {
					return erResStr
				}

				// recursively search for common error message patterns
				j := unmarshalJsonToMap(erResStr, &ext.log)
				if j == nil {
					return ""
				}
				return getErrorMessageFromResponse(j, ext.ErrorMessageKeys)
			}
		}
	}

	// For warehouse related errors
	whJson := unmarshalJsonToMap(jsonStr, &ext.log)
	if whJson == nil {
		return jsonStr
	}
	for _, whKey := range ext.WhErrorMessageKeys {
		if val, ok := whJson[whKey]; ok {
			valAsMap, isMap := val.(map[string]interface{})
			if !isMap {
				ext.log.Debugf("Failed while type asserting to map[string]interface{} warehouse error with whKey:%s", whKey)
				return ""
			}
			return getErrorFromWarehouse(valAsMap)
		}
	}
	return ""
}

func (ext *ExtractorT) GetErrorMessage(sampleResponse string) string {
	jsonResp := ext.getJsonResponse(sampleResponse)
	return ext.getSimpleMessage(jsonResp)
}

func findKeys(keys []string, jsonObj interface{}) map[string]interface{} {
	values := make(map[string]interface{})
	if len(keys) == 0 {
		return values
	}
	// recursively search for keys in nested JSON objects
	if checkForGoMapOrList(jsonObj) {
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
	// if jsonObj is not a map or slice, return an empty map
	return values
}

// This function takes a list of keys and a JSON object as input, and returns the value of the first key that exists in the JSON object.
func findFirstExistingKey(keys []string, jsonObj interface{}) interface{} {
	keyValues := findKeys(keys, jsonObj)
	result := getValue(keys, keyValues)
	if checkForGoMapOrList(result) {
		return findFirstExistingKey(keys, result)
	}
	return result
}

func getValue(keys []string, jsonObj map[string]interface{}) interface{} {
	for _, key := range keys {
		if value, ok := jsonObj[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

func getErrorMessageFromResponse(resp map[string]interface{}, messageKeys []string) string {
	if _, ok := resp["msg"]; ok {
		return resp["msg"].(string)
	}

	if destinationResponse, ok := resp["destinationResponse"].(map[string]interface{}); ok {
		if result := findFirstExistingKey(messageKeys, destinationResponse); result != nil {
			return result.(string)
		}
	}
	if message := findFirstExistingKey(messageKeys, resp); message != nil {
		if s, ok := message.(string); ok {
			return s
		}
	}

	if errors, ok := getValue([]string{errorsKey}, findKeys([]string{errorsKey}, resp)).([]interface{}); ok && len(errors) > 0 {
		s := make([]string, len(errors))
		for i, v := range errors {
			s[i] = fmt.Sprint(v)
		}
		return strings.Join(s, ".")
	}
	// split the error message by newline and take the first entry

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
	errorStrings := make([]string, len(errors))
	for i, v := range errors {
		errorStrings[i] = fmt.Sprint(v)
	}
	return strings.Join(errorStrings, ".")
}

func IsJSON(s string) bool {
	parsedBytes := gjson.ParseBytes([]byte(s))

	s = string(WhitespacesRegex.ReplaceAllLiteral([]byte(parsedBytes.String()), []byte("")))
	var isEndingFlowerBrace, isEndingArrBrace bool
	if len(s) > 0 {
		isEndingFlowerBrace = s[len(s)-1] == '}'
		isEndingArrBrace = s[len(s)-1] == ']'
	}
	return ((parsedBytes.IsObject() && isEndingFlowerBrace) || (parsedBytes.IsArray() && isEndingArrBrace))
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
