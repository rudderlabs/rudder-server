package router

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/tidwall/gjson"
)

//ResponseHandlerI - handle destination response
type ResponseHandlerI interface {
	IsSuccessStatus(respCode int, respBody string) (returnCode int)
}

//JSONResponseHandler handler for json response
type JSONResponseHandler struct {
	abortRules     []map[string]interface{}
	retryableRules []map[string]interface{}
	throttledRules []map[string]interface{}
}

//TXTResponseHandler handler for text response
type TXTResponseHandler struct {
	abortRules     []map[string]interface{}
	retryableRules []map[string]interface{}
	throttledRules []map[string]interface{}
}

func getRulesArrForKey(key string, rules map[string]interface{}) []map[string]interface{} {
	rulesArr := []map[string]interface{}{}

	rulesForKey, ok := rules[key].([]interface{})
	if !ok {
		return rulesArr
	}
	for _, value := range rulesForKey {
		if rule, ok := value.(map[string]interface{}); ok {
			rulesArr = append(rulesArr, rule)
		}
	}

	return rulesArr
}

//New returns a destination response handler. Can be nil(Check before using this)
func New(responseRules map[string]interface{}) ResponseHandlerI {
	if responseType, ok := responseRules["responseType"]; !ok || reflect.TypeOf(responseType).Kind() != reflect.String {
		return nil
	}

	if _, ok := responseRules["rules"]; !ok {
		return nil
	}

	var rules map[string]interface{}
	var ok bool
	if rules, ok = responseRules["rules"].(map[string]interface{}); !ok {
		return nil
	}

	abortRules := getRulesArrForKey("abortable", rules)
	retryableRules := getRulesArrForKey("retryable", rules)
	throttledRules := getRulesArrForKey("throttled", rules)

	if responseRules["responseType"].(string) == "JSON" {
		return &JSONResponseHandler{abortRules: abortRules, retryableRules: retryableRules, throttledRules: throttledRules}
	} else if responseRules["responseType"].(string) == "TXT" {
		return &TXTResponseHandler{abortRules: abortRules, retryableRules: retryableRules, throttledRules: throttledRules}
	}

	return nil
}

func getStringifiedVal(val interface{}) string {
	switch v := val.(type) {
	case int:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func stripQuotes(str string) string {
	if strings.HasPrefix(str, `"`) {
		return str[1 : len(str)-1]
	}

	return str
}

func evalBody(body string, rules []map[string]interface{}) bool {
	for _, rulesArr := range rules {
		var brokeOutOfLoop bool
		for k, v := range rulesArr {
			stringifiedVal := getStringifiedVal(v)
			result := gjson.Get(body, k)
			if stripQuotes(result.Raw) != stringifiedVal {
				brokeOutOfLoop = true
				break
			}
		}

		if brokeOutOfLoop {
			continue
		} else {
			return true
		}
	}

	return false
}

//JSONResponseHandler -- start

//IsSuccessStatus - returns the status code based on the response code and body
func (handler *JSONResponseHandler) IsSuccessStatus(respCode int, respBody string) (returnCode int) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			returnCode = respCode
		}
	}()

	//If it is not a 2xx, we don't need to look at the respBody, returning respCode
	if !isSuccessStatus(respCode) {
		return respCode
	}

	if evalBody(respBody, handler.abortRules) {
		return 400 //Rudder abort code
	}

	if evalBody(respBody, handler.retryableRules) {
		return 500 //Rudder retry code
	}

	if evalBody(respBody, handler.throttledRules) {
		return 429 //Rudder throttle code
	}

	return respCode
}

//TXTResponseHandler -- start

//IsSuccessStatus - returns the status code based on the response code and body
func (handler *TXTResponseHandler) IsSuccessStatus(respCode int, respBody string) (returnCode int) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			returnCode = respCode
		}
	}()

	returnCode = respCode
	return returnCode
}
