package router

import (
	"fmt"
	"github.com/tidwall/gjson"
	"reflect"
)

//ResponseHandlerI - handle destination response
type ResponseHandlerI interface {
	IsSuccessStatus(respCode int, respBody string) (returnCode int)
}

//JSONResponseHandler handler for json response
type JSONResponseHandler struct {
	rules map[string]interface{}
}

//TXTResponseHandler handler for text response
type TXTResponseHandler struct {
	rules map[string]interface{}
}

//New returns a destination response handler. Can be nil(Check before using this)
func New(responseRules map[string]interface{}) ResponseHandlerI {
	if responseType, ok := responseRules["responseType"]; !ok || reflect.TypeOf(responseType).Kind() != reflect.String {
		return nil
	}

	if responseRules["responseType"].(string) == "JSON" {
		if _, ok := responseRules["rules"]; !ok {
			return nil
		}
		var rules map[string]interface{}
		var ok bool
		if rules, ok = responseRules["rules"].(map[string]interface{}); !ok {
			return nil
		}
		return &JSONResponseHandler{rules: rules}
	} else if responseRules["responseType"].(string) == "TXT" {

		return &TXTResponseHandler{rules: responseRules}
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

func evalBody(body string, rules []map[string]interface{}) bool {
	for _, rulesArr := range rules {
		var brokeOutOfLoop bool
		for k, v := range rulesArr {
			stringifiedVal := getStringifiedVal(v)
			result := gjson.Get(body, k)
			if result.Raw != stringifiedVal {
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

	if handler == nil || handler.rules == nil {
		return respCode
	}

	//If it is not a 2xx, we don't need to look at the respBody, returning respCode
	if !isSuccessStatus(respCode) {
		return respCode
	}

	abortRulesArr, ok := handler.rules["abortable"].([]interface{})
	if !ok {
		return respCode
	}
	abortRules := []map[string]interface{}{}
	for _, value := range abortRulesArr {
		if rule, ok := value.(map[string]interface{}); ok {
			abortRules = append(abortRules, rule)
		}
	}
	if evalBody(respBody, abortRules) {
		return 400 //Rudder abort code
	}

	retryableRulesArr, ok := handler.rules["retryable"].([]interface{})
	if !ok {
		return respCode
	}
	retryableRules := []map[string]interface{}{}
	for _, value := range retryableRulesArr {
		if rule, ok := value.(map[string]interface{}); ok {
			retryableRules = append(retryableRules, rule)
		}
	}
	if evalBody(respBody, retryableRules) {
		return 500 //Rudder retry code
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

	if handler == nil || handler.rules == nil {
		return respCode
	}

	returnCode = respCode
	return returnCode
}
