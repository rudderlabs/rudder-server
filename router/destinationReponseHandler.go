package router

import (
	"fmt"

	"github.com/tidwall/gjson"
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

//New returns a destination response handler
func New(responseRules map[string]interface{}) ResponseHandlerI {
	if responseRules["reponseType"].(string) == "JSON" {
		return &JSONResponseHandler{rules: responseRules}
	} else if responseRules["reponseType"].(string) == "TXT" {
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

	if handler.rules == nil {
		return respCode
	}

	//If it is not a 2xx, we don't need to look at the respBody, returning respCode
	if !isSuccessStatus(respCode) {
		return respCode
	}

	abortRules, ok := handler.rules["abortable"].([]map[string]interface{})
	if !ok {
		return respCode
	}

	if evalBody(respBody, abortRules) {
		return 400 //Rudder abort code
	}

	retryableRules, ok := handler.rules["retryable"].([]map[string]interface{})
	if !ok {
		return respCode
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

	if handler.rules == nil {
		return respCode
	}

	returnCode = respCode
	return returnCode
}
