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

type ResponseStatsHandlerI interface {
	EvalStats(respBody string) (returnCode int)
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

// JSONResponseStatsHandler for json response based stats
type JSONResponseStatsHandler struct {
	abortableErrorRule                   []map[string]interface{} // 400
	expectingInstrumentationErrorRule    []map[string]interface{} // 601
	nonExpectingInstrumentationErrorRule []map[string]interface{} // 602
	configurationErrorRule               []map[string]interface{} // 603
}

// TXTResponseStatsHandler for text response based stats
type TXTResponseStatsHandler struct {
	abortableErrorRule                   []map[string]interface{}
	expectingInstrumentationErrorRule    []map[string]interface{}
	nonExpectingInstrumentationErrorRule []map[string]interface{}
	configurationErrorRule               []map[string]interface{}
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

//Get Response based stats handler. This will be use get stat rules for collecting response based stats
func GetStatsHandler(alertRules map[string]interface{}) ResponseStatsHandlerI {
	if responseType, ok := alertRules["responseType"]; !ok || reflect.TypeOf(responseType).Kind() != reflect.String {
		return nil
	}

	if _, ok := alertRules["routerRules"]; !ok {
		return nil
	}

	var routerRules map[string]interface{}
	var ok bool
	if routerRules, ok = alertRules["routerRules"].(map[string]interface{}); !ok {
		return nil
	}

	abortableErrorRule := getRulesArrForKey("400", routerRules)
	expectingInstrumentationErrorRule := getRulesArrForKey("601", routerRules)
	nonExpectingInstrumentationErrorRule := getRulesArrForKey("602", routerRules)
	configurationErrorRule := getRulesArrForKey("603", routerRules)

	if alertRules["responseType"].(string) == "JSON" {
		return &JSONResponseStatsHandler{
			abortableErrorRule:                   abortableErrorRule,
			expectingInstrumentationErrorRule:    expectingInstrumentationErrorRule,
			nonExpectingInstrumentationErrorRule: nonExpectingInstrumentationErrorRule,
			configurationErrorRule:               configurationErrorRule,
		}
	} else if alertRules["responseType"].(string) == "TXT" {
		return &TXTResponseStatsHandler{
			abortableErrorRule:                   abortableErrorRule,
			expectingInstrumentationErrorRule:    expectingInstrumentationErrorRule,
			nonExpectingInstrumentationErrorRule: nonExpectingInstrumentationErrorRule,
			configurationErrorRule:               configurationErrorRule,
		}
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

//EvalStats -- return codes used to uniquely identify stats
func (handler *JSONResponseStatsHandler) EvalStats(respBody string) (returnCode int) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			returnCode = 400
		}
	}()

	if evalBody(respBody, handler.abortableErrorRule) {
		return 400 //Stat code for abortable Error
	}

	if evalBody(respBody, handler.expectingInstrumentationErrorRule) {
		return 601 //Stat code for expecting instrumentation Error
	}

	if evalBody(respBody, handler.nonExpectingInstrumentationErrorRule) {
		return 602 //Stat code for non expecting instrumentation Error
	}

	if evalBody(respBody, handler.configurationErrorRule) {
		return 603 //Stat code for configuration Error
	}

	return 400
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

//EvalStats -- return codes used to uniquely identify stats
func (handler *TXTResponseStatsHandler) EvalStats(respBody string) (returnCode int) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			returnCode = 400
		}
	}()

	return 400
}
