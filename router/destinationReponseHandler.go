package router

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
		returnCode = respCode
	}

	//TODO complete this
	return returnCode
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
		returnCode = respCode
	}

	return returnCode
}
