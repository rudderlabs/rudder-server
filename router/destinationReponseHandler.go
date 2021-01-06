package router

type ResponseHandlerI interface {
	IsSuccessStatus(respCode int, respBody string) int
}

type JSONResponseHandler struct {
}

type TXTResponseHandler struct {
}

//New returns a destination response handler
func New(config map[string]interface{}) ResponseHandlerI {
	if config["reponseType"].(string) == "JSON" {
		return &JSONResponseHandler{}
	}

	return nil
}

func (handler *JSONResponseHandler) IsSuccessStatus(respCode int, respBody string) int {
	//TODO complete this
	return 200
}

func (handler *TXTResponseHandler) IsSuccessStatus(respCode int, respBody string) int {
	return 200
}
