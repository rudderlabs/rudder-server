package transformertest

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

// TransformerHandler is a function that takes a transformer request and returns a response
type TransformerHandler func(request []transformer.TransformerEvent) []transformer.TransformerResponse

// MirroringTransformerHandler mirrors the request payload in the response
var MirroringTransformerHandler TransformerHandler = func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
	for i := range request {
		req := request[i]
		response = append(response, transformer.TransformerResponse{
			Metadata:   req.Metadata,
			Output:     req.Message,
			StatusCode: 200,
		})
	}
	return
}

// ErrorTransformerHandler mirrors the request payload in the response but uses an error status code
func ErrorTransformerHandler(code int, err string) TransformerHandler {
	return func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
		for i := range request {
			req := request[i]
			response = append(response, transformer.TransformerResponse{
				Metadata:   req.Metadata,
				Output:     req.Message,
				StatusCode: code,
				Error:      err,
			})
		}
		return
	}
}

// ValidationErrorTransformerHandler mirrors the request payload in the response but uses an error status code along with the provided validation errors
func ViolationErrorTransformerHandler(code int, err string, validationErrors []transformer.ValidationError) TransformerHandler {
	return func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
		for i := range request {
			req := request[i]
			response = append(response, transformer.TransformerResponse{
				Metadata:         req.Metadata,
				Output:           req.Message,
				StatusCode:       code,
				Error:            err,
				ValidationErrors: validationErrors,
			})
		}
		return
	}
}

// EmptyTransformerHandler returns an empty response
var EmptyTransformerHandler TransformerHandler = func(request []transformer.TransformerEvent) []transformer.TransformerResponse {
	return []transformer.TransformerResponse{}
}

// DestTransformerHandler returns an empty response
func DestTransformerHandler(f func(event transformer.TransformerEvent) integrations.PostParametersT) func(request []transformer.TransformerEvent) []transformer.TransformerResponse {
	return func(request []transformer.TransformerEvent) (res []transformer.TransformerResponse) {
		for _, req := range request {
			postParameters := f(req)
			jsonString, _ := json.Marshal(postParameters)
			var output map[string]interface{}
			_ = json.Unmarshal(jsonString, &output)
			res = append(res, transformer.TransformerResponse{
				Metadata:   req.Metadata,
				Output:     output,
				StatusCode: 200,
			})
		}
		return
	}
}

// RESTJSONDestTransformerHandler transforms the request payload into a REST JSON destination request using the original message as the payload
func RESTJSONDestTransformerHandler(method, url string) func(request []transformer.TransformerEvent) []transformer.TransformerResponse {
	return DestTransformerHandler(func(event transformer.TransformerEvent) integrations.PostParametersT {
		return integrations.PostParametersT{
			Type:          "REST",
			URL:           url,
			RequestMethod: method,
			Body: map[string]interface{}{
				"JSON": event.Message,
			},
		}
	})
}
