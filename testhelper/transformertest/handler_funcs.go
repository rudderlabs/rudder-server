package transformertest

import (
	"encoding/json"
	"net/http"

	"github.com/rudderlabs/rudder-server/router/types"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

// TransformerHandler is a function that takes a transformer request and returns a response
type TransformerHandler func(request []transformer.TransformerEvent) []transformer.TransformerResponse

// RouterTransformerHandler is a function that takes a router transformer request and returns a response
type RouterTransformerHandler func(request types.TransformMessageT) types.DestinationJobs

// MirroringTransformerHandler mirrors the request payload in the response
var MirroringTransformerHandler TransformerHandler = func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
	for i := range request {
		req := request[i]
		response = append(response, transformer.TransformerResponse{
			Metadata:   req.Metadata,
			Output:     req.Message,
			StatusCode: http.StatusOK,
		})
	}
	return
}

// MirroringRouterTransformerHandler mirrors the router request payload in the response
var MirroringRouterTransformerHandler RouterTransformerHandler = func(request types.TransformMessageT) (response types.DestinationJobs) {
	response = make(types.DestinationJobs, len(request.Data))
	for j := range request.Data {
		req := request.Data[j]
		response[j] = types.DestinationJobT{
			Message:          req.Message,
			JobMetadataArray: []types.JobMetadataT{req.JobMetadata},
			Destination:      req.Destination,
			StatusCode:       http.StatusOK,
		}
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

// ViolationErrorTransformerHandler mirrors the request payload in the response but uses an error status code along with the provided validation errors
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
				StatusCode: http.StatusOK,
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

// WarehouseTransformerHandler mirrors the request payload in the response but uses an error, status code along with warehouse compatible output
func WarehouseTransformerHandler(tableName string, code int, err string) TransformerHandler {
	return func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
		for i := range request {
			req := request[i]
			response = append(response, transformer.TransformerResponse{
				Metadata: req.Metadata,
				Output: map[string]interface{}{
					"table": tableName,
					"data":  req.Message,
					"metadata": map[string]interface{}{
						"table":   tableName,
						"columns": map[string]interface{}{},
					},
				},
				StatusCode: code,
				Error:      err,
			})
		}
		return
	}
}
