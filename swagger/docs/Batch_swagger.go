package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/batch batch-tag batchEndpoint
// Batch sends a series of identify, track, page, group, and screen requests in a single batch.
// responses:
//   200: batchResponse

// This text will appear as description of your response body.
// swagger:response batchResponse
type batchResponseWrapper struct {
	// in:body
	Body api.BatchResponse
}

// swagger:parameters batchEndpoint
type batchParamsWrapper struct {
	// This text will appear as description of your request body.
	// in:body
	Body api.BatchRequest
}
