package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/identify identify-tag identifyEndpoint
// Identify captures relevant details about a visitor user and records associated traits.
// responses:
//   200: identifyResponse

// swagger:response identifyResponse
type identifyResponseWrapper struct {
	// in:body
	Body api.IdentifyResponse
}

// swagger:parameters identifyEndpoint
type identifyParamsWrapper struct {
	// in:body
	Body api.IdentifyRequest
}
