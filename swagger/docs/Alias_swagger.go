package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/alias alias-tag aliasEndpoint
// Alias captures details about a user's alternate/past identity and merges or associates it with the current one.
// responses:
//   200: aliasResponse

// swagger:response aliasResponse
type aliasResponseWrapper struct {
	// in:body
	Body api.AliasResponse
}

// swagger:parameters aliasEndpoint
type aliasParamsWrapper struct {
	// in:body
	Body api.AliasRequest
}
