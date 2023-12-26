package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/group group-tag groupEndpoint
// Group associates an identified user with a group and records group traits.
// responses:
//   200: groupResponse

// swagger:response groupResponse
type groupResponseWrapper struct {
	// in:body
	Body api.GroupResponse
}

// swagger:parameters groupEndpoint
type groupParamsWrapper struct {
	// in:body
	Body api.GroupRequest
}
