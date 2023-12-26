package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/page page-tag pageEndpoint
// Page records website page views with additional relevant information.
// responses:
//   200: pageResponse

// swagger:response pageResponse
type pageResponseWrapper struct {
	// in:body
	Body api.PageResponse
}

// swagger:parameters pageEndpoint
type pageParamsWrapper struct {
	// in:body
	Body api.PageRequest
}
