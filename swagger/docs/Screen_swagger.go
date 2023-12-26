package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/screen screen-tag screenEndpoint
// Screen records whenever a mobile app user sees a screen.
// responses:
//   200: screenResponse

// swagger:response screenResponse
type screenResponseWrapper struct {
	// in:body
	Body api.ScreenResponse
}

// swagger:parameters screenEndpoint
type screenParamsWrapper struct {
	// in:body
	Body api.ScreenRequest
}
