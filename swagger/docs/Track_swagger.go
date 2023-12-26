package docs

import "github.com/rudderlabs/rudder-server/swagger/api"

// swagger:route POST /v1/track track-tag trackEndpoint
// Track records user actions along with their associated properties.
// responses:
//   200: trackResponse

// swagger:response trackResponse
type trackResponseWrapper struct {
	// in:body
	Body api.TrackResponse
}

// swagger:parameters trackEndpoint
type trackParamsWrapper struct {
	// in:body
	Body api.TrackRequest
}
