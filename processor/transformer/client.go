package transformer

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/config"
)

// DestTransformURL is the transformer service base URL, configured by DEST_TRANSFORM_URL environmental variable.
var DestTransformURL string

func init() {
	DestTransformURL = strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/")
}

// GetURL returns a full URL for the configured transformer and provided endpoint.
// Endpoint should start with '/'.
func GetURL(endpoint string) string {
	return fmt.Sprintf("%s%s", DestTransformURL, endpoint)
}

//GetDestinationURL returns node URL
func GetDestinationURL(destID string) string {
	return GetURL(fmt.Sprintf("/v0/%s?whSchemaVersion=%s", strings.ToLower(destID), config.GetWHSchemaVersion()))
}

//GetUserTransformURL returns the port of running user transform
func GetUserTransformURL(processSessions bool) string {
	if processSessions {
		return GetURL("/customTransform?processSessions=true")
	}
	return GetURL("/customTransform")
}
