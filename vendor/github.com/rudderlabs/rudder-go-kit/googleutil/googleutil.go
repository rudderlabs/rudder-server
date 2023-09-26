package googleutil

import (
	"fmt"

	"golang.org/x/oauth2/google"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const (
	EMPTY_CREDS   = "{}"
	WI_CONFIG_KEY = "workloadIdentity"
)

func CompatibleGoogleCredentialsJSON(jsonKey []byte) error {
	// google.ConfigFromJSON checks if jsonKey is a valid console client_credentials.json
	// which we won't support so "err == nil" means it is bad for us.
	if _, err := google.ConfigFromJSON(jsonKey); err == nil {
		return fmt.Errorf("google developers console client_credentials.json file is not supported")
	}
	return nil
}

func ShouldSkipCredentialsInit(credentials string) bool {
	return isGKEEnabledWorkload() && isCredentialsStringEmpty(credentials)
}

/*
IsCredentialsStringEmpty checks for empty credentials.
The credentials are deemed to be empty when either the field credentials is
sent as empty string or when the field is set with "{}"

Note: This is true only for workload identity enabled rudderstack data-plane deployments
*/
func isCredentialsStringEmpty(credentials string) bool {
	return (credentials == "" || credentials == EMPTY_CREDS)
}

/*
IsGKEEnabledWorkload  checks against rudder-server configuration to find if workload identity for google destinations is enabled
*/
func isGKEEnabledWorkload() bool {
	workloadType := config.GetString(fmt.Sprintf("%s.type", WI_CONFIG_KEY), "")
	return workloadType == "GKE"
}
