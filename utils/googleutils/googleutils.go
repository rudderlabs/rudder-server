package googleutils

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/config"
	"golang.org/x/oauth2/google"
)

const (
	EMPTY_CREDS   = "{}"
	WI_CONFIG_KEY = "workloadIdentity"
)

func CompatibleGoogleCredentialsJSON(jsonKey []byte) error {
	if _, err := google.ConfigFromJSON(jsonKey); err == nil {
		return fmt.Errorf("Google Developers Console client_credentials.json file is not supported")
	}
	return nil
}

func ShouldSkipCredentialsInit(credentials string) bool {
	return IsGKEEnabledWorkload() && IsCredentialsStringEmpty(credentials)
}

/*
	The credentials are deemed to be empty when either the field credentials is
	sent as empty string or when the field is set with "{}"

	Note: This is true only for workload identity enabled rudderstack data-plane deployments
*/
func IsCredentialsStringEmpty(credentials string) bool {
	return (credentials == "" || credentials == EMPTY_CREDS)
}

/*
	We would check for rudder-server configuration for workload identity for google destinations
*/
func IsGKEEnabledWorkload() bool {
	workloadType := config.GetString(fmt.Sprintf("%s.type", WI_CONFIG_KEY), "")
	if workloadType == "GKE" {
		/*
			The workload when deployed in Google GKE cluster, this is to be used

			workloadIdentity.projectId -> The project which contains the workload identity pool in GKE
			workloadIdentity.gsaName -> The google IAM Service account name
			workloadIdentity.gsaProjectId -> The project in which the mentioned google IAM service account is present

			Note: We are currently only validating the schema rather than the values
		*/
		workloadProjectId := config.GetString(fmt.Sprintf("%s.gwiProjectId", WI_CONFIG_KEY), "")
		workloadGsaName := config.GetString(fmt.Sprintf("%s.gsaName", WI_CONFIG_KEY), "")
		workloadGsaProjectId := config.GetString(fmt.Sprintf("%s.gsaProjectId", WI_CONFIG_KEY), "")
		return workloadProjectId != "" && workloadGsaName != "" && workloadGsaProjectId != ""
	}
	return false
}
