package googleutils

import (
	"fmt"

	"golang.org/x/oauth2/google"
)

const (
	EMPTY_CREDS = "{}"
)

func CompatibleGoogleCredentialsJSON(jsonKey []byte) error {
	if _, err := google.ConfigFromJSON(jsonKey); err == nil {
		return fmt.Errorf("Google Developers Console client_credentials.json file is not supported")
	}
	return nil
}

/*
The credentials are deemed to be empty when either the field credentials is
sent as empty string or when the field is set with "{}"
*/
func IsEmptyCredentials(credentials string) bool {
	return credentials == "" || credentials == EMPTY_CREDS
}
