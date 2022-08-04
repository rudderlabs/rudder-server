package googleutils

import (
	"fmt"

	"golang.org/x/oauth2/google"
)

func CompatibleGoogleCredentialsJSON(jsonKey []byte) error {
	// google.ConfigFromJSON checks if jsonKey is a valid console client_credentials.json
	// which we won't support so "err == nil" means it is bad for us.
	if _, err := google.ConfigFromJSON(jsonKey); err == nil {
		return fmt.Errorf("google developers console client_credentials.json file is not supported")
	}
	return nil
}
