package googleutils

import (
	"fmt"

	"golang.org/x/oauth2/google"
)

func CompatibleGoogleCredentialsJSON(jsonKey []byte) error {
	if _, err := google.ConfigFromJSON(jsonKey); err == nil {
		return fmt.Errorf("google developers console client_credentials.json file is not supported")
	}
	return nil
}
