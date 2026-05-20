package googleauth

import (
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"google.golang.org/api/option"
)

type credentialsPayload struct {
	Type string `json:"type"`
}

// CredentialsTypeFromJSON returns the google option credential type based on
// the "type" field in the provided credential JSON.
//
// If type detection fails, it falls back to ServiceAccount for backwards
// compatibility with older destination configs that do not include type.
func CredentialsTypeFromJSON(credentials []byte) option.CredentialsType {
	var payload credentialsPayload
	if err := jsonrs.Unmarshal(credentials, &payload); err != nil {
		return option.ServiceAccount
	}

	switch payload.Type {
	case "service_account":
		return option.ServiceAccount
	case "authorized_user":
		return option.AuthorizedUser
	case "impersonated_service_account":
		return option.ImpersonatedServiceAccount
	case "external_account":
		return option.ExternalAccount
	default:
		return option.ServiceAccount
	}
}
