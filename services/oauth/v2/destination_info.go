package v2

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type DestinationInfo struct {
	WorkspaceID      string
	DefinitionName   string
	DefinitionConfig map[string]interface{}
	ID               string
	Config           map[string]interface{}
}

func (d *DestinationInfo) IsOAuthDestination() (bool, error) {
	authValue, _ := misc.NestedMapLookup(d.DefinitionConfig, "auth", "type")
	if authValue == nil {
		// valid use-case for non-OAuth destinations
		return false, nil
	}
	authType, ok := authValue.(string)
	if !ok {
		// we should throw error here, as we expect authValue to be a string if present
		return false, fmt.Errorf("auth type is not a string: %v", authValue)
	}
	return authType == string(oauth.OAuth), nil
}

/*
GetAccountID Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`GetAccountID(destDetail.Config, "rudderDeleteAccountId")` --> To be used when we make use of OAuth during regulation flow
`GetAccountID(destDetail.Config, "rudderAccountId")` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationInfo) GetAccountID(idKey string) (string, error) {
	rudderAccountIdInterface, found := d.Config[idKey]
	oauthDest, err := d.IsOAuthDestination()
	if err != nil {
		return "", fmt.Errorf("failed to check if destination is oauth destination: %v", err)
	}
	if !oauthDest || !found || idKey == "" {
		return "", fmt.Errorf("destination is not an oauth destination or accountId not found")
	}
	rudderAccountId, ok := rudderAccountIdInterface.(string)
	if !ok {
		return "", fmt.Errorf("rudderAccountId is not a string")
	}
	return rudderAccountId, nil
}
