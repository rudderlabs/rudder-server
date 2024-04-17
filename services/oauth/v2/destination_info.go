package v2

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

type DestinationInfo struct {
	WorkspaceID      string
	DefinitionName   string
	DefinitionConfig map[string]interface{}
	ID               string
	Config           map[string]interface{}
}

func (d *DestinationInfo) IsOAuthDestination(flow common.RudderFlow) (bool, error) {
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
	isScopeSupported, err := d.IsOAuthSupportedForFlow(string(flow))
	if err != nil {
		return false, err
	}
	return authType == string(oauth.OAuth) && isScopeSupported, nil
}

func (d *DestinationInfo) IsOAuthSupportedForFlow(flow string) (bool, error) {
	rudderScopesValue, _ := misc.NestedMapLookup(d.DefinitionConfig, "auth", "rudderScopes")
	if rudderScopesValue == nil {
		// valid use-case for non-OAuth destinations
		return false, nil
	}
	rudderScopes, ok := rudderScopesValue.([]string)
	if !ok {
		return false, fmt.Errorf("rudderScopes should be a string[]")
	}
	return lo.Contains(rudderScopes, flow), nil
}

/*
GetAccountID Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`GetAccountID(common.RudderFlowDelete)` --> To be used when we make use of OAuth during regulation flow
`GetAccountID(common.RudderFlowDelivery)` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationInfo) GetAccountID(flow common.RudderFlow) (string, error) {
	idKey := common.DeliveryAccountIDKey
	if flow == common.RudderFlowDelete {
		idKey = common.DeleteAccountIDKey
	}

	rudderAccountIdInterface, found := d.Config[idKey]
	oauthDest, err := d.IsOAuthDestination(flow)
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
