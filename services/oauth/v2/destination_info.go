package v2

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type DestinationInfo struct {
	WorkspaceID      string
	DefinitionName   string
	DefinitionConfig map[string]interface{}
	ID               string
	Config           map[string]interface{}
}

func (d *DestinationInfo) IsOptionalOAuth(flow common.RudderFlow) bool {
	authTypeValue, _ := misc.NestedMapLookup(d.DefinitionConfig, "auth", "type")
	if authTypeValue == nil {
		return false
	}
	authType, ok := authTypeValue.(string)
	if !ok {
		return false
	}
	accountID, err := d.GetAccountID(flow)
	if err != nil {
		return false
	}
	if strings.TrimSpace(accountID) == "" {
		return false
	}
	return authType == string(oauth.OptionalOAuth)
}

func (d *DestinationInfo) isOAuth(flow common.RudderFlow) (bool, error) {
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
	isScopeSupported, err := d.isOAuthSupportedForFlow(string(flow))
	if err != nil {
		return false, err
	}
	return authType == string(oauth.OAuth) && isScopeSupported, nil
}

func (d *DestinationInfo) IsOAuthDestination(flow common.RudderFlow) (bool, error) {
	isOAuth, err := d.isOAuth(flow)
	if err != nil {
		return false, err
	}
	isOptionalOAuth := d.IsOptionalOAuth(flow)
	return isOAuth || isOptionalOAuth, nil
}

func (d *DestinationInfo) isOAuthSupportedForFlow(flow string) (bool, error) {
	rudderScopesValue, _ := misc.NestedMapLookup(d.DefinitionConfig, "auth", "rudderScopes")
	if rudderScopesValue == nil {
		// valid use-case for non-OAuth destinations
		// when the auth.type is OAuth and rudderScopes is not mentioned, we would assume oauth flow is to be used when it is in "delivery" flow
		return flow == string(common.RudderFlowDelivery), nil
	}
	interfaceArr, ok := rudderScopesValue.([]interface{})
	if !ok {
		return false, fmt.Errorf("rudderScopes should be a interface[]")
	}
	var rudderScopes []string
	for _, scopeInterface := range interfaceArr {
		scope, ok := scopeInterface.(string)
		if !ok {
			return false, fmt.Errorf("%v in auth.rudderScopes should be string", scopeInterface)
		}
		rudderScopes = append(rudderScopes, scope)
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
	if !found || idKey == "" {
		return "", fmt.Errorf("destination is not an oauth destination or accountId not found")
	}
	rudderAccountId, ok := rudderAccountIdInterface.(string)
	if !ok {
		return "", fmt.Errorf("%s is not a string", idKey)
	}
	return rudderAccountId, nil
}
