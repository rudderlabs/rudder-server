package backendconfig

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	destinationDefinitionOAuthType = "OAuth"
	accountDefinitionOAuthType     = "oauth"
)

/*
GetAccountID Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`dest.GetAccountID(common.RudderFlowDelete)` --> To be used when we make use of OAuth during regulation flow
`dest.GetAccountID(common.RudderFlowDelivery)` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationT) GetAccountID(flow common.RudderFlow) (string, error) {
	account := d.resolveAccount(flow)
	if account == nil {
		return "", fmt.Errorf("account not found")
	}
	return account.ID, nil
}

// IsOAuthDestination checks if a destination is configured for OAuth authentication.
// If the destination has an account with an account definition for the given flow,
// the account definition's AuthenticationType is used to determine if it is OAuth.
// Otherwise, it falls back to checking the destination definition config.
func (d *DestinationT) IsOAuthDestination(flow common.RudderFlow) (bool, error) {
	if account := d.resolveAccount(flow); account != nil && account.AccountDefinition != nil {
		return account.AccountDefinition.AuthenticationType == accountDefinitionOAuthType, nil
	}

	authValue, _ := misc.NestedMapLookup(d.DestinationDefinition.Config, "auth", "type")
	if authValue == nil {
		// valid use-case for non-OAuth destinations
		return false, nil
	}
	authType, ok := authValue.(string)
	if !ok {
		// we should throw error here, as we expect authValue to be a string if present
		return false, fmt.Errorf("auth type is not a string: %v", authValue)
	}
	isScopeSupported, err := isOAuthSupportedForFlow(d.DestinationDefinition.Config, flow)
	if err != nil {
		return false, err
	}
	return authType == destinationDefinitionOAuthType && isScopeSupported, nil
}

// resolveAccount resolves the account associated with the destination based on the flow.
// It returns the account for the specified flow (delivery or delete) if it exists, otherwise it returns nil.
func (d *DestinationT) resolveAccount(flow common.RudderFlow) *Account {
	switch flow {
	case common.RudderFlowDelivery:
		return d.DeliveryAccount
	case common.RudderFlowDelete:
		return d.DeleteAccount
	}
	return nil
}

func isOAuthSupportedForFlow(definitionConfig map[string]any, flow common.RudderFlow) (bool, error) {
	rudderScopesValue, _ := misc.NestedMapLookup(definitionConfig, "auth", "rudderScopes")
	if rudderScopesValue == nil {
		// valid use-case for non-OAuth destinations
		// when the auth.type is OAuth and rudderScopes is not mentioned, we would assume oauth flow is to be used when it is in "delivery" flow
		return flow == common.RudderFlowDelivery, nil
	}
	interfaceArr, ok := rudderScopesValue.([]any)
	if !ok {
		return false, fmt.Errorf("rudderScopes should be a []any but got %T", rudderScopesValue)
	}
	var rudderScopes []string
	for _, scopeInterface := range interfaceArr {
		scope, ok := scopeInterface.(string)
		if !ok {
			return false, fmt.Errorf("%v in auth.rudderScopes should be string but got %T", scopeInterface, scopeInterface)
		}
		rudderScopes = append(rudderScopes, scope)
	}
	return lo.Contains(rudderScopes, string(flow)), nil
}
