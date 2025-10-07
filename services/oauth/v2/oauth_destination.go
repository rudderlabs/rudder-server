package v2

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func IsOAuthDestination(definitionConfig map[string]any, flow common.RudderFlow) (bool, error) {
	authValue, _ := misc.NestedMapLookup(definitionConfig, "auth", "type")
	if authValue == nil {
		// valid use-case for non-OAuth destinations
		return false, nil
	}
	authType, ok := authValue.(string)
	if !ok {
		// we should throw error here, as we expect authValue to be a string if present
		return false, fmt.Errorf("auth type is not a string: %v", authValue)
	}
	isScopeSupported, err := isOAuthSupportedForFlow(definitionConfig, string(flow))
	if err != nil {
		return false, err
	}
	return authType == string(common.OAuth) && isScopeSupported, nil
}

func isOAuthSupportedForFlow(definitionConfig map[string]any, flow string) (bool, error) {
	rudderScopesValue, _ := misc.NestedMapLookup(definitionConfig, "auth", "rudderScopes")
	if rudderScopesValue == nil {
		// valid use-case for non-OAuth destinations
		// when the auth.type is OAuth and rudderScopes is not mentioned, we would assume oauth flow is to be used when it is in "delivery" flow
		return flow == string(common.RudderFlowDelivery), nil
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
	return lo.Contains(rudderScopes, flow), nil
}
