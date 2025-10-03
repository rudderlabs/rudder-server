package v2

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type DestinationInfo struct {
	WorkspaceID      string
	DestType         string
	DefinitionConfig map[string]any
	ID               string
	Config           map[string]any
}

/*
GetAccountID Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`GetAccountID(common.RudderFlowDelete)` --> To be used when we make use of OAuth during regulation flow
`GetAccountID(common.RudderFlowDelivery)` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationInfo) GetAccountID(flow common.RudderFlow) (string, error) {
	oauthDest, err := IsOAuthDestination(d.DefinitionConfig, flow)
	if err != nil {
		return "", fmt.Errorf("failed to check if destination is oauth destination: %v", err)
	}

	idKey := common.DeliveryAccountIDKey
	if flow == common.RudderFlowDelete {
		idKey = common.DeleteAccountIDKey
	}
	rudderAccountIdInterface, found := d.Config[idKey]
	if !oauthDest || !found || idKey == "" {
		return "", fmt.Errorf("destination is not an oauth destination or accountId not found")
	}
	rudderAccountId, ok := rudderAccountIdInterface.(string)
	if !ok {
		return "", fmt.Errorf("rudderAccountId is not a string")
	}
	return rudderAccountId, nil
}
