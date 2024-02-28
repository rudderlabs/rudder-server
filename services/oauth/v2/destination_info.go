package v2

import "github.com/rudderlabs/rudder-server/utils/misc"

type DestinationInfo struct {
	WorkspaceID   string
	DestDefName   string
	DestDefConfig map[string]interface{}
	DestinationId string
	DestConfig    map[string]interface{}
}

func (d *DestinationInfo) IsOAuthDestination() bool {
	authValue, err := misc.NestedMapLookup(d.DestDefConfig, "auth", "type")
	if err != nil {
		return false
	}
	if authType, ok := authValue.(string); ok {
		return authType == "OAuth"
	}
	return false
}

/*
Gets AccountId for OAuth destination based on if rudderFlow is `Delivery` or `Delete`

Example:
`GetAccountId(destDetail.Config, "rudderDeleteAccountId")` --> To be used when we make use of OAuth during regulation flow
`GetAccountId(destDetail.Config, "rudderAccountId")` --> To be used when we make use of OAuth during normal event delivery
*/
func (d *DestinationInfo) GetAccountID(idKey string) string {
	rudderAccountIdInterface, found := d.DestConfig[idKey]
	if !d.IsOAuthDestination() || !found || idKey == "" {
		return ""
	}
	rudderAccountId, _ := rudderAccountIdInterface.(string)
	return rudderAccountId
}
