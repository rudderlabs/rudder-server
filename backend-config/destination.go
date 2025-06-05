package backendconfig

import (
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/backend-config/destination"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// DestinationCache is a map-based implementation of DynamicConfigCache.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
type DestinationCache map[string]*destination.RevisionInfo

// Get retrieves a cache entry for a destination ID.
func (c DestinationCache) Get(destID string) (*destination.RevisionInfo, bool) {
	info, exists := c[destID]
	return info, exists
}

// Set stores a cache entry for a destination ID.
func (c DestinationCache) Set(destID string, info *destination.RevisionInfo) {
	c[destID] = info
}

// Len returns the number of entries in the cache.
func (c DestinationCache) Len() int {
	return len(c)
}

// ProcessDestinationsInSources processes all destinations in all sources
// and updates their HasDynamicConfig flag using the provided cache.
// This utility function is used to avoid code duplication across different parts of the codebase.
//
// Parameters:
//   - sources: A slice of SourceT containing destinations to process
//   - cache: A destination.Cache implementation to store and retrieve dynamic config information
//
// Usage example:
//
//	ProcessDestinationsInSources(workspace.Sources, dynamicConfigCache)
func ProcessDestinationsInSources(sources []SourceT, cache destination.Cache) {
	for i := range sources {
		for j := range sources[i].Destinations {
			dest := &sources[i].Destinations[j]
			dest.UpdateDerivedFields(cache)
		}
	}
}

func isOAuthByAccountDefinition(accountDefintion AccountDefinition) bool {
	refreshOAuthToken, ok := accountDefintion.Config["refreshOAuthToken"].(bool)
	return ok && refreshOAuthToken
}

func isOAuthByDestinationDefinition(destDef DestinationDefinitionT, flow common.RudderFlow) bool {
	authValue, ok := destDef.Config["auth"].(map[string]interface{})
	if !ok {
		return false
	}
	if authValue["type"] != common.OAuth {
		return false
	}
	// Validate that rudderScopes contains only valid string values
	if authScopes, ok := authValue["rudderScopes"].([]interface{}); ok {
		rudderScopes := misc.ConvertInterfaceToStringArray(authScopes)
		return lo.Contains(rudderScopes, string(flow))
	}
	return true
}

func (d *DestinationT) SetOAuthFlags() {
	if d.DeliveryAccount != nil && d.DeliveryAccount.AccountDefinition != nil {
		d.DeliveryByOAuth = isOAuthByAccountDefinition(*d.DeliveryAccount.AccountDefinition)
	} else {
		d.DeliveryByOAuth = isOAuthByDestinationDefinition(d.DestinationDefinition, common.RudderFlowDelivery)
	}
	if d.DeleteAccount != nil && d.DeleteAccount.AccountDefinition != nil {
		d.DeleteByOAuth = isOAuthByAccountDefinition(*d.DeleteAccount.AccountDefinition)
	} else {
		d.DeleteByOAuth = isOAuthByDestinationDefinition(d.DestinationDefinition, common.RudderFlowDelete)
	}
}

func (d *DestinationT) SetDynamicConfigFlags() {
	d.HasDynamicConfig = destination.ContainsDynamicConfigPattern(d.Config)
}
