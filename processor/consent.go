package processor

import (
	"slices"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/consentmanagementfilter"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// filterDestinations filters destinations based on consent categories, supports oneTrustCookieCategories and ketchConsentPurposes
func (proc *Handle) filterDestinations(event types.SingularEventT, destinations []backendconfig.DestinationT) []backendconfig.DestinationT {
	// If there are no deniedConsentIds, then return all destinations
	consentManagementInfo := consentmanagementfilter.GetConsentManagementInfo(event)
	if len(consentManagementInfo.DeniedConsentIds) == 0 && len(consentManagementInfo.AllowedConsentIds) == 0 {
		return destinations
	}

	return lo.Filter(destinations, func(dest backendconfig.DestinationT, _ int) bool {
		// This field differentiates legacy and generic consent management
		if consentManagementInfo.Provider != "" {
			if cmpData := proc.getGCMData(dest.ID, consentManagementInfo.Provider); len(cmpData.Consents) > 0 {

				finalResolutionStrategy := consentManagementInfo.ResolutionStrategy
				if consentManagementInfo.Provider == "custom" {
					finalResolutionStrategy = cmpData.ResolutionStrategy
				}

				switch finalResolutionStrategy {
				case "or":
					return len(lo.Intersect(cmpData.Consents, consentManagementInfo.AllowedConsentIds)) > 0
				default: // "and"
					return slices.Equal(cmpData.Consents, lo.Intersect(cmpData.Consents, consentManagementInfo.AllowedConsentIds))
				}
			}
		} else {
			// Legacy consent management
			// If the destination has oneTrustCookieCategories, returns false if any of the oneTrustCategories are present in deniedCategories
			if oneTrustCategories := proc.GetOneTrustConsentData(dest.ID); len(oneTrustCategories) > 0 {
				return len(lo.Intersect(oneTrustCategories, consentManagementInfo.DeniedConsentIds)) == 0
			}

			// If the destination has ketchConsentPurposes, returns false if all ketchCategories are present in deniedCategories
			if ketchCategories := proc.GetKetchConsentData(dest.ID); len(ketchCategories) > 0 {
				return !lo.Every(consentManagementInfo.DeniedConsentIds, ketchCategories)
			}
		}
		return true
	})
}

// returns the consent management data for a destination and provider
func (proc *Handle) getGCMData(destinationID, provider string) consentmanagementfilter.GenericConsentManagementProviderData {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()

	defRetVal := consentmanagementfilter.GenericConsentManagementProviderData{}
	destinationData, ok := proc.config.destGenericConsentManagementData[destinationID]
	if !ok {
		return defRetVal
	}

	providerData, ok := destinationData[provider]
	if !ok {
		return defRetVal
	}

	return providerData
}

func (proc *Handle) GetOneTrustConsentData(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.oneTrustConsentCategoriesMap[destinationID]
}

func (proc *Handle) GetKetchConsentData(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.ketchConsentCategoriesMap[destinationID]
}
