package processor

import (
	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type ConsentManagementInfo struct {
	DeniedConsentIds   []string `json:"deniedConsentIds"`
	AllowedConsentIds  []string `json:"allowedConsentIds"`
	Provider           string   `json:"provider"`
	ResolutionStrategy string   `json:"resolutionStrategy"`
}

type GenericConsentManagementProviderData struct {
	ResolutionStrategy string
	Consents           []string
}

type GenericConsentsConfig struct {
	Consent string `json:"consent"`
}

type GenericConsentManagementProviderConfig struct {
	Provider           string                  `json:"provider"`
	ResolutionStrategy string                  `json:"resolutionStrategy"`
	Consents           []GenericConsentsConfig `json:"consents"`
}

// filterDestinations filters destinations based on consent categories, supports oneTrustCookieCategories and ketchConsentPurposes
func (proc *Handle) filterDestinations(event types.SingularEventT, destinations []backendconfig.DestinationT) []backendconfig.DestinationT {
	// If there are no deniedConsentIds, then return all destinations
	consentManagementInfo := GetConsentManagementInfo(event)
	if len(consentManagementInfo.DeniedConsentIds) == 0 {
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
					return !lo.Every(consentManagementInfo.DeniedConsentIds, cmpData.Consents)
				default: // "and"
					return len(lo.Intersect(cmpData.Consents, consentManagementInfo.DeniedConsentIds)) == 0
				}
			}
		} else {
			// Legacy consent management
			// If the destination has oneTrustCookieCategories, returns false if any of the oneTrustCategories are present in deniedCategories
			if oneTrustCategories := proc.getOneTrustConsentData(dest.ID); len(oneTrustCategories) > 0 {
				return len(lo.Intersect(oneTrustCategories, consentManagementInfo.DeniedConsentIds)) == 0
			}

			// If the destination has ketchConsentPurposes, returns false if all ketchCategories are present in deniedCategories
			if ketchCategories := proc.getKetchConsentData(dest.ID); len(ketchCategories) > 0 {
				return !lo.Every(consentManagementInfo.DeniedConsentIds, ketchCategories)
			}
		}
		return true
	})
}

func (proc *Handle) getOneTrustConsentData(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.oneTrustConsentCategoriesMap[destinationID]
}

func (proc *Handle) getKetchConsentData(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.ketchConsentCategoriesMap[destinationID]
}

// returns the consent management data for a destination and provider
func (proc *Handle) getGCMData(destinationID, provider string) GenericConsentManagementProviderData {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()

	defRetVal := GenericConsentManagementProviderData{}
	destinationData, ok := proc.config.destGenericConsentManagementMap[destinationID]
	if !ok {
		return defRetVal
	}

	providerData, ok := destinationData[provider]
	if !ok {
		return defRetVal
	}

	return providerData
}

func GetOneTrustConsentCategories(dest *backendconfig.DestinationT) []string {
	cookieCategories, _ := misc.MapLookup(dest.Config, "oneTrustCookieCategories").([]interface{})
	if len(cookieCategories) == 0 {
		return nil
	}
	return lo.FilterMap(cookieCategories, func(cookieCategory interface{}, _ int) (string, bool) {
		switch category := cookieCategory.(type) {
		case map[string]interface{}:
			cCategory, ok := category["oneTrustCookieCategory"].(string)
			return cCategory, ok && cCategory != ""
		default:
			return "", false
		}
	})
}

func GetKetchConsentCategories(dest *backendconfig.DestinationT) []string {
	consentPurposes, _ := misc.MapLookup(dest.Config, "ketchConsentPurposes").([]interface{})
	if len(consentPurposes) == 0 {
		return nil
	}
	return lo.FilterMap(consentPurposes, func(consentPurpose interface{}, _ int) (string, bool) {
		switch t := consentPurpose.(type) {
		case map[string]interface{}:
			purpose, ok := t["purpose"].(string)
			return purpose, ok && purpose != ""
		default:
			return "", false
		}
	})
}

func GetGenericConsentManagementData(dest *backendconfig.DestinationT) map[string]GenericConsentManagementProviderData {
	genericConsentManagementData := make(map[string]GenericConsentManagementProviderData)

	if _, ok := dest.Config["consentManagement"]; !ok {
		return genericConsentManagementData
	}

	consentManagementConfigBytes, mErr := jsonfast.Marshal(dest.Config["consentManagement"])
	if mErr != nil {
		return genericConsentManagementData
	}

	consentManagementConfig := make([]GenericConsentManagementProviderConfig, 0)
	err := jsonfast.Unmarshal(consentManagementConfigBytes, &consentManagementConfig)

	if err != nil || len(consentManagementConfig) == 0 {
		return genericConsentManagementData
	}

	for _, providerConfig := range consentManagementConfig {
		consentsConfig := providerConfig.Consents

		if len(consentsConfig) > 0 && providerConfig.Provider != "" {
			consentIds := lo.FilterMap(
				consentsConfig,
				func(consentsObj GenericConsentsConfig, _ int) (string, bool) {
					return consentsObj.Consent, consentsObj.Consent != ""
				},
			)

			if len(consentIds) > 0 {
				genericConsentManagementData[providerConfig.Provider] = GenericConsentManagementProviderData{
					ResolutionStrategy: providerConfig.ResolutionStrategy,
					Consents:           consentIds,
				}
			}
		}
	}

	return genericConsentManagementData
}

func GetConsentManagementInfo(se types.SingularEventT) ConsentManagementInfo {
	consentManagementInfo := ConsentManagementInfo{}
	if consentManagement, ok := misc.MapLookup(se, "context", "consentManagement").(map[string]interface{}); ok {
		consentManagementStr, mErr := jsonfast.Marshal(consentManagement)
		if mErr != nil {
			return consentManagementInfo
		}

		err := jsonfast.Unmarshal(consentManagementStr, &consentManagementInfo)
		if err != nil {
			return consentManagementInfo
		}

		filterPredicate := func(consent string, _ int) (string, bool) {
			return consent, consent != ""
		}

		consentManagementInfo.AllowedConsentIds = lo.FilterMap(consentManagementInfo.AllowedConsentIds, filterPredicate)
		consentManagementInfo.DeniedConsentIds = lo.FilterMap(consentManagementInfo.DeniedConsentIds, filterPredicate)
	}

	return consentManagementInfo
}
