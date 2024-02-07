package processor

import (
	"fmt"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type ConsentManagementInfo struct {
	DeniedConsentIDs   []string    `json:"deniedConsentIds"`
	AllowedConsentIDs  interface{} `json:"allowedConsentIds"` // Not used currently but added for future use
	Provider           string      `json:"provider"`
	ResolutionStrategy string      `json:"resolutionStrategy"`
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

/*
Filters and returns destinations based on the consents configured for the destination and the user consents present in the event.

Supports legacy and generic consent management.
*/
func (proc *Handle) getConsentFilteredDestinations(event types.SingularEventT, destinations []backendconfig.DestinationT) []backendconfig.DestinationT {
	// If the event does not have denied consent IDs, do not filter any destinations
	consentManagementInfo, err := getConsentManagementInfo(event)
	if err != nil {
		// Log the error for debugging purposes
		proc.logger.Errorw("failed to get consent management info", "error", err.Error())
	}

	if len(consentManagementInfo.DeniedConsentIDs) == 0 {
		return destinations
	}

	return lo.Filter(destinations, func(dest backendconfig.DestinationT, _ int) bool {
		// Generic consent management
		if cmpData := proc.getGCMData(dest.ID, consentManagementInfo.Provider); len(cmpData.Consents) > 0 {

			finalResolutionStrategy := consentManagementInfo.ResolutionStrategy

			// For custom provider, the resolution strategy is to be picked from the destination config
			if consentManagementInfo.Provider == "custom" {
				finalResolutionStrategy = cmpData.ResolutionStrategy
			}

			switch finalResolutionStrategy {
			// The user must consent to at least one of the configured consents in the destination
			case "or":
				return !lo.Every(consentManagementInfo.DeniedConsentIDs, cmpData.Consents)

			// The user must consent to all of the configured consents in the destination
			default: // "and"
				return len(lo.Intersect(cmpData.Consents, consentManagementInfo.DeniedConsentIDs)) == 0
			}
		}

		// Legacy consent management
		if consentManagementInfo.Provider == "" || consentManagementInfo.Provider == "oneTrust" {
			// If the destination has oneTrustCookieCategories, returns false if any of the oneTrustCategories are present in deniedCategories
			if oneTrustCategories := proc.getOneTrustConsentData(dest.ID); len(oneTrustCategories) > 0 {
				return len(lo.Intersect(oneTrustCategories, consentManagementInfo.DeniedConsentIDs)) == 0
			}
		}

		if consentManagementInfo.Provider == "" || consentManagementInfo.Provider == "ketch" {
			// If the destination has ketchConsentPurposes, returns false if all ketchPurposes are present in deniedCategories
			if ketchPurposes := proc.getKetchConsentData(dest.ID); len(ketchPurposes) > 0 {
				return !lo.Every(consentManagementInfo.DeniedConsentIDs, ketchPurposes)
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

func getOneTrustConsentCategories(dest *backendconfig.DestinationT) []string {
	cookieCategories, ok := misc.MapLookup(dest.Config, "oneTrustCookieCategories").([]interface{})
	if !ok {
		// Handle the case where oneTrustCookieCategories is not a slice
		return nil
	}
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

func getKetchConsentCategories(dest *backendconfig.DestinationT) []string {
	consentPurposes, ok := misc.MapLookup(dest.Config, "ketchConsentPurposes").([]interface{})
	if !ok {
		// Handle the case where ketchConsentPurposes is not a slice
		return nil
	}
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

func getGenericConsentManagementData(dest *backendconfig.DestinationT) (map[string]GenericConsentManagementProviderData, error) {
	genericConsentManagementData := make(map[string]GenericConsentManagementProviderData)

	if _, ok := dest.Config["consentManagement"]; !ok {
		return genericConsentManagementData, nil
	}

	consentManagementConfigBytes, mErr := jsonfast.Marshal(dest.Config["consentManagement"])
	if mErr != nil {
		return genericConsentManagementData, fmt.Errorf("error marshalling consentManagement: %v for destination ID: %s", mErr, dest.ID)
	}

	consentManagementConfig := make([]GenericConsentManagementProviderConfig, 0)
	unmErr := jsonfast.Unmarshal(consentManagementConfigBytes, &consentManagementConfig)

	if unmErr != nil {
		return genericConsentManagementData, fmt.Errorf("error unmarshalling consentManagementConfig: %v for destination ID: %s", unmErr, dest.ID)
	}

	for _, providerConfig := range consentManagementConfig {
		consentsConfig := providerConfig.Consents

		if len(consentsConfig) > 0 && providerConfig.Provider != "" {
			consentIDs := lo.FilterMap(
				consentsConfig,
				func(consentsObj GenericConsentsConfig, _ int) (string, bool) {
					return consentsObj.Consent, consentsObj.Consent != ""
				},
			)

			if len(consentIDs) > 0 {
				genericConsentManagementData[providerConfig.Provider] = GenericConsentManagementProviderData{
					ResolutionStrategy: providerConfig.ResolutionStrategy,
					Consents:           consentIDs,
				}
			}
		}
	}

	return genericConsentManagementData, nil
}

func getConsentManagementInfo(event types.SingularEventT) (ConsentManagementInfo, error) {
	consentManagementInfo := ConsentManagementInfo{}
	if consentManagement, ok := misc.MapLookup(event, "context", "consentManagement").(map[string]interface{}); ok {
		consentManagementObjBytes, mErr := jsonfast.Marshal(consentManagement)
		if mErr != nil {
			return consentManagementInfo, fmt.Errorf("error marshalling consentManagement: %v", mErr)
		}

		unmErr := jsonfast.Unmarshal(consentManagementObjBytes, &consentManagementInfo)
		if unmErr != nil {
			return consentManagementInfo, fmt.Errorf("error unmarshalling consentManagementInfo: %v", unmErr)
		}

		filterPredicate := func(consent string, _ int) (string, bool) {
			return consent, consent != ""
		}

		consentManagementInfo.DeniedConsentIDs = lo.FilterMap(consentManagementInfo.DeniedConsentIDs, filterPredicate)
	}

	return consentManagementInfo, nil
}
