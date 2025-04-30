package processor

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type ConsentManagementInfo struct {
	AllowedConsentIDs  []string
	DeniedConsentIDs   []string
	Provider           string
	ResolutionStrategy string
}

type EventConsentManagementInfo struct {
	AllowedConsentIDs  interface{} `json:"allowedConsentIds"`
	DeniedConsentIDs   []string    `json:"deniedConsentIds"`
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
For GCM based filtering, uses source and destination IDs to fetch the appropriate GCM data from the config.
*/
func (proc *Handle) getConsentFilteredDestinations(event types.SingularEventT, sourceID string, destinations []backendconfig.DestinationT) []backendconfig.DestinationT {
	// If the event does not have denied consent IDs, do not filter any destinations
	consentManagementInfo, err := getConsentManagementInfo(event)
	if err != nil {
		// Log the error for debugging purposes
		proc.logger.Errorw("failed to get consent management info", "error", err.Error())
	}

	// If the event does not have any consents, do not filter any destinations
	if len(consentManagementInfo.AllowedConsentIDs) == 0 && len(consentManagementInfo.DeniedConsentIDs) == 0 {
		return destinations
	}

	return lo.Filter(destinations, func(dest backendconfig.DestinationT, _ int) bool {
		// Generic consent management
		if cmpData := proc.getGCMData(sourceID, dest.ID, consentManagementInfo.Provider); len(cmpData.Consents) > 0 {
			finalResolutionStrategy := consentManagementInfo.ResolutionStrategy

			// For custom provider, the resolution strategy is to be picked from the destination config
			if consentManagementInfo.Provider == "custom" {
				finalResolutionStrategy = cmpData.ResolutionStrategy
			}

			// TODO: Remove "or" and "and" support once all the SDK clients stop sending it.
			// Currently, it is added for backward compatibility.
			switch finalResolutionStrategy {
			case "any", "or":
				if len(consentManagementInfo.AllowedConsentIDs) > 0 {
					// The user must consent to at least one of the configured consents in the destination
					return lo.Some(consentManagementInfo.AllowedConsentIDs, cmpData.Consents) || len(cmpData.Consents) == 0
				}
				// All of the configured consents should not be in denied
				return !lo.Every(consentManagementInfo.DeniedConsentIDs, cmpData.Consents)

			default: // "all" / "and"
				if len(consentManagementInfo.AllowedConsentIDs) > 0 {
					// The user must consent to all of the configured consents in the destination
					return lo.Every(consentManagementInfo.AllowedConsentIDs, cmpData.Consents)
				}
				// None of the configured consents should be in denied
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

func (proc *Handle) getGCMData(sourceID, destinationID, provider string) GenericConsentManagementProviderData {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()

	defRetVal := GenericConsentManagementProviderData{}
	destinationData, ok := proc.config.genericConsentManagementMap[SourceID(sourceID)][DestinationID(destinationID)]
	if !ok {
		return defRetVal
	}

	providerData, ok := destinationData[ConsentProviderKey(provider)]
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

func getGenericConsentManagementData(dest *backendconfig.DestinationT) (ConsentProviderMap, error) {
	genericConsentManagementData := make(ConsentProviderMap)

	if _, ok := dest.Config["consentManagement"]; !ok {
		return genericConsentManagementData, nil
	}

	consentManagementConfigBytes, mErr := jsonrs.Marshal(dest.Config["consentManagement"])
	if mErr != nil {
		return genericConsentManagementData, fmt.Errorf("error marshalling consentManagement: %v for destination ID: %s", mErr, dest.ID)
	}

	consentManagementConfig := make([]GenericConsentManagementProviderConfig, 0)
	unmErr := jsonrs.Unmarshal(consentManagementConfigBytes, &consentManagementConfig)

	if unmErr != nil {
		return genericConsentManagementData, fmt.Errorf("error unmarshalling consentManagementConfig: %v for destination ID: %s", unmErr, dest.ID)
	}

	for _, providerConfig := range consentManagementConfig {
		consentsConfig := providerConfig.Consents

		if len(consentsConfig) > 0 && providerConfig.Provider != "" {
			consentIDs := lo.Map(consentsConfig, func(consentsObj GenericConsentsConfig, _ int) string {
				return strings.TrimSpace(consentsObj.Consent)
			})

			consentIDs = lo.FilterMap(
				consentIDs,
				func(consentID string, _ int) (string, bool) {
					return consentID, consentID != ""
				},
			)

			if len(consentIDs) > 0 {
				genericConsentManagementData[ConsentProviderKey(providerConfig.Provider)] = GenericConsentManagementProviderData{
					ResolutionStrategy: providerConfig.ResolutionStrategy,
					Consents:           consentIDs,
				}
			}
		}
	}

	return genericConsentManagementData, nil
}

func getConsentManagementInfo(event types.SingularEventT) (ConsentManagementInfo, error) {
	consentManagementInfoFromEvent := EventConsentManagementInfo{}
	consentManagementInfo := ConsentManagementInfo{}
	if consentManagement, ok := misc.MapLookup(event, "context", "consentManagement").(map[string]interface{}); ok {
		consentManagementObjBytes, mErr := jsonrs.Marshal(consentManagement)
		if mErr != nil {
			return consentManagementInfo, fmt.Errorf("error marshalling consentManagement: %v", mErr)
		}

		unmErr := jsonrs.Unmarshal(consentManagementObjBytes, &consentManagementInfoFromEvent)
		if unmErr != nil {
			return consentManagementInfo, fmt.Errorf("error unmarshalling consentManagementInfo: %v", unmErr)
		}

		consentManagementInfo.DeniedConsentIDs = consentManagementInfoFromEvent.DeniedConsentIDs
		consentManagementInfo.Provider = consentManagementInfoFromEvent.Provider
		consentManagementInfo.ResolutionStrategy = consentManagementInfoFromEvent.ResolutionStrategy

		// This is to support really old version of the JS SDK v3 that sent this data as an object
		// for OneTrust provider.
		// Handle AllowedConsentIDs based on its type (array or map)
		switch val := consentManagementInfoFromEvent.AllowedConsentIDs.(type) {
		case []interface{}:
			// Convert []interface{} to []string
			consentManagementInfo.AllowedConsentIDs = make([]string, 0, len(val))
			for _, v := range val {
				if strVal, ok := v.(string); ok {
					consentManagementInfo.AllowedConsentIDs = append(consentManagementInfo.AllowedConsentIDs, strVal)
				}
			}
		case []string:
			// Already a string array
			consentManagementInfo.AllowedConsentIDs = val
		case map[string]interface{}:
			// Use keys from the map (legacy OneTrust format)
			consentManagementInfo.AllowedConsentIDs = lo.Keys(val)
		default:
			consentManagementInfo.AllowedConsentIDs = []string{}
		}

		// Ideally, the clean up and filter is not needed for standard providers
		// but useful for custom providers where users send this data directly
		// to the SDKs.
		sanitizePredicate := func(consent string, _ int) string {
			return strings.TrimSpace(consent)
		}

		consentManagementInfo.AllowedConsentIDs = lo.Map(consentManagementInfo.AllowedConsentIDs, sanitizePredicate)
		consentManagementInfo.DeniedConsentIDs = lo.Map(consentManagementInfo.DeniedConsentIDs, sanitizePredicate)

		// Filter out empty values
		filterPredicate := func(consent string, _ int) (string, bool) {
			return consent, consent != ""
		}
		consentManagementInfo.AllowedConsentIDs = lo.FilterMap(consentManagementInfo.AllowedConsentIDs, filterPredicate)
		consentManagementInfo.DeniedConsentIDs = lo.FilterMap(consentManagementInfo.DeniedConsentIDs, filterPredicate)
	}

	return consentManagementInfo, nil
}
