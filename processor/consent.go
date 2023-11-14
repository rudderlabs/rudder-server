package processor

import (
	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// filterDestinations filters destinations based on consent categories, supports oneTrustCookieCategories and ketchConsentPurposes
func (proc *Handle) filterDestinations(event types.SingularEventT, destinations []backendconfig.DestinationT) []backendconfig.DestinationT {
	// If there are no deniedConsentIds, then return all destinations
	deniedCategories := deniedConsentCategories(event)
	if len(deniedCategories) == 0 {
		return destinations
	}

	return lo.Filter(destinations, func(dest backendconfig.DestinationT, _ int) bool {
		// If the destination has oneTrustCookieCategories, returns false if any of the oneTrustCategories are present in deniedCategories
		if oneTrustCategories := proc.oneConsentCategories(dest.ID); len(oneTrustCategories) > 0 {
			return len(lo.Intersect(oneTrustCategories, deniedCategories)) == 0
		}

		// If the destination has ketchConsentPurposes, returns false if all ketchCategories are present in deniedCategories
		if ketchCategories := proc.ketchConsentCategories(dest.ID); len(ketchCategories) > 0 {
			return !lo.Every(deniedCategories, ketchCategories)
		}
		return true
	})
}

func (proc *Handle) oneConsentCategories(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.oneTrustConsentCategoriesMap[destinationID]
}

func (proc *Handle) ketchConsentCategories(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.ketchConsentCategoriesMap[destinationID]
}

func deniedConsentCategories(se types.SingularEventT) []string {
	if deniedConsents, _ := misc.MapLookup(se, "context", "consentManagement", "deniedConsentIds").([]interface{}); len(deniedConsents) > 0 {
		return lo.FilterMap(deniedConsents, func(consent interface{}, _ int) (string, bool) {
			consentStr, ok := consent.(string)
			return consentStr, ok && consentStr != ""
		})
	}
	return nil
}

func oneTrustConsentCategories(dest *backendconfig.DestinationT) []string {
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

func ketchConsentCategories(dest *backendconfig.DestinationT) []string {
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
