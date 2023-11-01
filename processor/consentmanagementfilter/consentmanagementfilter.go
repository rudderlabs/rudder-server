package consentmanagementfilter

import (
	jsoniter "github.com/json-iterator/go"
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
	consent string
}

type GenericConsentManagementProviderConfig struct {
	provider           string
	resolutionStrategy string
	consents           []GenericConsentsConfig
}

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

func GetConsentCategories(dest *backendconfig.DestinationT) []string {
	config := dest.Config
	cookieCategories, _ := misc.MapLookup(
		config,
		"oneTrustCookieCategories",
	).([]interface{})
	if len(cookieCategories) == 0 {
		return nil
	}
	return lo.FilterMap(
		cookieCategories,
		func(cookieCategory interface{}, _ int) (string, bool) {
			switch category := cookieCategory.(type) {
			case map[string]interface{}:
				cCategory, ok := category["oneTrustCookieCategory"].(string)
				return cCategory, ok && cCategory != ""
			default:
				return "", false
			}
		},
	)
}

func GetGenericConsentManagementData(dest *backendconfig.DestinationT) map[string]GenericConsentManagementProviderData {
	genericConsentManagementData := make(map[string]GenericConsentManagementProviderData)

	if _, ok := dest.Config["consentManagement"]; !ok {
		return genericConsentManagementData
	}

	consentManagementConfigStr, mErr := jsonfast.Marshal(dest.Config["consentManagement"])
	if mErr != nil {
		return genericConsentManagementData
	}

	consentManagementConfig := make([]GenericConsentManagementProviderConfig, 0)
	err := jsonfast.Unmarshal(consentManagementConfigStr, &consentManagementConfig)

	if err != nil || len(consentManagementConfig) == 0 {
		return genericConsentManagementData
	}

	for _, providerConfig := range consentManagementConfig {
		consentsConfig := providerConfig.consents

		if len(consentsConfig) > 0 && providerConfig.provider != "" {
			consentIds := lo.FilterMap(
				consentsConfig,
				func(consentsObj GenericConsentsConfig, _ int) (string, bool) {
					return consentsObj.consent, consentsObj.consent != ""
				},
			)

			if len(consentIds) > 0 {
				genericConsentManagementData[providerConfig.provider] = GenericConsentManagementProviderData{
					ResolutionStrategy: providerConfig.resolutionStrategy,
					Consents:           consentIds,
				}
			}
		}
	}

	return genericConsentManagementData
}

func getValidConsents(consents []string) []string {
	return lo.FilterMap(consents, func(consent string, _ int) (string, bool) {
		return consent, consent != ""
	})
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
	}

	consentManagementInfo.AllowedConsentIds = getValidConsents(consentManagementInfo.AllowedConsentIds)
	consentManagementInfo.DeniedConsentIds = getValidConsents(consentManagementInfo.DeniedConsentIds)

	return consentManagementInfo
}
