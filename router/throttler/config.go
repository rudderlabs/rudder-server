package throttler

var (
	destSettingsMap map[string]Settings
)

func init() {
	destSettingsMap = map[string]Settings{
		// https://customer.io/docs/api/#api-documentationlimits
		"CUSTOMERIO": {
			limit:               30,
			timeWindowInS:       1,
			userLevelThrottling: false,
		},
		// https://help.amplitude.com/hc/en-us/articles/360032842391-HTTP-API-V2#upload-limit
		"AM": {
			limit:                  1000,
			timeWindowInS:          1,
			userLevelThrottling:    true,
			userLevelLimit:         10,
			userLevelTimeWindowInS: 1,
		},
	}
}
