package processor

import (
	"testing"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestGetOneTrustConsentCategories(t *testing.T) {
	testCases := []struct {
		description string
		dest        *backendconfig.DestinationT
		expected    []string
	}{
		{
			description: "no oneTrustCookieCategories",
			dest:        &backendconfig.DestinationT{},
			expected:    nil,
		},
		{
			description: "empty oneTrustCookieCategories",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"oneTrustCookieCategories": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			description: "some oneTrustCookieCategories",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"oneTrustCookieCategories": []interface{}{
						map[string]interface{}{
							"oneTrustCookieCategory": "foo",
						},
						map[string]interface{}{
							"oneTrustCookieCategory": 123,
						},
						123,
					},
				},
			},
			expected: []string{"foo"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			require.EqualValues(t, tc.expected, GetOneTrustConsentCategories(tc.dest))
		})
	}
}

func TestGetKetchConsentCategories(t *testing.T) {
	testCases := []struct {
		description string
		dest        *backendconfig.DestinationT
		expected    []string
	}{
		{
			description: "no ketchConsentPurposes",
			dest:        &backendconfig.DestinationT{},
			expected:    nil,
		},
		{
			description: "empty ketchConsentPurposes",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"ketchConsentPurposes": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			description: "some ketchConsentPurposes",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"ketchConsentPurposes": []interface{}{
						map[string]interface{}{
							"purpose": "foo",
						},
						map[string]interface{}{
							"purpose": 123,
						},
						123,
					},
				},
			},
			expected: []string{"foo"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			require.EqualValues(t, tc.expected, GetKetchConsentCategories(tc.dest))
		})
	}
}

func TestFilterDestinations(t *testing.T) {
	testCases := []struct {
		description     string
		event           types.SingularEventT
		destinations    []backendconfig.DestinationT
		expectedDestIDs []string
	}{
		{
			description: "no denied consent categories",
			event:       types.SingularEventT{},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo",
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1"},
		},
		{
			description: "filter out destination with oneTrustCookieCategories",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-4",
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-4",
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-2",
							},
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-3",
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-1",
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "filter out destination with ketchConsentPurposes",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{
								"purpose": "foo-4",
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{
								"purpose": "foo-1",
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{
								"purpose": "foo-1",
							},
							map[string]interface{}{
								"purpose": "foo-4",
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{
								"purpose": "foo-1",
							},
							map[string]interface{}{
								"purpose": "foo-2",
							},
							map[string]interface{}{
								"purpose": "foo-3",
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{
								"purpose": "foo-1",
							},
							map[string]interface{}{
								"purpose": "foo-1",
							},
							map[string]interface{}{
								"purpose": "foo-1",
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "filter out destination with generic consent management (Ketch)",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or",
						"deniedConsentIds":   []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-2"},
									{"consent": "foo-3"},
								},
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-1"},
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "filter out destination with generic consent management (OneTrust)",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"deniedConsentIds":   []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-2"},
									{"consent": "foo-3"},
								},
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-1"},
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "filter out destination with generic consent management (Custom - AND)",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":         "custom",
						"deniedConsentIds": []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-2"},
									{"consent": "foo-3"},
								},
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-1"},
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "filter out destination with generic consent management (Custom - OR)",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "custom",
						"resolutionStrategy": "or",
						"deniedConsentIds":   []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID: "destID-1",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
					},
				},
				{
					ID: "destID-2",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
				{
					ID: "destID-4",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-4"},
								},
							},
						},
					},
				},
				{
					ID: "destID-5",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-2"},
									{"consent": "foo-3"},
								},
							},
						},
					},
				},
				{
					ID: "destID-6",
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "foo-1"},
									{"consent": "foo-1"},
									{"consent": "foo-1"},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "test onetrust with denied consents",
			event: types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"allowedConsentIds":  []interface{}{"consent category 1"},
						"deniedConsentIds":   []interface{}{"consent category 2", "someOtherCategory"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID:                 "dest-id-6",
					Name:               "D6",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 4"},
									{"consent": "purpose 2"},
								},
							},
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 1"},
									{"consent": "custom consent category 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-7",
					Name:               "D7",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "consent category 1"},
						},
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 1"},
									{"consent": "consent category 2"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-8",
					Name:               "D8",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-9",
					Name:               "D9",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-10",
					Name:               "D10",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-11",
					Name:               "D11",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-12",
					Name:               "D12",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-13",
					Name:               "D13",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "custom",
								"consents": []map[string]interface{}{
									{"consent": "someOtherCategory"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-14",
					Name:               "D14",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 3"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
			expectedDestIDs: []string{"dest-id-8", "dest-id-9", "dest-id-10", "dest-id-11", "dest-id-12", "dest-id-13", "dest-id-14"},
		},
		{
			description: "test custom with denied consents",
			event: types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "custom",
						"allowedConsentIds":  []interface{}{"consent category 1", "consent category 2"},
						"deniedConsentIds":   []interface{}{"someOtherCategory", "consent category 3"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID:                 "dest-id-6",
					Name:               "D6",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 4"},
									{"consent": "purpose 2"},
								},
							},
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 1"},
									{"consent": "custom consent category 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-7",
					Name:               "D7",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "consent category 1"},
						},
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 1"},
									{"consent": "consent category 2"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-8",
					Name:               "D8",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-9",
					Name:               "D9",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-10",
					Name:               "D10",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-11",
					Name:               "D11",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-12",
					Name:               "D12",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-13",
					Name:               "D13",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "custom",
								"consents": []map[string]interface{}{
									{"consent": "someOtherCategory"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-14",
					Name:               "D14",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 3"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
			expectedDestIDs: []string{"dest-id-6", "dest-id-7", "dest-id-8", "dest-id-9", "dest-id-10", "dest-id-11", "dest-id-12", "dest-id-14"},
		},
		{
			description: "test ketch with denied consents",
			event: types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or",
						"allowedConsentIds":  []interface{}{"consent category 2"},
						"deniedConsentIds":   []interface{}{"purpose 1", "purpose 3"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			},
			destinations: []backendconfig.DestinationT{
				{
					ID:                 "dest-id-6",
					Name:               "D6",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 4"},
									{"consent": "purpose 2"},
								},
							},
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 1"},
									{"consent": "custom consent category 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-7",
					Name:               "D7",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "consent category 1"},
						},
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 1"},
									{"consent": "consent category 2"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-8",
					Name:               "D8",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-9",
					Name:               "D9",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-10",
					Name:               "D10",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-11",
					Name:               "D11",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-12",
					Name:               "D12",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-13",
					Name:               "D13",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "custom",
								"consents": []map[string]interface{}{
									{"consent": "someOtherCategory"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-14",
					Name:               "D14",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 3"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
			expectedDestIDs: []string{"dest-id-6", "dest-id-8", "dest-id-9", "dest-id-10", "dest-id-11", "dest-id-12", "dest-id-13", "dest-id-14"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			proc := &Handle{}
			proc.config.oneTrustConsentCategoriesMap = make(map[string][]string)
			proc.config.ketchConsentCategoriesMap = make(map[string][]string)
			proc.config.destGenericConsentManagementMap = make(map[string]map[string]GenericConsentManagementProviderData)

			for _, dest := range tc.destinations {
				proc.config.oneTrustConsentCategoriesMap[dest.ID] = GetOneTrustConsentCategories(&dest)
				proc.config.ketchConsentCategoriesMap[dest.ID] = GetKetchConsentCategories(&dest)
				proc.config.destGenericConsentManagementMap[dest.ID] = GetGenericConsentManagementData(&dest)
			}

			filteredDestinations := proc.filterDestinations(tc.event, tc.destinations)

			require.EqualValues(t, tc.expectedDestIDs, lo.Map(filteredDestinations, func(dest backendconfig.DestinationT, _ int) string {
				return dest.ID
			}))
		})
	}
}

func TestGetConsentManagementInfo(t *testing.T) {
	type testCaseT struct {
		description string
		input       types.SingularEventT
		expected    ConsentManagementInfo
	}

	defConsentManagementInfo := ConsentManagementInfo{}
	testCases := []testCaseT{
		{
			description: "should return empty consent management info when no context is sent",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return empty consent management info when no consent management data is sent",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return default values for allowed consent IDs when it is missing in the event",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider": "custom",
						"allowedConsentIds": []string{
							"consent category 1",
							"consent category 2",
						},
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider: "custom",
				AllowedConsentIds: []string{
					"consent category 1",
					"consent category 2",
				},
				DeniedConsentIds:   []string{},
				ResolutionStrategy: "",
			},
		},
		{
			description: "should return default values for denied consent IDs when it is missing in the event",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider": "custom",
						"deniedConsentIds": []string{
							"consent category 1",
							"consent category 2",
						},
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider: "custom",
				DeniedConsentIds: []string{
					"consent category 1",
					"consent category 2",
				},
				AllowedConsentIds:  []string{},
				ResolutionStrategy: "",
			},
		},
		{
			description: "should return default values for allowed consent IDs when it is malformed",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":          "custom",
						"allowedConsentIds": "dummy",
					},
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return default values for denied consent IDs when it is malformed",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":         "custom",
						"deniedConsentIds": "dummy",
					},
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return empty consent management info when consent management data is not a valid object",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": "not an object",
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return empty consent management info when consent management data is malformed",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider": make(chan int),
					},
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return empty consent management info when consent management data is not as per the expected structure",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"allowedConsentIds": map[string]interface{}{ // this should have been an array
							"consent": "consent category 1",
						},
					},
				},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return consent management info when consent management data is sent correctly",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider": "custom",
						"allowedConsentIds": []string{
							"consent category 1",
							"consent category 2",
							"",
							"",
						},
						"deniedConsentIds": []string{
							"consent category 3",
							"",
							"consent category 4",
							"",
						},
						"resolutionStrategy": "and",
						"extra":              "extra field",
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider: "custom",
				AllowedConsentIds: []string{
					"consent category 1",
					"consent category 2",
				},
				DeniedConsentIds: []string{
					"consent category 3",
					"consent category 4",
				},
				ResolutionStrategy: "and",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			actual := GetConsentManagementInfo(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestGetGenericConsentManagementData(t *testing.T) {
	type testCaseT struct {
		description string
		input       *backendconfig.DestinationT
		expected    map[string]GenericConsentManagementProviderData
	}

	defGenericConsentManagementData := make(map[string]GenericConsentManagementProviderData)
	testCases := []testCaseT{
		{
			description: "should return empty generic consent management data when no consent management config is present",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{},
			},
			expected: defGenericConsentManagementData,
		},
		{
			description: "should return empty generic consent management data when consent management config is malformed",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"consentManagement": make(chan int),
				},
			},
			expected: defGenericConsentManagementData,
		},
		{
			description: "should return empty generic consent management data when consent management config is not as per the expected structure",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"consentManagement": map[string]interface{}{ // this should be an array
						"provider": "dummy",
					},
				},
			},
			expected: defGenericConsentManagementData,
		},
		{
			description: "should return empty generic consent management data when consent management config entries count is 0",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"consentManagement": []interface{}{},
				},
			},
			expected: defGenericConsentManagementData,
		},
		{
			description: "should return generic consent management data when consent management config entries are properly defined",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"consentManagement": []interface{}{
						map[string]interface{}{
							"provider": "oneTrust",
							"consents": []map[string]interface{}{
								{
									"consent": "consent category 1",
								},
								{
									"consent": "",
								},
								{
									"consent": "consent category 2",
								},
							},
						},
						map[string]interface{}{
							"provider": "ketch",
							"consents": []map[string]interface{}{
								{
									"consent": "purpose 1",
								},
								{
									"consent": "",
								},
								{
									"consent": "purpose 2",
								},
							},
						},
						map[string]interface{}{
							"provider": "custom",
							"consents": []map[string]interface{}{
								{
									"consent": "custom consent 1",
								},
								{
									"consent": "",
								},
								{
									"consent": "custom consent 2",
								},
								{
									"consent": "",
								},
							},
							"resolutionStrategy": "or",
						},
					},
				},
			},
			expected: map[string]GenericConsentManagementProviderData{
				"oneTrust": {
					Consents: []string{
						"consent category 1",
						"consent category 2",
					},
				},
				"ketch": {
					Consents: []string{
						"purpose 1",
						"purpose 2",
					},
				},
				"custom": {
					Consents: []string{
						"custom consent 1",
						"custom consent 2",
					},
					ResolutionStrategy: "or",
				},
			},
		},
		{
			description: "should return generic consent management data excluding the providers with empty consents",
			input: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"consentManagement": []interface{}{
						map[string]interface{}{
							"provider": "oneTrust",
							"consents": []map[string]interface{}{
								{
									"consent": "consent category 1",
								},
								{
									"consent": "",
								},
								{
									"consent": "consent category 2",
								},
							},
						},
						map[string]interface{}{
							"provider": "ketch",
							"consents": []map[string]interface{}{
								{
									"consent": "",
								},
							},
						},
						map[string]interface{}{
							"provider": "custom",
							"consents": []map[string]interface{}{
								{
									"consent": "",
								},
								{
									"consent": "",
								},
							},
							"resolutionStrategy": "or",
						},
					},
				},
			},
			expected: map[string]GenericConsentManagementProviderData{
				"oneTrust": {
					Consents: []string{
						"consent category 1",
						"consent category 2",
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			actual := GetGenericConsentManagementData(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}
