package processor

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

type ConnectionInfo struct {
	sourceId     string
	destinations []backendconfig.DestinationT
}

func TestGetOneTrustConsentCategories(t *testing.T) {
	testCases := []struct {
		description string
		dest        *backendconfig.DestinationT
		expected    []string
	}{
		{
			description: "should return nil when no oneTrustCookieCategories are configured",
			dest:        &backendconfig.DestinationT{},
			expected:    nil,
		},
		{
			description: "should return nil when oneTrustCookieCategories array is empty",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"oneTrustCookieCategories": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			description: "should return valid categories when oneTrustCookieCategories contains valid and invalid entries",
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
			require.EqualValues(t, tc.expected, getOneTrustConsentCategories(tc.dest))
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
			description: "should return nil when no ketchConsentPurposes are configured",
			dest:        &backendconfig.DestinationT{},
			expected:    nil,
		},
		{
			description: "should return nil when ketchConsentPurposes array is empty",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"ketchConsentPurposes": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			description: "should return valid categories when ketchConsentPurposes contains valid and invalid entries",
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
			require.EqualValues(t, tc.expected, getKetchConsentCategories(tc.dest))
		})
	}
}

func TestFilterDestinations(t *testing.T) {
	testCases := []struct {
		description     string
		event           types.SingularEventT
		sourceId        string
		connectionInfo  []ConnectionInfo
		expectedDestIDs []string
	}{
		{
			description: "should not filter any destination when consent management info is not present in the event",
			event:       types.SingularEventT{},
			sourceId:    "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1"},
		},
		{
			description: "should not filter any destination when consent IDs are not present in the event",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "all",
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1"},
		},
		{
			description: "should not filter any destination when consent IDs are empty in the event",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "all",
						"allowedConsentIds":  []string{},
						"deniedConsentIds":   []string{},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1"},
		},
		{
			description: "should not filter any destination when consent info is malformed in the event",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "all",
						"allowedConsentIds":  "dummy",
						"deniedConsentIds":   "dummy",
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1"},
		},
		{
			description: "should use oneTrustCookieCategories when provider is not specified in legacy SDKs",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{ // this should be ignored
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
										{
											"consent": "foo-2",
										},
									},
								},
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{},
								},
							},
						},
						{
							ID: "destID-2",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{ // this should be ignored
									"provider": "oneTrust",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
										{
											"consent": "foo-2",
										},
									},
								},
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{
										"oneTrustCookieCategory": "",
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-7"},
		},
		{
			description: "should use oneTrustCookieCategories when GCM config is not available and provider in the event matches",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "or", // this should be ignored
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "should use oneTrustCookieCategories when GCM config is not available and provider in the event does not exist",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"resolutionStrategy": "or", // this should be ignored
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "should use ketchConsentPurposes when provider is not specified in legacy SDKs",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{ // this should be ignored
									"provider": "ketch",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
										{
											"consent": "foo-2",
										},
									},
								},
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{},
								},
							},
						},
						{
							ID: "destID-2",
							Config: map[string]interface{}{
								"consentManagement": map[string]interface{}{ // this should be ignored
									"provider": "ketch",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
										{
											"consent": "foo-2",
										},
									},
								},
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
								"consentManagement": map[string]interface{}{ // this should be ignored
									"provider": "ketch",
									"consents": []map[string]interface{}{
										{
											"consent": "foo-1",
										},
										{
											"consent": "foo-2",
										},
									},
								},
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "",
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4", "destID-7"},
		},
		{
			description: "should use ketchConsentPurposes when GCM config is not available and provider in the event matches",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or", // this should be ignored
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "should use ketchConsentPurposes when GCM config is not available and provider in the event does not exist",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"resolutionStrategy": "or", // this should be ignored
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "should use generic consent management config for Ketch",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or",
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
				// Some destinations are connected to different source with different consent management info
				{
					sourceId: "sourceID-2",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "custom",
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
										"provider": "custom",
										"consents": []map[string]interface{}{
											{"consent": "foo-4"},
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
										"provider": "ketch",
										"consents": []map[string]interface{}{
											{"consent": "foo-4"},
											{"consent": "foo-5"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4", "destID-7"},
		},
		{
			description: "should filter out destination when generic consent management is unavailable and falls back to legacy consents for Ketch",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or",
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						// Destination with GCM (for Ketch) and ketchConsentPurposes but GCM will be preferred
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "ketch",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
										},
									},
								},
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "foo-5",
									},
								},
							},
						},
						// Destination without GCM but ketchConsentPurposes. ketchConsentPurposes will be used.
						{
							ID: "destID-2",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{ // should fallback to this
									map[string]interface{}{
										"purpose": "foo-4",
									},
								},
							},
						},
						// Destination without GCM and ketchConsentPurposes. oneTrustCookieCategories will NOT be used.
						{
							ID: "destID-3",
							Config: map[string]interface{}{
								"oneTrustCookieCategories": []interface{}{ // should not fallback to this just because ketch and gcm is unavailable
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-1",
									},
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-2",
									},
								},
							},
						},
						// Destination without GCM but ketchConsentPurposes. ketchConsentPurposes will be used.
						{
							ID: "destID-4",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{ // should fallback to this
									map[string]interface{}{
										"purpose": "foo-1",
									},
								},
							},
						},
						// Destination with GCM (not for Ketch) and ketchConsentPurposes but ketchConsentPurposes will be preferred
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "custom",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
										},
									},
								},
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "foo-5",
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-6",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "",
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-2", "destID-3", "destID-5", "destID-6"},
		},
		{
			description: "should filter out destination with generic consent management for OneTrust",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
				// Some destinations are connected to different source with different consent management info
				{
					sourceId: "sourceID-2",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "custom",
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
										"provider": "custom",
										"consents": []map[string]interface{}{
											{"consent": "foo-4"},
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
										"provider": "oneTrust",
										"consents": []map[string]interface{}{
											{"consent": "foo-4"},
											{"consent": "foo-5"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-7"},
		},
		{
			description: "should filter out destination when generic consent management is unavailable and falls back to legacy consents for OneTrust",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						// Destination with GCM (for OneTrust) and ketchConsentPurposes but GCM will be preferred
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "oneTrust",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
										},
									},
								},
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-5",
									},
								},
							},
						},
						// Destination without GCM but oneTrustCookieCategories. oneTrustCookieCategories will be used.
						{
							ID: "destID-2",
							Config: map[string]interface{}{
								"oneTrustCookieCategories": []interface{}{ // should fallback to this
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-4",
									},
								},
							},
						},
						// Destination without GCM and oneTrustCookieCategories. ketchConsentPurposes will NOT be used.
						{
							ID: "destID-3",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{ // should not fallback to this just because ketch and gcm is unavailable
									map[string]interface{}{
										"purpose": "foo-1",
									},
									map[string]interface{}{
										"purpose": "foo-2",
									},
								},
							},
						},
						// Destination without GCM but ketchConsentPurposes. ketchConsentPurposes will be used.
						{
							ID: "destID-4",
							Config: map[string]interface{}{
								"oneTrustCookieCategories": []interface{}{ // should fallback to this
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-1",
									},
								},
							},
						},
						// Destination with GCM (not for OneTrust) and oneTrustCookieCategories but oneTrustCookieCategories will be preferred
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "custom",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
										},
									},
								},
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-5",
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-6",
							Config: map[string]interface{}{
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{
										"oneTrustCookieCategory": "",
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-2", "destID-3", "destID-5", "destID-6"},
		},
		{
			description: "should filter out destination with generic consent management for Custom with AND strategy",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "custom",
						"resolutionStrategy": "or", // this will be ignored and the value from the destination config will be used
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-7"},
		},
		{
			description: "should filter out destination with generic consent management for Custom with OR strategy",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "custom",
						"resolutionStrategy": "and", // this will be ignored and the value from the destination config will be used
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
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
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-7",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4", "destID-7"},
		},
		{
			description: "should filter out destinations when generic consent management for Custom is unavailable but does not fall back to legacy consents",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "custom",
						"resolutionStrategy": "or",
						"allowedConsentIds":  []string{"foo-4"},
						"deniedConsentIds":   []string{"foo-1", "foo-2", "foo-3"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "ketch",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
										},
									},
								},
							},
						},
						// Destination with ketchConsentPurposes but it'll not be used
						{
							ID: "destID-2",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "foo-1",
									},
								},
							},
						},
						// Destination with oneTrustCookieCategories but it'll not be used
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
						// Destination with ketchConsentPurposes and oneTrustCookieCategories but it'll not be used
						{
							ID: "destID-4",
							Config: map[string]interface{}{
								"ketchConsentPurposes": []interface{}{
									map[string]interface{}{
										"purpose": "foo-1",
									},
								},
								"oneTrustCookieCategories": []interface{}{
									map[string]interface{}{
										"oneTrustCookieCategory": "foo-1",
									},
								},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-3", "destID-4"},
		},
		{
			description: "should treat resolution strategy 'any' as 'or' for custom provider",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":          "custom",
						"allowedConsentIds": []string{"foo-1", "foo-2", "foo-3"},
						"deniedConsentIds":  []string{"foo-4"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider":           "custom",
										"resolutionStrategy": "any",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
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
										"resolutionStrategy": "any",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
										"resolutionStrategy": "any",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-3",
											},
											{
												"consent": "foo-4",
											},
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
										"resolutionStrategy": "any",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-4",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-3", "destID-5"},
		},
		{
			description: "should treat resolution strategy 'all' as 'and' for custom provider",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":          "custom",
						"allowedConsentIds": []string{"foo-1", "foo-2", "foo-3"},
						"deniedConsentIds":  []string{"foo-4"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider":           "custom",
										"resolutionStrategy": "all",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
										"resolutionStrategy": "all",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-4",
											},
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
										"resolutionStrategy": "all",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-4",
											},
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
										"resolutionStrategy": "all",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-4", "destID-5"},
		},
		{
			description: "should treat resolution strategy 'any' as 'or' for ketch provider",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "any",
						"allowedConsentIds":  []string{"foo-1", "foo-2", "foo-3"},
						"deniedConsentIds":   []string{"foo-4"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "ketch",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
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
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
											{
												"consent": "foo-3",
											},
											{
												"consent": "foo-4",
											},
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
											{
												"consent": "foo-4",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-3", "destID-5"},
		},
		{
			description: "should treat resolution strategy 'all' as 'and' for oneTrust provider",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "all",
						"allowedConsentIds":  []string{"foo-1", "foo-2", "foo-3"},
						"deniedConsentIds":   []string{"foo-4"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "oneTrust",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-4",
											},
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
											{
												"consent": "foo-4",
											},
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
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-5",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-4", "destID-5"},
		},
		{
			description: "should use denied consent IDs from event when allowed consent IDs are not present (resolution strategy 'any')",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "any",
						"deniedConsentIds":   []string{"foo-4", "foo-5"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "ketch",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-4",
											},
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
											{
												"consent": "foo-4",
											},
											{
												"consent": "foo-5",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-4",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "should use denied consent IDs from event when allowed consent IDs are not present (resolution strategy 'all')",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "all",
						"deniedConsentIds":   []string{"foo-4", "foo-5"},
					},
				},
			},
			sourceId: "sourceID-1",
			connectionInfo: []ConnectionInfo{
				{
					sourceId: "sourceID-1",
					destinations: []backendconfig.DestinationT{
						{
							ID: "destID-1",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{
									map[string]interface{}{
										"provider": "oneTrust",
										"consents": []map[string]interface{}{
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-3",
											},
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
											{
												"consent": "foo-1",
											},
											{
												"consent": "foo-2",
											},
											{
												"consent": "foo-4",
											},
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
											{
												"consent": "foo-4",
											},
											{
												"consent": "foo-5",
											},
										},
									},
								},
							},
						},
						// empty consents. Consent management is practically not configured for this destination.
						{
							ID: "destID-4",
							Config: map[string]interface{}{
								"consentManagement": []interface{}{},
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-4"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			proc := &Handle{}
			proc.config.oneTrustConsentCategoriesMap = make(map[string][]string)
			proc.config.ketchConsentCategoriesMap = make(map[string][]string)
			proc.config.genericConsentManagementMap = make(SourceConsentMap)
			proc.logger = logger.NewLogger().Child("processor")

			for _, connection := range tc.connectionInfo {
				proc.config.genericConsentManagementMap[SourceID(connection.sourceId)] = make(DestConsentMap)
				for _, dest := range connection.destinations {
					proc.config.oneTrustConsentCategoriesMap[dest.ID] = getOneTrustConsentCategories(&dest)
					proc.config.ketchConsentCategoriesMap[dest.ID] = getKetchConsentCategories(&dest)
					proc.config.genericConsentManagementMap[SourceID(connection.sourceId)][DestinationID(dest.ID)], _ = getGenericConsentManagementData(&dest)
				}
			}

			// Find the connection based on the source ID we're interested in
			connection, _ := lo.Find(tc.connectionInfo, func(connection ConnectionInfo) bool {
				return connection.sourceId == tc.sourceId
			})

			// Get the filtered destinations based on the event and the connection info
			filteredDestinations := proc.getConsentFilteredDestinations(tc.event, tc.sourceId, connection.destinations)

			// Assert that the filtered destinations are as expected
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
			description: "should return default values for allowed and denied consent IDs when they are missing in the event",
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
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider:           "custom",
				ResolutionStrategy: "",
				AllowedConsentIDs:  []string{},
				DeniedConsentIDs:   []string{},
			},
		},
		{
			description: "should return default values for allowed and denied consent IDs when they are malformed",
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
						"deniedConsentIds":  "dummy1",
						"allowedConsentIds": "dummy2",
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
						"deniedConsentIds": map[string]interface{}{ // this should have been an array
							"consent": "consent category 2",
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
				AllowedConsentIDs: []string{
					"consent category 1",
					"consent category 2",
				},
				DeniedConsentIDs: []string{
					"consent category 3",
					"consent category 4",
				},
				ResolutionStrategy: "and",
			},
		},
		{
			description: "should return consent management info when consent management data is sent from older SDKs",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type":        "track",
				"event":       "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []string{
							"consent category 3",
							"",
							"consent category 4",
							"",
						},
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider:          "",
				AllowedConsentIDs: []string{},
				DeniedConsentIDs: []string{
					"consent category 3",
					"consent category 4",
				},
				ResolutionStrategy: "",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			actual, _ := getConsentManagementInfo(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestGetGenericConsentManagementData(t *testing.T) {
	type testCaseT struct {
		description string
		input       *backendconfig.DestinationT
		expected    ConsentProviderMap
	}

	defGenericConsentManagementData := make(ConsentProviderMap)
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
									"consent": " purpose 1 ",
								},
								{
									"consent": "  ",
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
									"consent": "custom consent 1 ",
								},
								{
									"consent": "",
								},
								{
									"consent": " custom consent 2",
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
			expected: ConsentProviderMap{
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
									"consent": "   ",
								},
							},
							"resolutionStrategy": "or",
						},
					},
				},
			},
			expected: ConsentProviderMap{
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
			actual, _ := getGenericConsentManagementData(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}
