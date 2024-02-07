package processor

import (
	"testing"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
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
			require.EqualValues(t, tc.expected, getKetchConsentCategories(tc.dest))
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
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			description: "filter out destination with oneTrustCookieCategories with event containing provider details",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust", // this should be ignored
						"resolutionStrategy": "and",      // this should be ignored
						"deniedConsentIds":   []interface{}{"foo-1", "foo-2", "foo-3"},
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
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
		{
			description: "filter out destination with ketchConsentPurposes with event containing provider details",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch", // this should be ignored
						"resolutionStrategy": "or",    // this should be ignored
						"deniedConsentIds":   []interface{}{"foo-1", "foo-2", "foo-3"},
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
			description: "filter out destination when generic consent management is unavailable and falls back to legacy consents (Ketch)",
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
								"provider": "custom",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
						"ketchConsentPurposes": []interface{}{ // should fallback to this
							map[string]interface{}{
								"purpose": "foo-5",
							},
						},
					},
				},
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
				{
					ID: "destID-4",
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
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-3"},
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
			description: "filter out destination when generic consent management is unavailable and falls back to legacy consents (OneTrust)",
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
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{},
								},
							},
						},
						"oneTrustCookieCategories": []interface{}{ // should fallback to this
							map[string]interface{}{
								"oneTrustCookieCategory": "foo-5",
							},
						},
					},
				},
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
				{
					ID: "destID-3",
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{ // should not fallback to this just because oneTrust and gcm is unavailable
							map[string]interface{}{
								"purpose": "foo-1",
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
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-3"},
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
			description: "filter out destination when generic consent management (Custom) is unavailable but doesn't falls back to legacy consents",
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
					ID:     "destID-2",
					Config: map[string]interface{}{},
				},
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			proc := &Handle{}
			proc.config.oneTrustConsentCategoriesMap = make(map[string][]string)
			proc.config.ketchConsentCategoriesMap = make(map[string][]string)
			proc.config.destGenericConsentManagementMap = make(map[string]map[string]GenericConsentManagementProviderData)
			proc.logger = logger.NewLogger().Child("processor")

			for _, dest := range tc.destinations {
				proc.config.oneTrustConsentCategoriesMap[dest.ID] = getOneTrustConsentCategories(&dest)
				proc.config.ketchConsentCategoriesMap[dest.ID] = getKetchConsentCategories(&dest)
				proc.config.destGenericConsentManagementMap[dest.ID], _ = getGenericConsentManagementData(&dest)
			}

			filteredDestinations := proc.getConsentFilteredDestinations(tc.event, tc.destinations)

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
						"allowedConsentIds": []string{
							"consent category 1",
							"consent category 2",
						},
					},
				},
			},
			expected: ConsentManagementInfo{
				Provider: "custom",
				AllowedConsentIDs: []interface{}{
					"consent category 1",
					"consent category 2",
				},
				DeniedConsentIDs:   []string{},
				ResolutionStrategy: "",
			},
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
						"deniedConsentIds": map[string]interface{}{ // this should have been an array
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
						"allowedConsentIds": map[string]interface{}{
							"C0001": "consent category 1",
							"C0002": "consent category 2",
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
				AllowedConsentIDs: map[string]interface{}{
					"C0001": "consent category 1",
					"C0002": "consent category 2",
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
				Provider: "",
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
			actual, _ := getGenericConsentManagementData(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}
