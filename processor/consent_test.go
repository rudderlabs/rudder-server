package processor

import (
	"testing"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/require"
)

func TestDeniedConsentCategories(t *testing.T) {
	testCases := []struct {
		name     string
		event    types.SingularEventT
		expected []string
	}{
		{
			name:     "no context",
			event:    types.SingularEventT{},
			expected: nil,
		},
		{
			name: "no consentManagement",
			event: types.SingularEventT{
				"context": map[string]interface{}{},
			},
			expected: nil,
		},
		{
			name: "no deniedConsentIds",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{},
				},
			},
			expected: nil,
		},
		{
			name: "empty deniedConsentIds",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{},
					},
				},
			},
			expected: nil,
		},
		{
			name: "some deniedConsentIds",
			event: types.SingularEventT{
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"foo", 123},
					},
				},
			},
			expected: []string{"foo"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.expected, deniedConsentCategories(tc.event))
		})
	}
}

func TestOneTrustConsentCategories(t *testing.T) {
	testCases := []struct {
		name     string
		dest     *backendconfig.DestinationT
		expected []string
	}{
		{
			name:     "no oneTrustCookieCategories",
			dest:     &backendconfig.DestinationT{},
			expected: nil,
		},
		{
			name: "empty oneTrustCookieCategories",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"oneTrustCookieCategories": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			name: "some oneTrustCookieCategories",
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
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.expected, oneTrustConsentCategories(tc.dest))
		})
	}
}

func TestKetchConsentCategories(t *testing.T) {
	testCases := []struct {
		name     string
		dest     *backendconfig.DestinationT
		expected []string
	}{
		{
			name:     "no ketchConsentPurposes",
			dest:     &backendconfig.DestinationT{},
			expected: nil,
		},
		{
			name: "empty ketchConsentPurposes",
			dest: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"ketchConsentPurposes": []interface{}{},
				},
			},
			expected: nil,
		},
		{
			name: "some ketchConsentPurposes",
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
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.expected, ketchConsentCategories(tc.dest))
		})
	}
}

func TestHandle_FilterDestinations(t *testing.T) {
	testCases := []struct {
		name            string
		event           types.SingularEventT
		destinations    []backendconfig.DestinationT
		expectedDestIDs []string
	}{
		{
			name:  "no denied consent categories",
			event: types.SingularEventT{},
			destinations: []backendconfig.DestinationT{
				{
					ID: "foo",
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{
								"oneTrustCookieCategory": "foo",
							},
						},
					},
				},
			},
			expectedDestIDs: []string{"foo"},
		},
		{
			name: "filter out destination with oneTrustCookieCategories",
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
			},
			expectedDestIDs: []string{"destID-1", "destID-2"},
		},
		{
			name: "filter out destination with ketchConsentPurposes",
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
			},
			expectedDestIDs: []string{"destID-1", "destID-2", "destID-4"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := &Handle{}
			proc.config.oneTrustConsentCategoriesMap = make(map[string][]string)
			proc.config.ketchConsentCategoriesMap = make(map[string][]string)

			for _, dest := range tc.destinations {
				proc.config.oneTrustConsentCategoriesMap[dest.ID] = oneTrustConsentCategories(&dest)
				proc.config.ketchConsentCategoriesMap[dest.ID] = ketchConsentCategories(&dest)
			}
			t.Log(proc.config.oneTrustConsentCategoriesMap)
			t.Log(proc.config.ketchConsentCategoriesMap)

			filteredDestinations := proc.filterDestinations(tc.event, tc.destinations)

			require.EqualValues(t, tc.expectedDestIDs, lo.Map(filteredDestinations, func(dest backendconfig.DestinationT, _ int) string {
				return dest.ID
			}))
		})
	}
}
