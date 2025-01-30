package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractJSONPathInfo(t *testing.T) {
	testCases := []struct {
		name      string
		jsonPaths []string
		expected  jsonPathInfo
	}{
		{
			name:      "Valid JSON paths with track prefix",
			jsonPaths: []string{"track.properties.name", "track.properties.age", "properties.name", "properties.age"},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"track_properties_name": 2, "track_properties_age": 2},
				legacyKeysMap: map[string]int{"properties_name": 1, "properties_age": 1},
			},
		},
		{
			name:      "Valid JSON paths with identify prefix",
			jsonPaths: []string{"identify.traits.address.city", "identify.traits.address.zip", "traits.address.city", "traits.address.zip"},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"identify_traits_address_city": 3, "identify_traits_address_zip": 3},
				legacyKeysMap: map[string]int{"traits_address_city": 2, "traits_address_zip": 2},
			},
		},
		{
			name:      "Whitespace and empty path",
			jsonPaths: []string{"   ", "track.properties.name", "", " track.properties.age "},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"track_properties_name": 2, "track_properties_age": 2},
				legacyKeysMap: make(map[string]int),
			},
		},
		{
			name:      "Unknown prefix JSON paths",
			jsonPaths: []string{"unknown.prefix.eventType.name", "unknown.prefix.eventType.value"},
			expected: jsonPathInfo{
				keysMap:       make(map[string]int),
				legacyKeysMap: map[string]int{"unknown_prefix_eventType_name": 3, "unknown_prefix_eventType_value": 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, extractJSONPathInfo(tc.jsonPaths))
		})
	}
}

func TestIsValidJSONPathKey(t *testing.T) {
	testCases := []struct {
		name, key string
		level     int
		isValid   bool
	}{
		{
			name:    "Valid JSON path key with track prefix",
			key:     "track_properties_name",
			level:   2,
			isValid: true,
		},
		{
			name:    "Valid JSON path key with identify prefix",
			key:     "identify_traits_address_city",
			level:   3,
			isValid: true,
		},
		{
			name:    "Valid JSON path key with unknown prefix",
			key:     "unknown_prefix_eventType_name",
			level:   3,
			isValid: false,
		},
		{
			name:    "Invalid JSON path key",
			key:     "invalid_key",
			level:   0,
			isValid: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathsInfo := extractJSONPathInfo(
				[]string{
					"track.properties.name", "properties.name",
					"identify.traits.address.city", "traits.address.city",
					"unknown.prefix.eventType.name",
				},
			)
			require.Equal(t, tc.isValid, isValidJSONPathKey(tc.key, tc.level, pathsInfo.keysMap))
		})
	}
}

func TestIsValidLegacyJSONPathKey(t *testing.T) {
	testCases := []struct {
		name, key, eventType string
		level                int
		isValid              bool
	}{
		{
			name:      "Valid JSON path key with track prefix",
			key:       "properties_name",
			eventType: "track",
			level:     1,
			isValid:   true,
		},
		{
			name:      "Valid JSON path key with identify prefix",
			key:       "traits_address_city",
			eventType: "identify",
			level:     2,
			isValid:   false,
		},
		{
			name:      "Valid JSON path key with unknown prefix",
			key:       "unknown_prefix_eventType_name",
			eventType: "track",
			level:     3,
			isValid:   true,
		},
		{
			name:      "Invalid JSON path key",
			key:       "invalid_key",
			eventType: "track",
			level:     0,
			isValid:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathsInfo := extractJSONPathInfo(
				[]string{
					"track.properties.name", "properties.name",
					"identify.traits.address.city", "traits.address.city",
					"unknown.prefix.eventType.name",
				},
			)
			require.Equal(t, tc.isValid, isValidLegacyJSONPathKey(tc.eventType, tc.key, tc.level, pathsInfo.legacyKeysMap))
		})
	}
}
