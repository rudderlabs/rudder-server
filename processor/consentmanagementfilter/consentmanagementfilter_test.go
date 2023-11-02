package consentmanagementfilter

import (
	"testing"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/require"
)

func TestGetConsentManagementInfo(t *testing.T) {
	type testCaseT struct {
		description string
		input types.SingularEventT
		expected ConsentManagementInfo
	}

	defConsentManagementInfo := ConsentManagementInfo{}
	testCases := []testCaseT {
		{
			description: "should return empty consent management info when no context is sent",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type": "track",
				"event": "test",
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
				"type": "track",
				"event": "test",
				"properties": map[string]interface{}{
					"category": "test",
				},
				"context": map[string]interface{}{},
			},
			expected: defConsentManagementInfo,
		},
		{
			description: "should return empty consent management info when consent management data is not a valid object",
			input: types.SingularEventT{
				"anonymousId": "123",
				"type": "track",
				"event": "test",
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
				"type": "track",
				"event": "test",
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
				"type": "track",
				"event": "test",
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
				"type": "track",
				"event": "test",
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
						"extra": "extra field",
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
