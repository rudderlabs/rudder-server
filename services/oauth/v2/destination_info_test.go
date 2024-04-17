package v2_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type isOAuthResult struct {
	isOAuth bool
	err     error
}

type destInfoTestCase struct {
	description    string
	flow           common.RudderFlow
	inputDefConfig map[string]interface{}
	expected       isOAuthResult
}

var isOAuthDestTestCases = []destInfoTestCase{
	{
		description: "should pass for a destination which contains OAuth and rudderScopes",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type":         "OAuth",
				"rudderScopes": []interface{}{"delivery"},
			},
		},
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description: "should pass for a destination which contains OAuth but not rudderScopes",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type": "OAuth",
			},
		},
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description: "should return 'false' without error for a destination which contains OAuth with delete rudderScopes when flow is delivery",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type":         "OAuth",
				"rudderScopes": []interface{}{"delete"},
			},
		},
		expected: isOAuthResult{
			isOAuth: false,
		},
	},
	{
		description: "should return 'true' without error for a destination which contains OAuth withoutrudderScopes when flow is delivery",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type": "OAuth",
			},
		},
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description: "should return 'false' with error for a destination which contains OAuth with one of invalid rudderScopes when flow is delivery",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type":         "OAuth",
				"rudderScopes": []interface{}{"delivery", 1},
			},
		},
		expected: isOAuthResult{
			isOAuth: false,
			err:     fmt.Errorf("1 in auth.rudderScopes should be string"),
		},
	},
	{
		description: "should return 'false' with error for a destination which contains OAuth with invalid rudderScopes type when flow is delivery",
		flow:        common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{
			"auth": map[string]interface{}{
				"type":         "OAuth",
				"rudderScopes": []interface{}{"a"}[0],
			},
		},
		expected: isOAuthResult{
			isOAuth: false,
			err:     fmt.Errorf("rudderScopes should be a interface[]"),
		},
	},
	{
		description:    "should return 'false' without error for a non-OAuth destination when flow is delivery",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
	},
}

var _ = Describe("DestinationInfo tests", func() {
	Describe("IsOAuthDestination tests", func() {
		for _, tc := range isOAuthDestTestCases {
			It(tc.description, func() {
				d := &v2.DestinationInfo{
					DefinitionName: "dest_def_name",
				}
				d.DefinitionConfig = tc.inputDefConfig
				isOAuth, err := d.IsOAuthDestination(tc.flow)

				Expect(isOAuth).To(Equal(tc.expected.isOAuth))
				if tc.expected.err != nil {
					Expect(err).To(Equal(tc.expected.err))
				} else {
					Expect(err).To(BeNil())
				}
			})
		}
	})
})
