package v2_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
	account        *backendconfig.AccountWithDefinition
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
	// scenarios considering only account is present mostly possible for any new oauth destinations
	{
		description:    "should return 'true' without error for a OAuth destination when account is presence and refreshOAuthToken is true",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: true,
		},
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				Name:               "DESTINATION_HUBSPOT_OAUTH",
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOAuthToken": true,
				},
			},
		},
	},
	{
		description:    "should return 'false' without error for a OAuth destination when account is presence and refreshOAuthToken is false",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				Name:               "DESTINATION_HUBSPOT_OAUTH",
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOAuthToken": false,
				},
			},
		},
	},
	{
		description:    "should return 'false' without error for a OAuth destination when account is presence and refreshOAuthToken is an object",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				Name:               "DESTINATION_HUBSPOT_OAUTH",
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOAuthToken": map[string]interface{}{
						"value": true,
					},
				},
			},
		},
	},
	{
		description:    "should return 'false' without error for a OAuth destination when account is presence and refreshOAuthToken is not present",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOauthTokenValue": true,
				},
			},
		},
	},
	{
		description:    "should return 'false' without error for a non-OAuth destination when account is not present",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
		account: nil,
	},
	// scenarios considering both account and definition config are present mostly applicable for oauth/non-oauth destinations which are already present in production
	{
		description: "should return true without error for a OAuth destination when account is present",
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
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				Name:               "DESTINATION_HUBSPOT_OAUTH",
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOAuthToken": true,
				},
			},
		},
	},
	{
		description:    "should return false without error for non-OAuth destination when account is present",
		flow:           common.RudderFlowDelivery,
		inputDefConfig: map[string]interface{}{},
		expected: isOAuthResult{
			isOAuth: false,
		},
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				AuthenticationType: "BearerToken",
				Config:             map[string]interface{}{},
			},
		},
	},
	{
		description: "should return true without error for a OAuth destination when account is present but refreshOAuthToken is in wrong format",
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
		account: &backendconfig.AccountWithDefinition{
			AccountDefinition: backendconfig.AccountDefinition{
				AuthenticationType: "OAuth",
				Config: map[string]interface{}{
					"refreshOAuthToken": map[string]interface{}{
						"value": true,
					},
				},
			},
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
				d.Account = tc.account
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
