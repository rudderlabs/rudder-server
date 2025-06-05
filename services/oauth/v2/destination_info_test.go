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
	description     string
	flow            common.RudderFlow
	deliveryByOAuth bool
	deleteByOAuth   bool
	expected        isOAuthResult
}

var isOAuthDestTestCases = []destInfoTestCase{
	{
		description:     "should return true for delivery flow when DeliveryByOAuth is true",
		flow:            common.RudderFlowDelivery,
		deliveryByOAuth: true,
		deleteByOAuth:   false,
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description:     "should return false for delivery flow when DeliveryByOAuth is false",
		flow:            common.RudderFlowDelivery,
		deliveryByOAuth: false,
		deleteByOAuth:   true,
		expected: isOAuthResult{
			isOAuth: false,
		},
	},
	{
		description:     "should return true for delete flow when DeleteByOAuth is true",
		flow:            common.RudderFlowDelete,
		deliveryByOAuth: false,
		deleteByOAuth:   true,
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description:     "should return false for delete flow when DeleteByOAuth is false",
		flow:            common.RudderFlowDelete,
		deliveryByOAuth: true,
		deleteByOAuth:   false,
		expected: isOAuthResult{
			isOAuth: false,
		},
	},
	{
		description:     "should return true for delivery flow when both flags are true",
		flow:            common.RudderFlowDelivery,
		deliveryByOAuth: true,
		deleteByOAuth:   true,
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description:     "should return true for delete flow when both flags are true",
		flow:            common.RudderFlowDelete,
		deliveryByOAuth: true,
		deleteByOAuth:   true,
		expected: isOAuthResult{
			isOAuth: true,
		},
	},
	{
		description:     "should return false for both flows when both flags are false",
		flow:            common.RudderFlowDelivery,
		deliveryByOAuth: false,
		deleteByOAuth:   false,
		expected: isOAuthResult{
			isOAuth: false,
		},
	},
}

var unsupportedFlowTestCases = []struct {
	description string
	flow        common.RudderFlow
	expected    isOAuthResult
}{
	{
		description: "should return error for unsupported flow type",
		flow:        common.RudderFlow("unsupported"),
		expected: isOAuthResult{
			isOAuth: false,
			err:     fmt.Errorf("unsupported flow type: unsupported"),
		},
	},
}

var _ = Describe("DestinationInfo tests", func() {
	Describe("IsOAuthDestination tests", func() {
		for _, tc := range isOAuthDestTestCases {
			It(tc.description, func() {
				d := &v2.DestinationInfo{
					DefinitionName:  "dest_def_name",
					DeliveryByOAuth: tc.deliveryByOAuth,
					DeleteByOAuth:   tc.deleteByOAuth,
				}
				isOAuth, err := d.IsOAuthDestination(tc.flow)

				Expect(isOAuth).To(Equal(tc.expected.isOAuth))
				if tc.expected.err != nil {
					Expect(err).To(Equal(tc.expected.err))
				} else {
					Expect(err).To(BeNil())
				}
			})
		}

		Describe("IsOAuthDestination unsupported flow tests", func() {
			for _, tc := range unsupportedFlowTestCases {
				It(tc.description, func() {
					d := &v2.DestinationInfo{
						DefinitionName:  "dest_def_name",
						DeliveryByOAuth: true,
						DeleteByOAuth:   true,
					}
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
})
