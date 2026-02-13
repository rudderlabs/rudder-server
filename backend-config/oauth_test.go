package backendconfig

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

func TestDestinationT_IsOAuthDestination(t *testing.T) {
	tests := []struct {
		name           string
		flow           common.RudderFlow
		inputDefConfig map[string]interface{}
		expectedOAuth  bool
		expectedErr    error
	}{
		{
			name: "should pass for a destination which contains OAuth and rudderScopes",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type":         "OAuth",
					"rudderScopes": []interface{}{"delivery"},
				},
			},
			expectedOAuth: true,
			expectedErr:   nil,
		},
		{
			name: "should pass for a destination which contains OAuth but not rudderScopes",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type": "OAuth",
				},
			},
			expectedOAuth: true,
			expectedErr:   nil,
		},
		{
			name: "should return 'false' without error for a destination which contains OAuth with delete rudderScopes when flow is delivery",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type":         "OAuth",
					"rudderScopes": []interface{}{"delete"},
				},
			},
			expectedOAuth: false,
			expectedErr:   nil,
		},
		{
			name: "should return 'true' without error for a destination which contains OAuth without rudderScopes when flow is delivery",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type": "OAuth",
				},
			},
			expectedOAuth: true,
			expectedErr:   nil,
		},
		{
			name: "should return 'false' with error for a destination which contains OAuth with one of invalid rudderScopes when flow is delivery",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type":         "OAuth",
					"rudderScopes": []interface{}{"delivery", 1},
				},
			},
			expectedOAuth: false,
			expectedErr:   fmt.Errorf("1 in auth.rudderScopes should be string but got int"),
		},
		{
			name: "should return 'false' with error for a destination which contains OAuth with invalid rudderScopes type when flow is delivery",
			flow: common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{
				"auth": map[string]interface{}{
					"type":         "OAuth",
					"rudderScopes": []interface{}{"a"}[0],
				},
			},
			expectedOAuth: false,
			expectedErr:   fmt.Errorf("rudderScopes should be a []any but got string"),
		},
		{
			name:           "should return 'false' without error for a non-OAuth destination when flow is delivery",
			flow:           common.RudderFlowDelivery,
			inputDefConfig: map[string]interface{}{},
			expectedOAuth:  false,
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DestinationT{
				DestinationDefinition: DestinationDefinitionT{
					Name:   "dest_def_name",
					Config: tt.inputDefConfig,
				},
			}
			isOAuth, err := d.IsOAuthDestination(tt.flow)

			require.Equal(t, tt.expectedOAuth, isOAuth)
			if tt.expectedErr != nil {
				require.EqualError(t, err, tt.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
