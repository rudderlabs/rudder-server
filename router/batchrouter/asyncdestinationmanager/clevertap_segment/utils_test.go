package clevertapSegment

import (
	"testing"

	"github.com/stretchr/testify/assert"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func TestGetCleverTapEndpoint(t *testing.T) {
	tests := []struct {
		region    string
		expectErr bool
		expectURL string
	}{
		{region: "IN", expectURL: "in1.api.clevertap.com"},
		{region: "SINGAPORE", expectURL: "sg1.api.clevertap.com"},
		{region: "US", expectURL: "us1.api.clevertap.com"},
		{region: "UNKNOWN", expectErr: true, expectURL: ""},
	}

	for _, tc := range tests {
		t.Run(tc.region, func(t *testing.T) {
			endpoint, err := GetCleverTapEndpoint(tc.region)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, endpoint, tc.expectURL)
		})
	}
}

func TestGetBulkApi(t *testing.T) {
	destConfig := DestinationConfig{Region: "IN"}
	assert.Equal(t, "IN", destConfig.Region)

	endpoints, err := GetBulkApi(destConfig)
	assert.Nil(t, err)
	assert.NotNil(t, endpoints)
	assert.NotEmpty(t, endpoints.BulkApi)
	assert.NotEmpty(t, endpoints.NotifyApi)
	// TODO: Validate endpoints.BulkApi and endpoints.NotifyApi
}

func TestConvertToConnectionConfig(t *testing.T) {
	type expected struct {
		senderName string
		isErr      bool
	}
	tests := []struct {
		conn     *backendconfig.Connection
		expected expected
	}{
		{
			conn: &backendconfig.Connection{
				SourceID:      "source123",
				DestinationID: "destination456",
				Enabled:       true,
				Config:        nil,
			},
			expected: expected{
				isErr: true,
			},
		},
		{
			conn: &backendconfig.Connection{
				SourceID:      "source123",
				DestinationID: "destination456",
				Enabled:       true,
				Config: map[string]interface{}{
					"Destination": map[string]interface{}{
						"SchemaVersion": "v1.0",
						"SegmentName":   "User Segment A",
						"AdminEmail":    "admin@example.com",
						"SenderName":    "Rudderstack",
					},
				},
			},
			expected: expected{
				senderName: "Rudderstack",
				isErr:      false,
			},
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			connConfig, err := ConvertToConnectionConfig(tc.conn)

			if tc.expected.isErr {
				assert.Error(t, err)
				assert.Equal(t, "", connConfig.Config.Destination.SenderName)
				return
			}
			assert.NoError(t, err)

			if connConfig.Config.Destination.SenderName == "" {
				assert.Equal(t, DEFAULT_SENDER_NAME, connConfig.Config.Destination.SenderName)
			} else {
				assert.Equal(t, tc.expected.senderName, connConfig.Config.Destination.SenderName)
			}
		})
	}
}
