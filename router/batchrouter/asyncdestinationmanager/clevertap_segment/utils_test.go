package clevertapSegment

import (
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func TestGetCleverTapEndpoint(t *testing.T) {
	tests := []struct {
		region    string
		expectErr bool
		expectURL string
	}{
		{"IN", false, "in1.api.clevertap.com"},
		{"SINGAPORE", false, "sg1.api.clevertap.com"},
		{"US", false, "us1.api.clevertap.com"},
		{"UNKNOWN", true, ""},
	}

	for _, test := range tests {
		t.Run(test.region, func(t *testing.T) {
			service := &ClevertapServiceImpl{}
			endpoint, err := service.getCleverTapEndpoint(test.region)

			if test.expectErr && err == nil {
				t.Errorf("expected an error for region %s, got none", test.region)
			}
			if !test.expectErr && err != nil {
				t.Errorf("did not expect an error for region %s, got: %v", test.region, err)
			}
			if endpoint != test.expectURL {
				t.Errorf("expected URL %s, got %s", test.expectURL, endpoint)
			}
		})
	}
}

func TestGetBulkApi(t *testing.T) {
	service := &ClevertapServiceImpl{}
	destConfig := DestinationConfig{Region: "IN"}

	bulkApi := service.getBulkApi(destConfig)
	if bulkApi == nil {
		t.Fatal("expected a non-nil bulk API service")
	}
	if bulkApi.BulkApi == "" {
		t.Error("expected a non-empty BulkApi URL")
	}
}

func TestConvertToConnectionConfig(t *testing.T) {
	tests := []struct {
		conn      *backendconfig.Connection
		expectErr bool
	}{
		{nil, true},
		{&backendconfig.Connection{
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
		}, false},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			service := &ClevertapServiceImpl{}
			connConfig, err := service.convertToConnectionConfig(test.conn)

			if test.expectErr && err == nil {
				t.Error("expected an error, got none")
			}
			if !test.expectErr && err != nil {
				t.Errorf("did not expect an error, got: %v", err)
			}
			if !test.expectErr && connConfig.Config.Destination.SenderName != DEFAULT_SENDER_NAME {
				t.Errorf("expected SenderName to be set to default, got: %s", connConfig.Config.Destination.SenderName)
			}
		})
	}
}
