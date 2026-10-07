package lyticsBulkUpload

import (
	"net/url"
	"testing"
)

func TestGetBulkAPI(t *testing.T) {
	t.Run("preserves endpoint for plain values", func(t *testing.T) {
		service := (&LyticsServiceImpl{}).getBulkApi(DestinationConfig{
			LyticsStreamName: "test",
			TimestampField:   "timestamp",
		})

		const expected = "https://bulk.lytics.io/collect/bulk/test?timestamp_field=timestamp"
		if service.BulkApi != expected {
			t.Fatalf("expected bulk API %q, got %q", expected, service.BulkApi)
		}
	})

	t.Run("encodes customer config values", func(t *testing.T) {
		service := (&LyticsServiceImpl{}).getBulkApi(DestinationConfig{
			LyticsStreamName: "VIP stream/with spaces?#&",
			TimestampField:   "event_time&filename=x",
		})

		const expected = "https://bulk.lytics.io/collect/bulk/VIP%20stream%2Fwith%20spaces%3F%23&?timestamp_field=event_time%26filename%3Dx"
		if service.BulkApi != expected {
			t.Fatalf("expected bulk API %q, got %q", expected, service.BulkApi)
		}

		parsed, err := url.Parse(service.BulkApi)
		if err != nil {
			t.Fatalf("parse bulk API: %v", err)
		}
		if parsed.EscapedPath() != "/collect/bulk/VIP%20stream%2Fwith%20spaces%3F%23&" {
			t.Fatalf("expected escaped stream name to remain one path segment, got %q", parsed.EscapedPath())
		}
		if parsed.Query().Get("timestamp_field") != "event_time&filename=x" {
			t.Fatalf("expected timestamp field to round trip, got %q", parsed.Query().Get("timestamp_field"))
		}
		if parsed.Query().Get("filename") != "" {
			t.Fatalf("expected no injected filename parameter, got %q", parsed.Query().Get("filename"))
		}
	})
}
