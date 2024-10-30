package transformer

import (
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
)

func TestPageEvents(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	transformerResource, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	testCases := []struct {
		name             string
		configOverride   map[string]any
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "page (Postgres)",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getPageMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getPageDefaultOutput(),
						Metadata:   getPageMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) without properties",
			eventPayload: `{"type":"page","name":"Home","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getPageMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getPageMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) without context",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getPageMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getPageMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) store rudder event",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getPageMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"name\":\"Home\",\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"type\":\"page\",\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getPageMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) partial rules",
			eventPayload: `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getPageMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getPageMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getPageMetadata("BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getPageMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getPageDefaultMergeOutput(),
						Metadata:   getPageMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := setupConfig(transformerResource, tc.configOverride)
			eventsInfos := []testhelper.EventInfo{
				{
					Payload:     []byte(tc.eventPayload),
					Metadata:    tc.metadata,
					Destination: tc.destination,
				},
			}
			destinationTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
			warehouseTransformer := New(c, logger.NOP, stats.NOP)

			testhelper.ValidateEvents(t, eventsInfos, destinationTransformer, warehouseTransformer, tc.expectedResponse)
		})
	}
}

func getPageDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"name":                     "Home",
			"anonymous_id":             "anonymousId",
			"channel":                  "web",
			"context_ip":               "1.2.3.4",
			"context_passed_ip":        "1.2.3.4",
			"context_request_ip":       "5.6.7.8",
			"context_traits_email":     "rhedricks@example.com",
			"context_traits_logins":    float64(2),
			"context_traits_name":      "Richard Hendricks",
			"id":                       "messageId",
			"original_timestamp":       "2021-09-01T00:00:00.000Z",
			"received_at":              "2021-09-01T00:00:00.000Z",
			"sent_at":                  "2021-09-01T00:00:00.000Z",
			"timestamp":                "2021-09-01T00:00:00.000Z",
			"title":                    "Home | RudderStack",
			"url":                      "https://www.rudderstack.com",
			"user_id":                  "userId",
			"context_destination_id":   "destinationID",
			"context_destination_type": "POSTGRES",
			"context_source_id":        "sourceID",
			"context_source_type":      "sourceType",
		},
		"metadata": map[string]any{
			"columns": map[string]any{
				"name":                     "string",
				"anonymous_id":             "string",
				"channel":                  "string",
				"context_destination_id":   "string",
				"context_destination_type": "string",
				"context_source_id":        "string",
				"context_source_type":      "string",
				"context_ip":               "string",
				"context_passed_ip":        "string",
				"context_request_ip":       "string",
				"context_traits_email":     "string",
				"context_traits_logins":    "int",
				"context_traits_name":      "string",
				"id":                       "string",
				"original_timestamp":       "datetime",
				"received_at":              "datetime",
				"sent_at":                  "datetime",
				"timestamp":                "datetime",
				"title":                    "string",
				"url":                      "string",
				"user_id":                  "string",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "pages",
		},
		"userId": "",
	}
}

func getPageDefaultMergeOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"merge_property_1_type":  "anonymous_id",
			"merge_property_1_value": "anonymousId",
			"merge_property_2_type":  "user_id",
			"merge_property_2_value": "userId",
		},
		"metadata": map[string]any{
			"table":        "rudder_identity_merge_rules",
			"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
			"isMergeRule":  true,
			"receivedAt":   "2021-09-01T00:00:00.000Z",
			"mergePropOne": "anonymousId",
			"mergePropTwo": "userId",
		},
		"userId": "",
	}
}

func getPageMetadata(destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "page",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
	}
}