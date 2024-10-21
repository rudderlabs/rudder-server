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

func TestGroup(t *testing.T) {
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
			name:         "group (Postgres)",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getGroupDefaultOutput(),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) without traits",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) without context",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins"),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) store rudder event",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"group\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2}},\"groupId\":\"groupId\",\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) partial rules",
			eventPayload: `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "group (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							SetTableName("_groups"),
						Metadata:   getGroupMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getGroupDefaultMergeOutput(),
						Metadata:   getGroupMetadata("BQ"),
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

func getGroupDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"anonymous_id":             "anonymousId",
			"channel":                  "web",
			"context_ip":               "1.2.3.4",
			"context_passed_ip":        "1.2.3.4",
			"context_request_ip":       "5.6.7.8",
			"context_traits_email":     "rhedricks@example.com",
			"context_traits_logins":    float64(2),
			"id":                       "messageId",
			"original_timestamp":       "2021-09-01T00:00:00.000Z",
			"received_at":              "2021-09-01T00:00:00.000Z",
			"sent_at":                  "2021-09-01T00:00:00.000Z",
			"timestamp":                "2021-09-01T00:00:00.000Z",
			"title":                    "Home | RudderStack",
			"url":                      "https://www.rudderstack.com",
			"user_id":                  "userId",
			"group_id":                 "groupId",
			"context_destination_id":   "destinationID",
			"context_destination_type": "POSTGRES",
			"context_source_id":        "sourceID",
			"context_source_type":      "sourceType",
		},
		"metadata": map[string]any{
			"columns": map[string]any{
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
				"id":                       "string",
				"original_timestamp":       "datetime",
				"received_at":              "datetime",
				"sent_at":                  "datetime",
				"timestamp":                "datetime",
				"title":                    "string",
				"url":                      "string",
				"user_id":                  "string",
				"group_id":                 "string",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "groups",
		},
		"userId": "",
	}
}

func getGroupDefaultMergeOutput() testhelper.OutputBuilder {
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

func getGroupMetadata(destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "group",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
	}
}
