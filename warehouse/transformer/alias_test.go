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

func TestAlias(t *testing.T) {
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
			name:         "alias (Postgres)",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getAliasMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getAliasDefaultOutput(),
						Metadata:   getAliasMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without traits",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getAliasMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getAliasMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without context",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getAliasMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins"),
						Metadata:   getAliasMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) store rudder event",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getAliasMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"alias\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2}},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"previousId\":\"previousId\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getAliasMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) partial rules",
			eventPayload: `{"type":"alias","messageId":"messageId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getAliasMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getAliasMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getAliasMetadata("BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getAliasMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getAliasDefaultMergeOutput(),
						Metadata:   getAliasMetadata("BQ"),
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

func getAliasDefaultOutput() testhelper.OutputBuilder {
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
			"previous_id":              "previousId",
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
				"previous_id":              "string",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "aliases",
		},
		"userId": "",
	}
}

func getAliasDefaultMergeOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"merge_property_1_type":  "user_id",
			"merge_property_1_value": "userId",
			"merge_property_2_type":  "user_id",
			"merge_property_2_value": "previousId",
		},
		"metadata": map[string]any{
			"table":        "rudder_identity_merge_rules",
			"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
			"isMergeRule":  true,
			"receivedAt":   "2021-09-01T00:00:00.000Z",
			"mergePropOne": "userId",
			"mergePropTwo": "previousId",
		},
		"userId": "",
	}
}

func getAliasMetadata(destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "alias",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
	}
}
