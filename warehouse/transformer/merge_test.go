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
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
)

func TestMerge(t *testing.T) {
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
			name: "merge (Postgres)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload:     `{"type":"merge"}`,
			metadata:         getMergeMetadata("merge", "POSTGRES"),
			destination:      getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"merge_property_1_type":  "email",
								"merge_property_1_value": "alex@example.com",
								"merge_property_2_type":  "mobile",
								"merge_property_2_value": "+1-202-555-0146",
							},
							"metadata": map[string]any{
								"table":        "rudder_identity_merge_rules",
								"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "alex@example.com",
								"mergePropTwo": "+1-202-555-0146",
							},
							"userId": "",
						},
						Metadata:   getMergeMetadata("merge", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "merge (BQ) not enableIDResolution",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": false,
			},
			eventPayload:     `{"type":"merge"}`,
			metadata:         getMergeMetadata("merge", "BQ"),
			destination:      getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ) missing mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge"}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesMissing.Error(),
						StatusCode: response.ErrMergePropertiesMissing.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge", "mergeProperties": "invalid"}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotArray.Error(),
						StatusCode: response.ErrMergePropertiesNotArray.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) empty mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge", "mergeProperties": []}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) single mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"}]}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid merge property one",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":["invalid",{"type":"email","value":"alex@example.com"}]}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyOneInvalid.Error(),
						StatusCode: response.ErrMergePropertyOneInvalid.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid merge property two",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},"invalid"]}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyTwoInvalid.Error(),
						StatusCode: response.ErrMergePropertyTwoInvalid.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) missing mergeProperty",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type1":"email","value1":"alex@example.com"},{"type1":"mobile","value1":"+1-202-555-0146"}]}`,
			metadata:     getMergeMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyEmpty.Error(),
						StatusCode: response.ErrMergePropertyEmpty.StatusCode(),
						Metadata:   getMergeMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (SNOWFLAKE)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`,
			metadata:     getMergeMetadata("merge", "SNOWFLAKE"),
			destination:  getDestination("SNOWFLAKE", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"MERGE_PROPERTY_1_TYPE":  "email",
								"MERGE_PROPERTY_1_VALUE": "alex@example.com",
								"MERGE_PROPERTY_2_TYPE":  "mobile",
								"MERGE_PROPERTY_2_VALUE": "+1-202-555-0146",
							},
							"metadata": map[string]any{
								"table":        "RUDDER_IDENTITY_MERGE_RULES",
								"columns":      map[string]any{"MERGE_PROPERTY_1_TYPE": "string", "MERGE_PROPERTY_1_VALUE": "string", "MERGE_PROPERTY_2_TYPE": "string", "MERGE_PROPERTY_2_VALUE": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "alex@example.com",
								"mergePropTwo": "+1-202-555-0146",
							},
							"userId": "",
						},
						Metadata:   getMergeMetadata("merge", "SNOWFLAKE"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMergeMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMergeMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getAliasDefaultMergeOutput(),
						Metadata:   getMergeMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) no userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMergeMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("user_id", "previous_id").
							RemoveColumnFields("user_id", "previous_id"),
						Metadata:   getMergeMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) empty userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"","previousId":"","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMergeMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getAliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("user_id", "previous_id").
							RemoveColumnFields("user_id", "previous_id"),
						Metadata:   getMergeMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMergeMetadata("page", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMergeMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getPageDefaultMergeOutput(),
						Metadata:   getMergeMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ) no anonymousID",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMergeMetadata("page", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getPageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("anonymous_id").
							RemoveColumnFields("anonymous_id"),
						Metadata:   getMergeMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
							"data": map[string]any{
								"merge_property_1_type":  "user_id",
								"merge_property_1_value": "userId",
							},
							"metadata": map[string]any{
								"table":        "rudder_identity_merge_rules",
								"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "userId",
							},
							"userId": "",
						},
						Metadata:   getMergeMetadata("page", "BQ"),
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

func getMergeMetadata(eventType, destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       eventType,
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		MessageID:       "messageId",
	}
}
