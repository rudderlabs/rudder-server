package transformer

import (
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

func TestAlias(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	transformerResource, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	testsCases := []struct {
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
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
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
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without traits",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
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
									"user_id":                  "string",
									"previous_id":              "string",
									"uuid_ts":                  "datetime",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "aliases",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without context",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"anonymous_id":             "anonymousId",
								"channel":                  "web",
								"context_ip":               "5.6.7.8",
								"context_request_ip":       "5.6.7.8",
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
									"context_ip":               "string",
									"context_request_ip":       "string",
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
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "aliases",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) store rudder event",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name: "POSTGRES",
				Config: map[string]any{
					"storeFullEvent": true,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
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
								"rudder_event":             "{\"type\":\"alias\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2}},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"previousId\":\"previousId\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"userId\":\"userId\"}",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"anonymous_id":             "string",
									"channel":                  "string",
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
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"rudder_event":             "json",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "aliases",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) partial rules",
			eventPayload: `{"type":"alias","messageId":"messageId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"context_ip":               "1.2.3.4",
								"context_passed_ip":        "1.2.3.4",
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
									"context_ip":               "string",
									"context_passed_ip":        "string",
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
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "aliases",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "alias",
				DestinationType: "BQ",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "BQ",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "BQ",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
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
								"context_destination_type": "BQ",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"anonymous_id":             "string",
									"channel":                  "string",
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
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"loaded_at":                "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "aliases",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
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
						},
						Metadata: ptrans.Metadata{
							EventType:       "alias",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}

	for _, tc := range testsCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("DEST_TRANSFORM_URL", transformerResource.TransformerURL)
			c.Set("USER_TRANSFORM_URL", transformerResource.TransformerURL)

			for k, v := range tc.configOverride {
				c.Set(k, v)
			}

			eventsInfos := []eventsInfo{
				{
					payload:     []byte(tc.eventPayload),
					metadata:    tc.metadata,
					destination: tc.destination,
				},
			}
			destinationTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
			warehouseTransformer := New(c, logger.NOP, stats.NOP)

			testEvents(t, eventsInfos, destinationTransformer, warehouseTransformer, tc.expectedResponse)
		})
	}
}
