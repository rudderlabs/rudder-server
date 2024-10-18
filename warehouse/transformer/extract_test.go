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

func TestExtract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	transformerResource, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	testsCases := []struct {
		name             string
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "extract (Postgres)",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"name":                     "Home",
								"context_ip":               "1.2.3.4",
								"context_traits_email":     "rhedricks@example.com",
								"context_traits_logins":    float64(2),
								"context_traits_name":      "Richard Hendricks",
								"id":                       "recordID",
								"event":                    "event",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"title":                    "Home | RudderStack",
								"url":                      "https://www.rudderstack.com",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"context_ip":               "string",
									"context_traits_email":     "string",
									"context_traits_logins":    "int",
									"context_traits_name":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"title":                    "string",
									"url":                      "string",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without properties",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"context_traits_email":     "rhedricks@example.com",
								"context_traits_logins":    float64(2),
								"context_traits_name":      "Richard Hendricks",
								"id":                       "recordID",
								"event":                    "event",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"context_ip":               "string",
									"context_traits_email":     "string",
									"context_traits_logins":    "int",
									"context_traits_name":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without context",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"name":                     "Home",
								"id":                       "recordID",
								"event":                    "event",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"title":                    "Home | RudderStack",
								"url":                      "https://www.rudderstack.com",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"title":                    "string",
									"url":                      "string",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"name":                     "Home",
								"context_ip":               "1.2.3.4",
								"context_traits_email":     "rhedricks@example.com",
								"context_traits_logins":    float64(2),
								"context_traits_name":      "Richard Hendricks",
								"id":                       "recordID",
								"event":                    "accounts",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"title":                    "Home | RudderStack",
								"url":                      "https://www.rudderstack.com",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"context_ip":               "string",
									"context_traits_email":     "string",
									"context_traits_logins":    "int",
									"context_traits_name":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"title":                    "string",
									"url":                      "string",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "_accounts",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable with skipReservedKeywordsEscaping",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"name":                     "Home",
								"context_ip":               "1.2.3.4",
								"context_traits_email":     "rhedricks@example.com",
								"context_traits_logins":    float64(2),
								"context_traits_name":      "Richard Hendricks",
								"id":                       "recordID",
								"event":                    "accounts",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"title":                    "Home | RudderStack",
								"url":                      "https://www.rudderstack.com",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"context_ip":               "string",
									"context_traits_email":     "string",
									"context_traits_logins":    "int",
									"context_traits_name":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"title":                    "string",
									"url":                      "string",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "accounts",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderIsolatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "extract",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				RecordID:        "recordID",
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
								"name":                     "Home",
								"context_ip":               "1.2.3.4",
								"context_traits_email":     "rhedricks@example.com",
								"context_traits_logins":    float64(2),
								"context_traits_name":      "Richard Hendricks",
								"id":                       "recordID",
								"event":                    "users",
								"received_at":              "2021-09-01T00:00:00.000Z",
								"title":                    "Home | RudderStack",
								"url":                      "https://www.rudderstack.com",
								"context_destination_id":   "destinationID",
								"context_destination_type": "POSTGRES",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
									"context_destination_id":   "string",
									"context_destination_type": "string",
									"context_source_id":        "string",
									"context_source_type":      "string",
									"context_ip":               "string",
									"context_traits_email":     "string",
									"context_traits_logins":    "int",
									"context_traits_name":      "string",
									"id":                       "string",
									"event":                    "string",
									"received_at":              "datetime",
									"title":                    "string",
									"url":                      "string",
									"uuid_ts":                  "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "_users",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "extract",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							RecordID:        "recordID",
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
