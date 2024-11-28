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

func TestIdentify(t *testing.T) {
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
			name:         "identify (POSTGRES)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getIdentifyDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getUserDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (S3_DATALAKE)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("S3_DATALAKE"),
			destination: getDestination("S3_DATALAKE", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							SetDataField("_timestamp", "2021-09-01T00:00:00.000Z").
							SetColumnField("_timestamp", "datetime").
							RemoveDataFields("timestamp").
							RemoveColumnFields("timestamp").
							SetDataField("context_destination_type", "S3_DATALAKE"),
						Metadata:   getIdentifyMetadata("S3_DATALAKE"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("timestamp", "original_timestamp", "sent_at").
							RemoveColumnFields("timestamp", "original_timestamp", "sent_at").
							SetDataField("context_destination_type", "S3_DATALAKE"),
						Metadata:   getIdentifyMetadata("S3_DATALAKE"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without traits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without userProperties",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without context.traits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without context",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) not allowUsersContextTraits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("email", "logins", "name").
							RemoveColumnFields("email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("email", "logins", "name").
							RemoveColumnFields("email", "logins", "name"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) user_id already exists",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"user_id":"user_id","rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getIdentifyDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getUserDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) store rudder event",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent":          true,
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"product_id\":\"9578257311\",\"review_id\":\"86ac1cd43\"},\"type\":\"identify\",\"userId\":\"userId\",\"userProperties\":{\"rating\":3,\"review_body\":\"OK for the price. It works but the material feels flimsy.\"}}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getUserDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) partial rules",
			eventPayload: `{"type":"identify","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							RemoveDataFields("context_request_ip").
							RemoveColumnFields("context_request_ip"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) no userID",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							RemoveDataFields("user_id").
							RemoveColumnFields("user_id"),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) skipUsersTable (dstOpts)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: backendconfig.DestinationT{
				Name: "POSTGRES",
				Config: map[string]any{
					"allowUsersContextTraits": true,
					"skipUsersTable":          true,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getIdentifyDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) skipUsersTable (itrOpts)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`,
			metadata:     getIdentifyMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getIdentifyDefaultOutput(),
						Metadata:   getIdentifyMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "identify (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`,
			metadata:     getIdentifyMetadata("BQ"),
			destination: getDestination("BQ", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getIdentifyDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getIdentifyMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getIdentifyDefaultMergeOutput(),
						Metadata:   getIdentifyMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getUserDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getIdentifyMetadata("BQ"),
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

func getIdentifyDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"anonymous_id":             "anonymousId",
			"channel":                  "web",
			"context_destination_id":   "destinationID",
			"context_destination_type": "POSTGRES",
			"context_ip":               "1.2.3.4",
			"context_passed_ip":        "1.2.3.4",
			"context_request_ip":       "5.6.7.8",
			"context_source_id":        "sourceID",
			"context_source_type":      "sourceType",
			"context_traits_email":     "rhedricks@example.com",
			"context_traits_logins":    float64(2),
			"context_traits_name":      "Richard Hendricks",
			"email":                    "rhedricks@example.com",
			"id":                       "messageId",
			"logins":                   float64(2),
			"name":                     "Richard Hendricks",
			"original_timestamp":       "2021-09-01T00:00:00.000Z",
			"product_id":               "9578257311",
			"rating":                   3.0,
			"received_at":              "2021-09-01T00:00:00.000Z",
			"review_body":              "OK for the price. It works but the material feels flimsy.",
			"review_id":                "86ac1cd43",
			"sent_at":                  "2021-09-01T00:00:00.000Z",
			"timestamp":                "2021-09-01T00:00:00.000Z",
			"user_id":                  "userId",
		},
		"metadata": map[string]any{
			"columns": map[string]any{
				"anonymous_id":             "string",
				"channel":                  "string",
				"context_destination_id":   "string",
				"context_destination_type": "string",
				"context_ip":               "string",
				"context_passed_ip":        "string",
				"context_request_ip":       "string",
				"context_source_id":        "string",
				"context_source_type":      "string",
				"context_traits_email":     "string",
				"context_traits_logins":    "int",
				"context_traits_name":      "string",
				"email":                    "string",
				"id":                       "string",
				"logins":                   "int",
				"name":                     "string",
				"original_timestamp":       "datetime",
				"product_id":               "string",
				"rating":                   "int",
				"received_at":              "datetime",
				"review_body":              "string",
				"review_id":                "string",
				"sent_at":                  "datetime",
				"timestamp":                "datetime",
				"user_id":                  "string",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "identifies",
		},
		"userId": "",
	}
}

func getUserDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"context_destination_id":   "destinationID",
			"context_destination_type": "POSTGRES",
			"context_ip":               "1.2.3.4",
			"context_passed_ip":        "1.2.3.4",
			"context_request_ip":       "5.6.7.8",
			"context_source_id":        "sourceID",
			"context_source_type":      "sourceType",
			"context_traits_email":     "rhedricks@example.com",
			"context_traits_logins":    float64(2),
			"context_traits_name":      "Richard Hendricks",
			"email":                    "rhedricks@example.com",
			"id":                       "userId",
			"logins":                   float64(2),
			"name":                     "Richard Hendricks",
			"original_timestamp":       "2021-09-01T00:00:00.000Z",
			"product_id":               "9578257311",
			"rating":                   3.0,
			"received_at":              "2021-09-01T00:00:00.000Z",
			"review_body":              "OK for the price. It works but the material feels flimsy.",
			"review_id":                "86ac1cd43",
			"sent_at":                  "2021-09-01T00:00:00.000Z",
			"timestamp":                "2021-09-01T00:00:00.000Z",
		},
		"metadata": map[string]any{
			"columns": map[string]any{
				"context_destination_id":   "string",
				"context_destination_type": "string",
				"context_ip":               "string",
				"context_passed_ip":        "string",
				"context_request_ip":       "string",
				"context_source_id":        "string",
				"context_source_type":      "string",
				"context_traits_email":     "string",
				"context_traits_logins":    "int",
				"context_traits_name":      "string",
				"email":                    "string",
				"id":                       "string",
				"logins":                   "int",
				"name":                     "string",
				"original_timestamp":       "datetime",
				"product_id":               "string",
				"rating":                   "int",
				"received_at":              "datetime",
				"review_body":              "string",
				"review_id":                "string",
				"sent_at":                  "datetime",
				"timestamp":                "datetime",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "users",
		},
		"userId": "",
	}
}

func getIdentifyDefaultMergeOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"merge_property_1_type":  "anonymous_id",
			"merge_property_1_value": "anonymousId",
			"merge_property_2_type":  "user_id",
			"merge_property_2_value": "userId",
		},
		"metadata": map[string]any{
			"columns": map[string]any{
				"merge_property_1_type":  "string",
				"merge_property_1_value": "string",
				"merge_property_2_type":  "string",
				"merge_property_2_value": "string",
			},
			"isMergeRule":  true,
			"mergePropOne": "anonymousId",
			"mergePropTwo": "userId",
			"receivedAt":   "2021-09-01T00:00:00.000Z",
			"table":        "rudder_identity_merge_rules",
		},
		"userId": "",
	}
}

func getIdentifyMetadata(destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "identify",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		MessageID:       "messageId",
	}
}
