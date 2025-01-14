package transformer

import (
	"fmt"
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
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

func TestExtract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	transformerResource, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	testCases := []struct {
		name             string
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "extract (Postgres)",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getExtractMetadata(),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getExtractDefaultOutput(),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without properties",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getExtractMetadata(),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getExtractDefaultOutput().
							RemoveDataFields("name", "title", "url").
							RemoveColumnFields("name", "title", "url"),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without context",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getExtractMetadata(),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getExtractDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getExtractMetadata(),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getExtractDefaultOutput().
							SetDataField("event", "accounts").
							SetTableName("_accounts"),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable with skipReservedKeywordsEscaping",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`,
			metadata:     getExtractMetadata(),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getExtractDefaultOutput().
							SetDataField("event", "accounts").
							SetTableName("accounts"),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderIsolatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getExtractMetadata(),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getExtractDefaultOutput().
							SetDataField("event", "users").
							SetTableName("_users"),
						Metadata:   getExtractMetadata(),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := setupConfig(transformerResource, map[string]any{})
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

func getExtractDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
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
	}
}

func getExtractMetadata() ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "extract",
		DestinationType: "POSTGRES",
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		RecordID:        "recordID",
	}
}

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

func TestScreen(t *testing.T) {
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
			name:         "screen (Postgres)",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getScreenMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getScreenDefaultOutput(),
						Metadata:   getScreenMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) without properties",
			eventPayload: `{"type":"screen","name":"Main","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getScreenMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getScreenDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getScreenMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) without context",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getScreenMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getScreenDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getScreenMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) store rudder event",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getScreenMetadata("POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getScreenDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"name\":\"Main\",\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"type\":\"screen\",\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getScreenMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) partial rules",
			eventPayload: `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getScreenMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getScreenDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getScreenMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "screen (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getScreenMetadata("BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getScreenDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getScreenMetadata("BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getScreenDefaultMergeOutput(),
						Metadata:   getScreenMetadata("BQ"),
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

func getScreenDefaultOutput() testhelper.OutputBuilder {
	return testhelper.OutputBuilder{
		"data": map[string]any{
			"name":                     "Main",
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
			"table":      "screens",
		},
		"userId": "",
	}
}

func getScreenDefaultMergeOutput() testhelper.OutputBuilder {
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

func getScreenMetadata(destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "screen",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
	}
}

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

func TestTrack(t *testing.T) {
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
			name:         "track (POSTGRES)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getTrackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without properties",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getTrackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without userProperties",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getTrackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without context",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderCreatedTable",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts").
							SetTableName("_accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderCreatedTable with skipReservedKeywordsEscaping",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts").
							SetTableName("accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderIsolatedTable",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"users","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("event", "users").
							SetDataField("event_text", "users"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("event", "users").
							SetDataField("event_text", "users").
							SetTableName("_users"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) empty event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("event", "").
							RemoveDataFields("event_text").
							RemoveColumnFields("event_text"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) no event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("event", "").
							RemoveDataFields("event_text").
							RemoveColumnFields("event_text"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) store rudder event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"track\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"}},\"event\":\"event\",\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"product_id\":\"9578257311\",\"review_id\":\"86ac1cd43\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"userId\":\"userId\",\"userProperties\":{\"rating\":3,\"review_body\":\"OK for the price. It works but the material feels flimsy.\"}}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) partial rules",
			eventPayload: `{"type":"track","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","event":"event","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) skipTracksTable (dstOpts)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination: getDestination("POSTGRES", map[string]any{
				"skipTracksTable": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) skipTracksTable (itrOpts)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     getEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "track (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("BQ", "webhook"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getTrackMetadata("BQ", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getTrackMetadata("BQ", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     getTrackDefaultMergeOutput(),
						Metadata:   getTrackMetadata("BQ", "webhook"),
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

func getTrackDefaultOutput() testhelper.OutputBuilder {
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
			"event":                    "event",
			"event_text":               "event",
			"id":                       "messageId",
			"original_timestamp":       "2021-09-01T00:00:00.000Z",
			"received_at":              "2021-09-01T00:00:00.000Z",
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
				"event":                    "string",
				"event_text":               "string",
				"id":                       "string",
				"original_timestamp":       "datetime",
				"received_at":              "datetime",
				"sent_at":                  "datetime",
				"timestamp":                "datetime",
				"user_id":                  "string",
				"uuid_ts":                  "datetime",
			},
			"receivedAt": "2021-09-01T00:00:00.000Z",
			"table":      "tracks",
		},
		"userId": "",
	}
}

func getEventDefaultOutput() testhelper.OutputBuilder {
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
			"event":                    "event",
			"event_text":               "event",
			"id":                       "messageId",
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
				"event":                    "string",
				"event_text":               "string",
				"id":                       "string",
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
			"table":      "event",
		},
		"userId": "",
	}
}

func getTrackDefaultMergeOutput() testhelper.OutputBuilder {
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

func getTrackMetadata(destinationType, sourceCategory string) ptrans.Metadata {
	return ptrans.Metadata{
		EventType:       "track",
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		SourceCategory:  sourceCategory,
		MessageID:       "messageId",
	}
}

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

func TestTransformer(t *testing.T) {
	testCases := []struct {
		name             string
		configOverride   map[string]any
		envOverride      []string
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "Unknown event",
			eventPayload: `{"type":"unknown"}`,
			metadata:     getMetadata("unknown", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      "Unknown event type: \"unknown\"",
						Metadata:   getMetadata("unknown", "POSTGRES"),
						StatusCode: http.StatusBadRequest,
					},
				},
			},
		},
		// TODO: Enable this once we have the https://github.com/rudderlabs/rudder-transformer/pull/3806 changes in latest
		//{
		//	name: "Not populateSrcDestInfoInContext",
		//	configOverride: map[string]any{
		//		"Warehouse.populateSrcDestInfoInContext": false,
		//	},
		//	envOverride:  []string{"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT=false"},
		//	eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
		//	metadata:     getMetadata("track", "POSTGRES"),
		//	destination:  getDestination("POSTGRES", map[string]any{}),
		//	expectedResponse: ptrans.Response{
		//		Events: []ptrans.TransformerResponse{
		//			{
		//				Output: getTrackDefaultOutput().
		//					RemoveDataFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type").
		//					RemoveColumnFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type"),
		//				Metadata:   getMetadata("track", "POSTGRES"),
		//				StatusCode: http.StatusOK,
		//			},
		//			{
		//				Output: getEventDefaultOutput().
		//					RemoveDataFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type").
		//					RemoveColumnFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type"),
		//				Metadata:   getMetadata("track", "POSTGRES"),
		//				StatusCode: http.StatusOK,
		//			},
		//		},
		//	},
		//},
		{
			name:         "Too many columns",
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","context":{%s},"ip":"1.2.3.4"}`, 500),
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      "postgres transformer: Too many columns outputted from the event",
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusBadRequest,
					},
				},
			},
		},
		{
			name:         "Too many columns (DataLake)",
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500),
			metadata:     getMetadata("track", "GCS_DATALAKE"),
			destination:  getDestination("GCS_DATALAKE", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "GCS_DATALAKE"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "GCS_DATALAKE"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "Too many columns channel as sources",
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500),
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "StringLikeObject for context traits",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"0":"a","1":"b","2":"c"},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name").
							SetDataField("context_traits", "abc").
							SetColumnField("context_traits", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name").
							SetDataField("context_traits", "abc").
							SetColumnField("context_traits", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "StringLikeObject for group traits",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"0":"a","1":"b","2":"c"},"ip":"1.2.3.4"}}`,
			metadata:     getGroupMetadata("POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getGroupDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name").
							SetDataField("context_traits", "abc").
							SetColumnField("context_traits", "string"),
						Metadata:   getGroupMetadata("POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "Not StringLikeObject for context properties",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"0":"a","1":"b","2":"c"},"userProperties":{"rating":3,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id").
							SetDataField("_0", "a").SetColumnField("_0", "string").
							SetDataField("_1", "b").SetColumnField("_1", "string").
							SetDataField("_2", "c").SetColumnField("_2", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "context, properties and userProperties as null",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":null,"userProperties":null,"context":null}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "product_id", "rating", "review_body", "review_id").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "product_id", "rating", "review_body", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "context, properties and userProperties as not a object",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":"properties","userProperties":"userProperties","context":"context"}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrContextNotMap.Error(),
						StatusCode: response.ErrContextNotMap.StatusCode(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
					},
				},
			},
		},
		{
			name:         "context, properties and userProperties as empty map",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{},"userProperties":{},"context":{}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "product_id", "rating", "review_body", "review_id").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "product_id", "rating", "review_body", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "Nested object level with no limit when source category is not cloud",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2,"location":{"city":"Palo Alto","state":"California","country":"USA","coordinates":{"latitude":37.4419,"longitude":-122.143,"geo":{"altitude":30.5,"accuracy":5,"details":{"altitudeUnits":"meters","accuracyUnits":"meters"}}}}},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_traits_location_city", "Palo Alto").
							SetDataField("context_traits_location_state", "California").
							SetDataField("context_traits_location_country", "USA").
							SetDataField("context_traits_location_coordinates_latitude", 37.4419).
							SetDataField("context_traits_location_coordinates_longitude", -122.143).
							SetDataField("context_traits_location_coordinates_geo_altitude", 30.5).
							SetDataField("context_traits_location_coordinates_geo_accuracy", 5.0).
							SetDataField("context_traits_location_coordinates_geo_details_altitude_units", "meters").
							SetDataField("context_traits_location_coordinates_geo_details_accuracy_units", "meters").
							SetColumnField("context_traits_location_city", "string").
							SetColumnField("context_traits_location_state", "string").
							SetColumnField("context_traits_location_country", "string").
							SetColumnField("context_traits_location_coordinates_latitude", "float").
							SetColumnField("context_traits_location_coordinates_longitude", "float").
							SetColumnField("context_traits_location_coordinates_geo_altitude", "float").
							SetColumnField("context_traits_location_coordinates_geo_accuracy", "int").
							SetColumnField("context_traits_location_coordinates_geo_details_altitude_units", "string").
							SetColumnField("context_traits_location_coordinates_geo_details_accuracy_units", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_traits_location_city", "Palo Alto").
							SetDataField("context_traits_location_state", "California").
							SetDataField("context_traits_location_country", "USA").
							SetDataField("context_traits_location_coordinates_latitude", 37.4419).
							SetDataField("context_traits_location_coordinates_longitude", -122.143).
							SetDataField("context_traits_location_coordinates_geo_altitude", 30.5).
							SetDataField("context_traits_location_coordinates_geo_accuracy", 5.0).
							SetDataField("context_traits_location_coordinates_geo_details_altitude_units", "meters").
							SetDataField("context_traits_location_coordinates_geo_details_accuracy_units", "meters").
							SetColumnField("context_traits_location_city", "string").
							SetColumnField("context_traits_location_state", "string").
							SetColumnField("context_traits_location_country", "string").
							SetColumnField("context_traits_location_coordinates_latitude", "float").
							SetColumnField("context_traits_location_coordinates_longitude", "float").
							SetColumnField("context_traits_location_coordinates_geo_altitude", "float").
							SetColumnField("context_traits_location_coordinates_geo_accuracy", "int").
							SetColumnField("context_traits_location_coordinates_geo_details_altitude_units", "string").
							SetColumnField("context_traits_location_coordinates_geo_details_accuracy_units", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "Nested object level limits to 3 when source category is cloud",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2,"location":{"city":"Palo Alto","state":"California","country":"USA","coordinates":{"latitude":37.4419,"longitude":-122.143,"geo":{"altitude":30.5,"accuracy":5,"details":{"altitudeUnits":"meters","accuracyUnits":"meters"}}}}},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "cloud"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: getTrackDefaultOutput().
							SetDataField("context_traits_location_city", "Palo Alto").
							SetDataField("context_traits_location_state", "California").
							SetDataField("context_traits_location_country", "USA").
							SetDataField("context_traits_location_coordinates_latitude", 37.4419).
							SetDataField("context_traits_location_coordinates_longitude", -122.143).
							SetDataField("context_traits_location_coordinates_geo", `{"accuracy":5,"altitude":30.5,"details":{"accuracyUnits":"meters","altitudeUnits":"meters"}}`).
							SetColumnField("context_traits_location_city", "string").
							SetColumnField("context_traits_location_state", "string").
							SetColumnField("context_traits_location_country", "string").
							SetColumnField("context_traits_location_coordinates_latitude", "float").
							SetColumnField("context_traits_location_coordinates_geo", "string").
							SetColumnField("context_traits_location_coordinates_longitude", "float"),
						Metadata:   getTrackMetadata("POSTGRES", "cloud"),
						StatusCode: http.StatusOK,
					},
					{
						Output: getEventDefaultOutput().
							SetDataField("context_traits_location_city", "Palo Alto").
							SetDataField("context_traits_location_state", "California").
							SetDataField("context_traits_location_country", "USA").
							SetDataField("context_traits_location_coordinates_latitude", 37.4419).
							SetDataField("context_traits_location_coordinates_longitude", -122.143).
							SetDataField("context_traits_location_coordinates_geo", `{"accuracy":5,"altitude":30.5,"details":{"accuracyUnits":"meters","altitudeUnits":"meters"}}`).
							SetColumnField("context_traits_location_city", "string").
							SetColumnField("context_traits_location_state", "string").
							SetColumnField("context_traits_location_country", "string").
							SetColumnField("context_traits_location_coordinates_latitude", "float").
							SetColumnField("context_traits_location_coordinates_geo", "string").
							SetColumnField("context_traits_location_coordinates_longitude", "float"),
						Metadata:   getTrackMetadata("POSTGRES", "cloud"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			var opts []transformertest.Option
			for _, envOverride := range tc.envOverride {
				opts = append(opts, transformertest.WithEnv(envOverride))
			}
			transformerResource, err := transformertest.Setup(pool, t, opts...)
			require.NoError(t, err)

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

func setupConfig(resource *transformertest.Resource, configOverride map[string]any) *config.Config {
	c := config.New()
	c.Set("DEST_TRANSFORM_URL", resource.TransformerURL)
	c.Set("USER_TRANSFORM_URL", resource.TransformerURL)

	for k, v := range configOverride {
		c.Set(k, v)
	}
	return c
}

func getDestination(destinationType string, config map[string]any) backendconfig.DestinationT {
	return backendconfig.DestinationT{
		Name:   destinationType,
		Config: config,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: destinationType,
		},
	}
}

func getMetadata(eventType, destinationType string) ptrans.Metadata {
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

func TestGetDataType(t *testing.T) {
	testCases := []struct {
		name, destType, key string
		val                 any
		isJSONKey           bool
		expected            string
	}{
		// Primitive types
		{"Primitive Type Int", whutils.POSTGRES, "someKey", 42, false, "int"},
		{"Primitive Type Float", whutils.POSTGRES, "someKey", 42.0, false, "int"},
		{"Primitive Type Float (non-int)", whutils.POSTGRES, "someKey", 42.5, false, "float"},
		{"Primitive Type Bool", whutils.POSTGRES, "someKey", true, false, "boolean"},

		// Valid timestamp
		{"Valid Timestamp String", whutils.POSTGRES, "someKey", "2022-10-05T14:48:00.000Z", false, "datetime"},

		// JSON Key cases for different destinations
		{"Postgres JSON Key", whutils.POSTGRES, "someKey", "someValue", true, "json"},
		{"Snowflake JSON Key", whutils.SNOWFLAKE, "someKey", "someValue", true, "json"},
		{"Redshift JSON Key", whutils.RS, "someKey", "someValue", true, "json"},

		// Redshift with text and string types
		{"Redshift Text Type", whutils.RS, "someKey", string(make([]byte, 513)), false, "text"},
		{"Redshift String Type", whutils.RS, "someKey", "shortValue", false, "string"},
		{"Redshift String Type", whutils.RS, "someKey", nil, false, "string"},

		// Empty string values
		{"Empty String Value", whutils.POSTGRES, "someKey", "", false, "string"},
		{"Empty String with JSON Key", whutils.POSTGRES, "someKey", "", true, "json"},

		// Unsupported types (should default to string)
		{"Unsupported Type Struct", whutils.POSTGRES, "someKey", struct{}{}, false, "string"},
		{"Unsupported Type Map", whutils.POSTGRES, "someKey", map[string]any{"key": "value"}, false, "string"},

		// Special string values
		{"Special Timestamp-like String", whutils.POSTGRES, "someKey", "not-a-timestamp", false, "string"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destinationTransformer := &transformer{}

			actual := destinationTransformer.getDataType(tc.destType, tc.key, tc.val, tc.isJSONKey)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestGetColumns(t *testing.T) {
	testCases := []struct {
		name        string
		destType    string
		data        map[string]any
		columnTypes map[string]string
		maxColumns  int32
		expected    map[string]any
		wantError   bool
	}{
		{
			name:     "Basic data types",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean",
			},
		},
		{
			name:     "Basic data types (BQ)",
			destType: whutils.BQ,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean", "loaded_at": "datetime",
			},
		},
		{
			name:     "Basic data types (SNOWFLAKE)",
			destType: whutils.SNOWFLAKE,
			data: map[string]any{
				"FIELD1": "value1", "FIELD2": 123, "FIELD3": true,
			},
			columnTypes: map[string]string{
				"FIELD1": "string", "FIELD2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"UUID_TS": "datetime", "FIELD1": "string", "FIELD2": "int", "FIELD3": "boolean",
			},
		},
		{
			name:     "Key not in columnTypes",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{},
			maxColumns:  10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean",
			},
		},
		{
			name:     "Too many columns",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true, "field4": "extra",
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 3,
			expected:   nil,
			wantError:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			trans := &transformer{}
			trans.config.maxColumnsInEvent = config.SingleValueLoader(int(tc.maxColumns))

			columns, err := trans.getColumns(tc.destType, tc.data, tc.columnTypes)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, columns)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, columns)
		})
	}
}

func TestIntegrationOptions(t *testing.T) {
	t.Run("AllOptionsSet", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"destinationType": map[string]any{
						"options": map[string]any{
							"skipReservedKeywordsEscaping": true,
							"useBlendoCasing":              false,
							"skipTracksTable":              true,
							"skipUsersTable":               false,
							"jsonPaths":                    []any{"path1", "path2", "path3"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}

		opts := prepareIntegrationOptions(event)

		require.True(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Equal(t, []string{"path1", "path2", "path3"}, opts.jsonPaths)
	})
	t.Run("MissingOptions", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"destinationType": map[string]any{
						"options": map[string]any{},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}
		opts := prepareIntegrationOptions(event)

		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("NilIntegrationOptions", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"destinationType": map[string]any{
						"options": nil,
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}
		opts := prepareIntegrationOptions(event)

		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("PartialOptionsSet", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"destinationType": map[string]any{
						"options": map[string]any{
							"skipUsersTable": true,
							"jsonPaths":      []any{"path1"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}

		opts := prepareIntegrationOptions(event)

		require.True(t, opts.skipUsersTable)
		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.Equal(t, []string{"path1"}, opts.jsonPaths)
	})
}

func TestDestinationOptions(t *testing.T) {
	t.Run("AllOptionsSet", func(t *testing.T) {
		destConfig := map[string]any{
			"skipTracksTable":         true,
			"skipUsersTable":          false,
			"underscoreDivideNumbers": true,
			"allowUsersContextTraits": false,
			"storeFullEvent":          true,
			"jsonPaths":               "path1,path2",
		}

		opts := prepareDestinationOptions(whutils.POSTGRES, destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.True(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.True(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
	t.Run("MissingOptions", func(t *testing.T) {
		destConfig := map[string]any{}

		opts := prepareDestinationOptions(whutils.POSTGRES, destConfig)

		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("NilDestinationConfig", func(t *testing.T) {
		opts := prepareDestinationOptions(whutils.POSTGRES, nil)

		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("PartialOptionsSet", func(t *testing.T) {
		destConfig := map[string]any{
			"skipTracksTable":         true,
			"jsonPaths":               "path1,path2",
			"allowUsersContextTraits": true,
		}

		opts := prepareDestinationOptions(whutils.POSTGRES, destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.True(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
	t.Run("JSONPathSupported", func(t *testing.T) {
		destConfig := map[string]any{
			"jsonPaths": "path1,path2",
		}

		require.Equal(t, []string{"path1", "path2"}, prepareDestinationOptions(whutils.POSTGRES, destConfig).jsonPaths)
		require.Empty(t, prepareDestinationOptions(whutils.CLICKHOUSE, destConfig).jsonPaths)
	})
}

func TestExtractJSONPathInfo(t *testing.T) {
	testCases := []struct {
		name      string
		jsonPaths []string
		expected  jsonPathInfo
	}{
		{
			name:      "Valid JSON paths with track prefix",
			jsonPaths: []string{"track.properties.name", "track.properties.age", "properties.name", "properties.age"},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"track_properties_name": 2, "track_properties_age": 2},
				legacyKeysMap: map[string]int{"properties_name": 1, "properties_age": 1},
			},
		},
		{
			name:      "Valid JSON paths with identify prefix",
			jsonPaths: []string{"identify.traits.address.city", "identify.traits.address.zip", "traits.address.city", "traits.address.zip"},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"identify_traits_address_city": 3, "identify_traits_address_zip": 3},
				legacyKeysMap: map[string]int{"traits_address_city": 2, "traits_address_zip": 2},
			},
		},
		{
			name:      "Whitespace and empty path",
			jsonPaths: []string{"   ", "track.properties.name", ""},
			expected: jsonPathInfo{
				keysMap:       map[string]int{"track_properties_name": 2},
				legacyKeysMap: make(map[string]int),
			},
		},
		{
			name:      "Unknown prefix JSON paths",
			jsonPaths: []string{"unknown.prefix.eventType.name", "unknown.prefix.eventType.value"},
			expected: jsonPathInfo{
				keysMap:       make(map[string]int),
				legacyKeysMap: map[string]int{"unknown_prefix_eventType_name": 3, "unknown_prefix_eventType_value": 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, extractJSONPathInfo(tc.jsonPaths))
		})
	}
}

func TestIsValidJSONPathKey(t *testing.T) {
	testCases := []struct {
		name, key string
		level     int
		isValid   bool
	}{
		{
			name:    "Valid JSON path key with track prefix",
			key:     "track_properties_name",
			level:   2,
			isValid: true,
		},
		{
			name:    "Valid JSON path key with identify prefix",
			key:     "identify_traits_address_city",
			level:   3,
			isValid: true,
		},
		{
			name:    "Valid JSON path key with unknown prefix",
			key:     "unknown_prefix_eventType_name",
			level:   3,
			isValid: false,
		},
		{
			name:    "Invalid JSON path key",
			key:     "invalid_key",
			level:   0,
			isValid: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathsInfo := extractJSONPathInfo(
				[]string{
					"track.properties.name", "properties.name",
					"identify.traits.address.city", "traits.address.city",
					"unknown.prefix.eventType.name",
				},
			)
			require.Equal(t, tc.isValid, isValidJSONPathKey(tc.key, tc.level, pathsInfo.keysMap))
		})
	}
}

func TestIsValidLegacyJSONPathKey(t *testing.T) {
	testCases := []struct {
		name, key, eventType string
		level                int
		isValid              bool
	}{
		{
			name:      "Valid JSON path key with track prefix",
			key:       "properties_name",
			eventType: "track",
			level:     1,
			isValid:   true,
		},
		{
			name:      "Valid JSON path key with identify prefix",
			key:       "traits_address_city",
			eventType: "identify",
			level:     2,
			isValid:   false,
		},
		{
			name:      "Valid JSON path key with unknown prefix",
			key:       "unknown_prefix_eventType_name",
			eventType: "track",
			level:     3,
			isValid:   true,
		},
		{
			name:      "Invalid JSON path key",
			key:       "invalid_key",
			eventType: "track",
			level:     0,
			isValid:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathsInfo := extractJSONPathInfo(
				[]string{
					"track.properties.name", "properties.name",
					"identify.traits.address.city", "traits.address.city",
					"unknown.prefix.eventType.name",
				},
			)
			require.Equal(t, tc.isValid, isValidLegacyJSONPathKey(tc.eventType, tc.key, tc.level, pathsInfo.legacyKeysMap))
		})
	}
}
