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
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
)

func TestMerge(t *testing.T) {
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
			name: "merge (Postgres)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge"}`,
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`,
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
						Metadata: ptrans.Metadata{
							EventType:       "merge",
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
		{
			name: "merge (BQ) not enableIDResolution",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": false,
			},
			eventPayload: `{"type":"merge"}`,
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ) missing mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge"}`,
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesMissing.Error(),
						StatusCode: response.ErrMergePropertiesMissing.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotArray.Error(),
						StatusCode: response.ErrMergePropertiesNotArray.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyOneInvalid.Error(),
						StatusCode: response.ErrMergePropertyOneInvalid.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyTwoInvalid.Error(),
						StatusCode: response.ErrMergePropertyTwoInvalid.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
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
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyEmpty.Error(),
						StatusCode: response.ErrMergePropertyEmpty.StatusCode(),
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "BQ",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
						},
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
			metadata: ptrans.Metadata{
				EventType:       "merge",
				DestinationType: "SNOWFLAKE",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
			},
			destination: backendconfig.DestinationT{
				Name:   "SNOWFLAKE",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "SNOWFLAKE",
				},
			},
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
						Metadata: ptrans.Metadata{
							EventType:       "merge",
							DestinationType: "SNOWFLAKE",
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
			name: "alias (BQ)",
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
		{
			name: "alias (BQ) no userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
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
								"context_destination_id":   "destinationID",
								"context_destination_type": "BQ",
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
									"uuid_ts":                  "datetime",
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
				},
			},
		},
		{
			name: "alias (BQ) empty userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"","previousId":"","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
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
								"context_destination_id":   "destinationID",
								"context_destination_type": "BQ",
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
									"uuid_ts":                  "datetime",
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
				},
			},
		},
		{
			name: "page (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "page",
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
								"context_destination_type": "BQ",
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
									"loaded_at":                "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "pages",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "page",
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
						},
						Metadata: ptrans.Metadata{
							EventType:       "page",
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
		{
			name: "page (BQ) no anonymousID",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "page",
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
								"name":                     "Home",
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
								"context_destination_type": "BQ",
								"context_source_id":        "sourceID",
								"context_source_type":      "sourceType",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"name":                     "string",
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
									"loaded_at":                "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "pages",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "page",
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
						Metadata: ptrans.Metadata{
							EventType:       "page",
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
