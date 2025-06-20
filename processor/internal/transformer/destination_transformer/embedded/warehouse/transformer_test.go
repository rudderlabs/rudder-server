package warehouse_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/response"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/testhelper"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestTransformer(t *testing.T) {
	trackDefaultOutput := func() testhelper.OutputBuilder {
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
	trackEventDefaultOutput := func() testhelper.OutputBuilder {
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
	groupDefaultOutput := func() testhelper.OutputBuilder {
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
	maxColumnsCount := 1800

	testCases := []struct {
		name             string
		configOverride   map[string]any
		envOverride      []string
		eventPayload     string
		metadata         types.Metadata
		destination      backendconfig.DestinationT
		expectedResponse types.Response
	}{
		{
			name:         "Unknown event type",
			eventPayload: `{"type":"unknown"}`,
			metadata:     getMetadata("unknown", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "Unknown event type: \"unknown\"",
						Metadata:   getMetadata("unknown", "POSTGRES"),
						StatusCode: http.StatusBadRequest,
					},
				},
			},
		},
		{
			name:         "Undefined event type",
			eventPayload: `{}`,
			metadata:     getMetadata("", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "No event type: \"\"",
						Metadata:   getMetadata("", "POSTGRES"),
						StatusCode: http.StatusBadRequest,
					},
				},
			},
		},
		{
			name:         "Undefined event type but present in metadata",
			eventPayload: `{}`,
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "No event type: \"\"",
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusBadRequest,
					},
				},
			},
		},
		{
			name: "Not populateSrcDestInfoInContext",
			configOverride: map[string]any{
				"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT": false,
			},
			envOverride:  []string{"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT=false"},
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
							RemoveDataFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type").
							RemoveColumnFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type"),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							RemoveDataFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type").
							RemoveColumnFields("context_destination_id", "context_destination_type", "context_source_id", "context_source_type"),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "Too many columns",
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","context":{%s},"ip":"1.2.3.4"}`, maxColumnsCount),
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				FailedEvents: []types.TransformerResponse{
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
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, maxColumnsCount),
			metadata:     getMetadata("track", "GCS_DATALAKE"),
			destination:  getDestination("GCS_DATALAKE", map[string]any{}),
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(maxColumnsCount, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "GCS_DATALAKE"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(maxColumnsCount, func(index int) (string, string, string, string) {
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
			eventPayload: testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, maxColumnsCount),
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(maxColumnsCount, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(maxColumnsCount, func(index int) (string, string, string, string) {
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name").
							SetDataField("context_traits", "abc").
							SetColumnField("context_traits", "string"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
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
			metadata:     getMetadata("group", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: groupDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name").
							SetDataField("context_traits", "abc").
							SetColumnField("context_traits", "string"),
						Metadata:   getMetadata("group", "POSTGRES"),
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
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
			expectedResponse: types.Response{
				FailedEvents: []types.TransformerResponse{
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8").
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
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
						Output: trackEventDefaultOutput().
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
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: trackDefaultOutput().
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
						Output: trackEventDefaultOutput().
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
		{
			name:         "Empty slice and object",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4", "empty_slice":[],"empty_object":{},"nil":null}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: types.Response{
				Events: []types.TransformerResponse{
					{
						Output:     trackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     trackEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
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

			transformerResource, err := transformertest.Setup(pool, t, lo.Map(tc.envOverride, func(item string, index int) transformertest.Option {
				return transformertest.WithEnv(item)
			})...)
			require.NoError(t, err)

			c := setupConfig(transformerResource, tc.configOverride)

			processorTransformer := destination_transformer.New(c, logger.NOP, stats.Default)
			warehouseTransformer := warehouse.New(c, logger.NOP, stats.NOP)

			eventContexts := []testhelper.EventContext{
				{
					Payload:     []byte(tc.eventPayload),
					Metadata:    tc.metadata,
					Destination: tc.destination,
				},
			}
			testhelper.ValidateEvents(t, eventContexts, processorTransformer, warehouseTransformer, tc.expectedResponse)
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

func getMetadata(eventType, destinationType string) types.Metadata {
	return types.Metadata{
		EventType:       eventType,
		DestinationType: destinationType,
		ReceivedAt:      "2021-09-01T00:00:00.000Z",
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		MessageID:       "messageId",
		RecordID:        "recordID",
	}
}

func getTrackMetadata(destinationType, sourceCategory string) types.Metadata {
	metadata := getMetadata("track", destinationType)
	metadata.SourceCategory = sourceCategory
	return metadata
}
