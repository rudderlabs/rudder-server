package transformer

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
		{
			name: "Not populateSrcDestInfoInContext",
			configOverride: map[string]any{
				"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT": false,
			},
			envOverride:  []string{"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT=false"},
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("track", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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
						Output: trackDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "GCS_DATALAKE"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().SetDataField("context_destination_type", "GCS_DATALAKE").AddRandomEntries(500, func(index int) (string, string, string, string) {
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
						Output: trackDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(500, func(index int) (string, string, string, string) {
							return fmt.Sprintf("context_random_column_%d", index), fmt.Sprintf("random_value_%d", index), fmt.Sprintf("context_random_column_%d", index), "string"
						}),
						Metadata:   getMetadata("track", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().SetDataField("channel", "sources").AddRandomEntries(500, func(index int) (string, string, string, string) {
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
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
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

			processorTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
			warehouseTransformer := New(c, logger.NOP, stats.NOP)

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

func getMetadata(eventType, destinationType string) ptrans.Metadata {
	return ptrans.Metadata{
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

func getTrackMetadata(destinationType, sourceCategory string) ptrans.Metadata {
	metadata := getMetadata("track", destinationType)
	metadata.SourceCategory = sourceCategory
	return metadata
}

func TestTransformer_CompareAndLog(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "transformer_compare_log.*.txt")
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	maxLoggedEvents := 10

	c := config.New()
	c.Set("Warehouse.maxLoggedEvents", maxLoggedEvents)

	statsStore, err := memstats.New()
	require.NoError(t, err)

	trans := New(c, logger.NOP, statsStore)
	trans.loggedFileName = tmpFile.Name()

	metadata := &ptrans.Metadata{
		SourceID:        "sourceID",
		DestinationID:   "destinationID",
		SourceType:      "sourceType",
		DestinationType: "destinationType",
	}

	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt, 50)
	for index := 0; index < 50; index++ {
		eventsByMessageID[strconv.Itoa(index)] = types.SingularEventWithReceivedAt{
			SingularEvent: map[string]interface{}{
				"event": "track" + strconv.Itoa(index),
			},
		}
	}

	events := []ptrans.TransformerEvent{
		{
			Message: types.SingularEventT{
				"event":      "track",
				"context":    "context",
				"properties": "properties",
			},
		},
	}

	for i := 0; i < 1000; i++ {
		pResponse := ptrans.Response{
			Events: lo.RepeatBy(50, func(index int) ptrans.TransformerResponse {
				return ptrans.TransformerResponse{
					Output: types.SingularEventT{
						"event": "track" + strconv.Itoa(index+i),
					},
					Metadata: ptrans.Metadata{
						MessageID:       strconv.Itoa(index + i),
						SourceID:        "sourceID",
						DestinationID:   "destinationID",
						SourceType:      "sourceType",
						DestinationType: "destinationType",
					},
				}
			}),
		}
		wResponse := ptrans.Response{
			Events: lo.RepeatBy(50, func(index int) ptrans.TransformerResponse {
				return ptrans.TransformerResponse{
					Output: types.SingularEventT{
						"event": "track" + strconv.Itoa(index+i+1),
					},
					Metadata: ptrans.Metadata{
						MessageID:       strconv.Itoa(index + i + 1),
						SourceID:        "sourceID",
						DestinationID:   "destinationID",
						SourceType:      "sourceType",
						DestinationType: "destinationType",
					},
				}
			}),
		}

		trans.CompareAndLog(events, pResponse, wResponse, metadata, eventsByMessageID)
	}

	f, err := os.OpenFile(tmpFile.Name(), os.O_RDWR, 0o644)
	require.NoError(t, err)
	gzipReader, err := gzip.NewReader(f)
	require.NoError(t, err)
	data, err := io.ReadAll(gzipReader)
	require.NoError(t, err)
	require.NoError(t, gzipReader.Close())
	require.NoError(t, f.Close())

	differingEvents := strings.Split(strings.Trim(string(data), "\n"), "\n")
	require.Len(t, differingEvents, maxLoggedEvents)

	for i := 0; i < maxLoggedEvents; i++ {
		require.Contains(t, differingEvents[i], "track"+strconv.Itoa(i))
	}
	require.EqualValues(t, []float64{50, 50, 50, 50, 50, 50, 50, 50, 50, 50}, statsStore.Get("warehouse_dest_transform_mismatched_events", stats.Tags{}).Values())
}

func TestTransformer_GetColumns(t *testing.T) {
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
			c := config.New()
			c.Set("WH_MAX_COLUMNS_IN_EVENT", tc.maxColumns)

			trans := New(c, logger.NOP, stats.NOP)

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
