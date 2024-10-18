package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type eventsInfo struct {
	payload     []byte
	metadata    ptrans.Metadata
	destination backendconfig.DestinationT
}

func testEvents(t *testing.T, infos []eventsInfo, pTransformer, dTransformer ptrans.DestinationTransformer, expectedResponse ptrans.Response) {
	t.Helper()

	events := prepareEvents(t, infos)

	ctx := context.Background()
	batchSize := 100

	pResponse := pTransformer.Transform(ctx, events, batchSize)
	wResponse := dTransformer.Transform(ctx, events, batchSize)

	validateResponseLengths(t, expectedResponse, pResponse, wResponse)
	validateEventData(t, expectedResponse, pResponse, wResponse)
	validateEventEquality(t, expectedResponse, pResponse, wResponse)
	validateFailedEventEquality(t, expectedResponse, pResponse, wResponse)
}

func prepareEvents(t *testing.T, infos []eventsInfo) []ptrans.TransformerEvent {
	var events []ptrans.TransformerEvent
	for _, info := range infos {
		var singularEvent types.SingularEventT
		err := json.Unmarshal(info.payload, &singularEvent)
		require.NoError(t, err)

		events = append(events, ptrans.TransformerEvent{
			Message:     singularEvent,
			Metadata:    info.metadata,
			Destination: info.destination,
		})
	}
	return events
}

func validateResponseLengths(t *testing.T, expectedResponse, pResponse, wResponse ptrans.Response) {
	require.Equal(t, len(expectedResponse.Events), len(pResponse.Events))
	require.Equal(t, len(expectedResponse.Events), len(wResponse.Events))
	require.Equal(t, len(expectedResponse.FailedEvents), len(pResponse.FailedEvents))
	require.Equal(t, len(expectedResponse.FailedEvents), len(wResponse.FailedEvents))
}

func validateEventData(t *testing.T, expectedResponse, pResponse, wResponse ptrans.Response) {
	for i := range pResponse.Events {
		data := expectedResponse.Events[i].Output["data"]
		if data != nil && data.(map[string]any)["rudder_event"] != nil {
			expectedRudderEvent := expectedResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string)
			require.JSONEq(t, expectedRudderEvent, pResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))
			require.JSONEq(t, expectedRudderEvent, wResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))
			require.JSONEq(t, wResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string), pResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))

			// Clean up rudder_event key after comparison
			delete(pResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
			delete(wResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
			delete(expectedResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
		}
	}
}

func validateEventEquality(t *testing.T, expectedResponse, pResponse, wResponse ptrans.Response) {
	for i := range pResponse.Events {
		require.EqualValues(t, expectedResponse.Events[i], pResponse.Events[i])
		require.EqualValues(t, expectedResponse.Events[i], wResponse.Events[i])
		require.EqualValues(t, wResponse.Events[i], pResponse.Events[i])
	}
}

func validateFailedEventEquality(t *testing.T, expectedResponse, pResponse, wResponse ptrans.Response) {
	for i := range pResponse.FailedEvents {
		require.EqualValues(t, expectedResponse.FailedEvents[i], pResponse.FailedEvents[i])
		require.EqualValues(t, expectedResponse.FailedEvents[i], wResponse.FailedEvents[i])
		require.EqualValues(t, wResponse.FailedEvents[i], pResponse.FailedEvents[i])
	}
}

func TestTransformer(t *testing.T) {
	t.Skip()

	testsCases := []struct {
		name             string
		configOverride   map[string]any
		envOverride      []string
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "unknown event (POSTGRES)",
			eventPayload: `{"type":"unknown"}`,
			metadata: ptrans.Metadata{
				EventType:       "unknown",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				MessageID:       "messageId",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      "Unknown event type: \"unknown\"",
						StatusCode: http.StatusBadRequest,
						Metadata: ptrans.Metadata{
							EventType:       "unknown",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
					},
				},
			},
		},
		{
			name: "track (POSTGRES) not populateSrcDestInfoInContext",
			configOverride: map[string]any{
				"Warehouse.populateSrcDestInfoInContext": false,
			},
			envOverride:  []string{"WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT=false"},
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata: ptrans.Metadata{
				EventType:       "track",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				MessageID:       "messageId",
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
								"anonymous_id":          "anonymousId",
								"channel":               "web",
								"context_ip":            "1.2.3.4",
								"context_passed_ip":     "1.2.3.4",
								"context_request_ip":    "5.6.7.8",
								"context_traits_email":  "rhedricks@example.com",
								"context_traits_logins": float64(2),
								"context_traits_name":   "Richard Hendricks",
								"event":                 "event",
								"event_text":            "event",
								"id":                    "messageId",
								"original_timestamp":    "2021-09-01T00:00:00.000Z",
								"received_at":           "2021-09-01T00:00:00.000Z",
								"sent_at":               "2021-09-01T00:00:00.000Z",
								"timestamp":             "2021-09-01T00:00:00.000Z",
								"user_id":               "userId",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"anonymous_id":          "string",
									"channel":               "string",
									"context_ip":            "string",
									"context_passed_ip":     "string",
									"context_request_ip":    "string",
									"context_traits_email":  "string",
									"context_traits_logins": "int",
									"context_traits_name":   "string",
									"event":                 "string",
									"event_text":            "string",
									"id":                    "string",
									"original_timestamp":    "datetime",
									"received_at":           "datetime",
									"sent_at":               "datetime",
									"timestamp":             "datetime",
									"user_id":               "string",
									"uuid_ts":               "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "tracks",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
							"data": map[string]any{
								"anonymous_id":          "anonymousId",
								"channel":               "web",
								"context_ip":            "1.2.3.4",
								"context_passed_ip":     "1.2.3.4",
								"context_request_ip":    "5.6.7.8",
								"context_traits_email":  "rhedricks@example.com",
								"context_traits_logins": float64(2),
								"context_traits_name":   "Richard Hendricks",
								"event":                 "event",
								"event_text":            "event",
								"id":                    "messageId",
								"original_timestamp":    "2021-09-01T00:00:00.000Z",
								"product_id":            "9578257311",
								"rating":                3.0,
								"received_at":           "2021-09-01T00:00:00.000Z",
								"review_body":           "OK for the price. It works but the material feels flimsy.",
								"review_id":             "86ac1cd43",
								"sent_at":               "2021-09-01T00:00:00.000Z",
								"timestamp":             "2021-09-01T00:00:00.000Z",
								"user_id":               "userId",
							},
							"metadata": map[string]any{
								"columns": map[string]any{
									"anonymous_id":          "string",
									"channel":               "string",
									"context_ip":            "string",
									"context_passed_ip":     "string",
									"context_request_ip":    "string",
									"context_traits_email":  "string",
									"context_traits_logins": "int",
									"context_traits_name":   "string",
									"event":                 "string",
									"event_text":            "string",
									"id":                    "string",
									"original_timestamp":    "datetime",
									"product_id":            "string",
									"rating":                "int",
									"received_at":           "datetime",
									"review_body":           "string",
									"review_id":             "string",
									"sent_at":               "datetime",
									"timestamp":             "datetime",
									"user_id":               "string",
									"uuid_ts":               "datetime",
								},
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "track (POSTGRES) too many columns",
			eventPayload: fmt.Sprintf(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","context":{%s},"ip":"1.2.3.4"}`, strings.Join(
				lo.RepeatBy(500, func(index int) string {
					return fmt.Sprintf(`"column_%d": "value_%d"`, index, index)
				}), ",",
			)),
			metadata: ptrans.Metadata{
				EventType:       "track",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				MessageID:       "messageId",
			},
			destination: backendconfig.DestinationT{
				Name:   "POSTGRES",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      "postgres transformer: Too many columns outputted from the event",
						StatusCode: http.StatusBadRequest,
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
					},
				},
			},
		},
		{
			name: "track (GCS_DATALAKE) too many columns",
			eventPayload: fmt.Sprintf(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, strings.Join(
				lo.RepeatBy(500, func(index int) string {
					return fmt.Sprintf(`"column_%d": "value_%d"`, index, index)
				}), ",",
			)),
			metadata: ptrans.Metadata{
				EventType:       "track",
				DestinationType: "GCS_DATALAKE",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				MessageID:       "messageId",
			},
			destination: backendconfig.DestinationT{
				Name:   "GCS_DATALAKE",
				Config: map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "GCS_DATALAKE",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": lo.Assign(
								map[string]any{
									"anonymous_id":             "anonymousId",
									"channel":                  "web",
									"context_destination_id":   "destinationID",
									"context_destination_type": "GCS_DATALAKE",
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
								lo.SliceToMap(
									lo.RepeatBy(500, func(index int) string {
										return strconv.Itoa(index)
									}), func(item string) (string, any) {
										return fmt.Sprintf(`context_column_%s`, item), fmt.Sprintf(`value_%s`, item)
									},
								),
							),
							"metadata": map[string]any{
								"columns": lo.Assign(
									map[string]any{
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
									lo.SliceToMap(
										lo.RepeatBy(500, func(index int) string {
											return strconv.Itoa(index)
										}), func(item string) (string, any) {
											return fmt.Sprintf(`context_column_%s`, item), "string"
										},
									),
								),
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "tracks",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "GCS_DATALAKE",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
							"data": lo.Assign(
								map[string]any{
									"anonymous_id":             "anonymousId",
									"channel":                  "web",
									"context_destination_id":   "destinationID",
									"context_destination_type": "GCS_DATALAKE",
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
								lo.SliceToMap(
									lo.RepeatBy(500, func(index int) string {
										return strconv.Itoa(index)
									}), func(item string) (string, any) {
										return fmt.Sprintf(`context_column_%s`, item), fmt.Sprintf(`value_%s`, item)
									},
								),
							),
							"metadata": map[string]any{
								"columns": lo.Assign(
									map[string]any{
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
									lo.SliceToMap(
										lo.RepeatBy(500, func(index int) string {
											return strconv.Itoa(index)
										}), func(item string) (string, any) {
											return fmt.Sprintf(`context_column_%s`, item), "string"
										},
									),
								),
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "GCS_DATALAKE",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "track (POSTGRES) with sources channel too many columns",
			eventPayload: fmt.Sprintf(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, strings.Join(
				lo.RepeatBy(500, func(index int) string {
					return fmt.Sprintf(`"column_%d": "value_%d"`, index, index)
				}), ",",
			)),
			metadata: ptrans.Metadata{
				EventType:       "track",
				DestinationType: "POSTGRES",
				ReceivedAt:      "2021-09-01T00:00:00.000Z",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				SourceType:      "sourceType",
				MessageID:       "messageId",
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
							"data": lo.Assign(
								map[string]any{
									"anonymous_id":             "anonymousId",
									"channel":                  "sources",
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
								lo.SliceToMap(
									lo.RepeatBy(500, func(index int) string {
										return strconv.Itoa(index)
									}), func(item string) (string, any) {
										return fmt.Sprintf(`context_column_%s`, item), fmt.Sprintf(`value_%s`, item)
									},
								),
							),
							"metadata": map[string]any{
								"columns": lo.Assign(
									map[string]any{
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
									lo.SliceToMap(
										lo.RepeatBy(500, func(index int) string {
											return strconv.Itoa(index)
										}), func(item string) (string, any) {
											return fmt.Sprintf(`context_column_%s`, item), "string"
										},
									),
								),
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "tracks",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
							"data": lo.Assign(
								map[string]any{
									"anonymous_id":             "anonymousId",
									"channel":                  "sources",
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
								lo.SliceToMap(
									lo.RepeatBy(500, func(index int) string {
										return strconv.Itoa(index)
									}), func(item string) (string, any) {
										return fmt.Sprintf(`context_column_%s`, item), fmt.Sprintf(`value_%s`, item)
									},
								),
							),
							"metadata": map[string]any{
								"columns": lo.Assign(
									map[string]any{
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
									lo.SliceToMap(
										lo.RepeatBy(500, func(index int) string {
											return strconv.Itoa(index)
										}), func(item string) (string, any) {
											return fmt.Sprintf(`context_column_%s`, item), "string"
										},
									),
								),
								"receivedAt": "2021-09-01T00:00:00.000Z",
								"table":      "event",
							},
							"userId": "",
						},
						Metadata: ptrans.Metadata{
							EventType:       "track",
							DestinationType: "POSTGRES",
							ReceivedAt:      "2021-09-01T00:00:00.000Z",
							SourceID:        "sourceID",
							DestinationID:   "destinationID",
							SourceType:      "sourceType",
							MessageID:       "messageId",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}

	for _, tc := range testsCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			//opts := lo.Map(tc.envOverride, func(item string, index int) transformertest.Option {
			//	return transformertest.WithEnv(item)
			//})
			//opts = append(opts, transformertest.WithRepository("rudderstack/develop-rudder-transformer"))
			transformerResource, err := transformertest.Setup(pool, t)
			require.NoError(t, err)

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

func cmpEvents(t *testing.T, infos []eventsInfo, pTransformer, dTransformer ptrans.DestinationTransformer) {
	t.Helper()

	var events []ptrans.TransformerEvent
	for _, info := range infos {
		var singularEvent types.SingularEventT
		err := json.Unmarshal(info.payload, &singularEvent)
		require.NoError(t, err)

		events = append(events, ptrans.TransformerEvent{
			Message:     singularEvent,
			Metadata:    info.metadata,
			Destination: info.destination,
		})
	}

	ctx := context.Background()
	batchSize := 100

	pResponse := pTransformer.Transform(ctx, events, batchSize)
	wResponse := dTransformer.Transform(ctx, events, batchSize)
	require.Equal(t, len(pResponse.Events), len(wResponse.Events))
	require.Equal(t, len(pResponse.FailedEvents), len(wResponse.FailedEvents))

	for i := range pResponse.Events {
		require.EqualValues(t, wResponse.Events[i], pResponse.Events[i], "js vs go")
	}
	for i := range pResponse.FailedEvents {
		//FIXME: skipping error codes for now:
		// go test -run=FuzzTransformerJSON/0420b68b7550b8b8
		wResponse.FailedEvents[i].Error = pResponse.FailedEvents[i].Error
		require.EqualValues(t, wResponse.FailedEvents[i], pResponse.FailedEvents[i], "js vs go")
	}
}

func FuzzTransformerJSON(f *testing.F) {
	pool, err := dockertest.NewPool("")
	require.NoError(f, err)

	transformerResource, err := transformertest.Setup(pool, f)
	require.NoError(f, err)

	c := config.New()
	c.Set("DEST_TRANSFORM_URL", transformerResource.TransformerURL)
	c.Set("USER_TRANSFORM_URL", transformerResource.TransformerURL)
	c.Set("Warehouse.populateSrcDestInfoInContext", true)

	f.Add(fmt.Sprintf(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`,
		strings.Join(
			lo.RepeatBy(500, func(index int) string {
				return fmt.Sprintf(`"column_%d": "value_%d"`, index, index)
			}), ",",
		)),
	)
	for _, eventType := range []string{"track", "page", "identify", "group", "alias", "", "unknown"} {
		f.Add(`{"type":"` + eventType + `","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}})`)
	}

	columnNames := []string{
		// SQL keywords and reserved words
		"select", "from", "where", "and", "or", "not", "insert", "update", "delete",
		"create", "alter", "drop", "table", "index", "view", "primary", "foreign",
		"key", "constraint", "default", "null", "unique", "check", "references",
		"join", "inner", "outer", "left", "right", "full", "on", "group", "by",
		"having", "order", "asc", "desc", "limit", "offset", "union", "all",
		"distinct", "as", "in", "between", "like", "is", "null", "true", "false",

		// Data types (which can vary by database system)
		"int", "integer", "bigint", "smallint", "tinyint", "decimal", "numeric",
		"float", "real", "double", "precision", "char", "varchar", "text", "date",
		"time", "timestamp", "datetime", "boolean", "blob", "clob", "binary",

		// Names starting with numbers or special characters
		"1column", "2_column", "@column", "#column", "$column",

		// Names with spaces or special characters
		"column name", "column-name", "column.name", "column@name", "column#name",
		"column$name", "column%name", "column&name", "column*name", "column+name",
		"column/name", "column\\name", "column'name", "column\"name", "column`name",

		// Names with non-ASCII characters
		"columnñame", "colûmnname", "columnнаме", "列名", "カラム名",

		// Very long names (may exceed maximum length in some databases)
		"this_is_a_very_long_column_name_that_exceeds_the_maximum_allowed_length_in_many_database_systems",

		// Names that could be confused with functions
		"count", "sum", "avg", "max", "min", "first", "last", "now", "current_timestamp",

		// Names with potential encoding issues
		"column\u0000name", "column\ufffdname",

		// Names that might conflict with ORM conventions
		"id", "_id", "created_at", "updated_at", "deleted_at",

		// Names that might conflict with common programming conventions
		"class", "interface", "enum", "struct", "function", "var", "let", "const",

		// Names with emoji or other Unicode symbols
		"column😀name", "column→name", "column★name",

		// Names with mathematical symbols
		"column+name", "column-name", "column*name", "column/name", "column^name",
		"column=name", "column<name", "column>name", "column≠name", "column≈name",

		// Names with comment-like syntax
		"column--name", "column/*name*/",

		// Names that might be interpreted as operators
		"column||name", "column&&name", "column!name", "column?name",

		// Names with control characters
		"column\tname", "column\nname", "column\rname",

		// Names that might conflict with schema notation
		"schema.column", "database.schema.column",

		// Names with brackets or parentheses
		"column(name)", "column[name]", "column{name}",

		// Names with quotes
		"'column'", "\"column\"", "`column`",

		// Names that might be interpreted as aliases
		"column as alias",

		// Names that might conflict with database-specific features
		"rowid", "oid", "xmin", "ctid", // These are specific to certain databases

		// Names that might conflict with common column naming conventions
		"fk_", "idx_", "pk_", "ck_", "uq_",

		// Names with invisible characters
		"column\u200bname", // Zero-width space
		"column\u00A0name", // Non-breaking space

		// Names with combining characters
		"columǹ", // 'n' with combining grave accent

		// Names with bi-directional text
		"column\u202Ename\u202C", // Using LTR and RTL markers

		// Names with unusual capitalization
		"COLUMN", "Column", "cOlUmN",

		// Names that are empty or only whitespace
		"", " ", "\t", "\n",

		// Names with currency symbols
		"column¢name", "column£name", "column€name", "column¥name",

		// Names with less common punctuation
		"column·name", "column…name", "column•name", "column‽name",

		// Names with fractions or other numeric forms
		"column½name", "column²name", "columnⅣname",
	}

	for _, columnName := range columnNames {
		f.Add(`{"type":"tracks","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","` + columnName + `":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}})`)
	}

	eventNames := []string{
		"omega",
		"omega v2 ",
		"9mega",
		"mega&",
		"ome$ga",
		"omega$",
		"ome_ ga",
		"9mega________-________90",
		"Cízǔ",
		"Rudderstack",
		"___",
		"group",
		"k3_namespace",
		"k3_namespace",
		"select",
		"drop",
		"create",
		"alter",
		"index",
		"table",
		"from",
		"where",
		"join",
		"union",
		"insert",
		"update",
		"delete",
		"truncate",
		"1invalid",
		"invalid-name",
		"invalid.name",
		"name with spaces",
		"name@with@special@chars",
		"verylongnamethatiswaytoolongforadatabasetablenameandexceedsthemaximumlengthallowed",
		"ñáme_wíth_áccents",
		"schema.tablename",
		"'quoted_name'",
		"name--with--comments",
		"name/*with*/comments",
		"name;with;semicolons",
		"name,with,commas",
		"name(with)parentheses",
		"name[with]brackets",
		"name{with}braces",
		"name+with+plus",
		"name=with=equals",
		"name<with>angle_brackets",
		"name|with|pipes",
		"name\\with\\backslashes",
		"name/with/slashes",
		"name\"with\"quotes",
		"name'with'single_quotes",
		"name`with`backticks",
		"name!with!exclamation",
		"name?with?question",
		"name#with#hash",
		"name%with%percent",
		"name^with^caret",
		"name~with~tilde",
		"primary",
		"foreign",
		"key",
		"constraint",
		"default",
		"null",
		"not null",
		"auto_increment",
		"identity",
		"unique",
		"check",
		"references",
		"on delete",
		"on update",
		"cascade",
		"restrict",
		"set null",
		"set default",
		"temporary",
		"temp",
		"view",
		"function",
		"procedure",
		"trigger",
		"序列化",     // Chinese for "serialization"
		"テーブル",    // Japanese for "table"
		"таблица", // Russian for "table"
		"0day",
		"_system",
		"__hidden__",
		"name:with:colons",
		"name★with★stars",
		"name→with→arrows",
		"name•with•bullets",
		"name‼with‼double_exclamation",
		"name⁉with⁉interrobang",
		"name‽with‽interrobang",
		"name⚠with⚠warning",
		"name☢with☢radiation",
		"name❗with❗exclamation",
		"name❓with❓question",
		"name⏎with⏎return",
		"name⌘with⌘command",
		"name⌥with⌥option",
		"name⇧with⇧shift",
		"name⌃with⌃control",
		"name⎋with⎋escape",
		"name␣with␣space",
		"name⍽with⍽space",
		"name¶with¶pilcrow",
		"name§with§section",
		"name‖with‖double_vertical_bar",
		"name¦with¦broken_bar",
		"name¬with¬negation",
		"name¤with¤currency",
		"name‰with‰permille",
		"name‱with‱permyriad",
		"name∞with∞infinity",
		"name≠with≠not_equal",
		"name≈with≈approximately_equal",
		"name≡with≡identical",
		"name√with√square_root",
		"name∛with∛cube_root",
		"name∜with∜fourth_root",
		"name∫with∫integral",
		"name∑with∑sum",
		"name∏with∏product",
		"name∀with∀for_all",
		"name∃with∃exists",
		"name∄with∄does_not_exist",
		"name∅with∅empty_set",
		"name∈with∈element_of",
		"name∉with∉not_element_of",
		"name∋with∋contains",
		"name∌with∌does_not_contain",
		"name∩with∩intersection",
		"name∪with∪union",
		"name⊂with⊂subset",
		"name⊃with⊃superset",
		"name⊄with⊄not_subset",
		"name⊅with⊅not_superset",
	}
	for _, eventName := range eventNames {
		f.Add(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"sources","event":"` + eventName + `","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
	}

	f.Fuzz(func(t *testing.T, payload string) {

		type payloadSchema struct {
			Type        string `json:"type"`
			EventName   string `json:"event"`
			MessageID   string `json:"messageId"`
			AnonymousID string `json:"anonymousId"`
			UserID      string `json:"userId"`
			SentAt      string `json:"sentAt"`
			Timestamp   string `json:"timestamp"`
			// This is set from rudderserver so it will always be a time.Time:
			ReceivedAt        time.Time         `json:"receivedAt"`
			OriginalTimestamp string            `json:"originalTimestamp"`
			Properties        map[string]string `json:"properties"`
			Context           map[string]string `json:"context"`
		}
		var p payloadSchema
		err := json.Unmarshal([]byte(payload), &p)
		if err != nil {
			return
		}

		// FIXME: This is a bug an incosistency in the JS transformer:
		if strings.ToLower(p.Type) != p.Type {
			return
		}
		// FIXME: This is a bug an incosistency in the JS transformer:
		if p.Type == "" {
			return
		}

		// // FIXME: This is a bug an incosistency in the JS transformer:
		// // - Error: (string) (len=22) "Unknown event type: \"\"",
		// // + Error: (string) (len=31) "Unknown event type: \"undefined\"",
		// //
		// // - Error: (string) (len=27) "Unknown event type: \"00000\"",
		// // + Error: (string) (len=31) "Unknown event type: \"undefined\"",
		// if p.Type == "" {
		// 	return
		// }

		eventsInfos := []eventsInfo{
			{
				payload: []byte(payload),
				metadata: ptrans.Metadata{
					EventType: p.Type,
					// todo test multiple destinations:
					DestinationType: "POSTGRES",
					ReceivedAt:      p.ReceivedAt.String(),
					SourceID:        "sourceID",
					DestinationID:   "destinationID",
					SourceType:      "sourceType",
					MessageID:       p.MessageID,
				},
				destination: backendconfig.DestinationT{
					Name:   "POSTGRES",
					Config: map[string]any{},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: "POSTGRES",
					},
				},
			},
		}
		destinationTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
		warehouseTransformer := New(c, logger.NOP, stats.NOP)

		cmpEvents(t, eventsInfos, destinationTransformer, warehouseTransformer)
	})
}

func FuzzTransformerFuzzHeaders(f *testing.F) {
	pool, err := dockertest.NewPool("")
	require.NoError(f, err)

	transformerResource, err := transformertest.Setup(pool, f)
	require.NoError(f, err)

	c := config.New()
	c.Set("DEST_TRANSFORM_URL", transformerResource.TransformerURL)
	c.Set("USER_TRANSFORM_URL", transformerResource.TransformerURL)
	c.Set("Warehouse.populateSrcDestInfoInContext", true)

	eventTypes := []string{"track", "page", "identify", "group", "alias", "", "unknown"}

	f.Fuzz(func(t *testing.T, data []byte, eventIndex uint8) {
		fz := fuzz.NewConsumer(data)

		type payload struct {
			Type              string            `json:"type"`
			EventName         string            `json:"event"`
			MessageID         string            `json:"messageId"`
			AnonymousID       string            `json:"anonymousId"`
			UserID            string            `json:"userId"`
			SentAt            string            `json:"sentAt"`
			Timestamp         string            `json:"timestamp"`
			ReceivedAt        string            `json:"receivedAt"`
			OriginalTimestamp string            `json:"originalTimestamp"`
			Properties        map[string]string `json:"properties"`
			Context           map[string]string `json:"context"`
		}
		var p payload
		p.Type = eventTypes[int(eventIndex)%len(eventTypes)]

		err = fz.GenerateStruct(&p)
		if err != nil {
			return
		}
		p.Type = eventTypes[int(eventIndex)%len(eventTypes)]

		eventPayload, err := json.Marshal(p)
		require.NoError(t, err)

		eventsInfos := []eventsInfo{
			{
				payload: eventPayload,
				metadata: ptrans.Metadata{
					EventType: p.Type,
					// todo test multiple destinations:
					DestinationType: "POSTGRES",
					ReceivedAt:      p.ReceivedAt,
					SourceID:        "sourceID",
					DestinationID:   "destinationID",
					SourceType:      "sourceType",
					MessageID:       p.MessageID,
				},
				destination: backendconfig.DestinationT{
					Name:   "POSTGRES",
					Config: map[string]any{},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: "POSTGRES",
					},
				},
			},
		}
		destinationTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
		warehouseTransformer := New(c, logger.NOP, stats.NOP)

		cmpEvents(t, eventsInfos, destinationTransformer, warehouseTransformer)
	})
}
