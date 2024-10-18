package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
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
		require.EqualValues(t, wResponse.Events[i], pResponse.Events[i])
	}
	for i := range pResponse.FailedEvents {
		require.EqualValues(t, wResponse.FailedEvents[i], pResponse.FailedEvents[i])
	}
}

func testEvents(t *testing.T, infos []eventsInfo, pTransformer, dTransformer ptrans.DestinationTransformer, expectedResponse ptrans.Response) {
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
	require.Equal(t, len(expectedResponse.Events), len(pResponse.Events))
	require.Equal(t, len(expectedResponse.Events), len(wResponse.Events))
	require.Equal(t, len(expectedResponse.FailedEvents), len(pResponse.FailedEvents))
	require.Equal(t, len(expectedResponse.FailedEvents), len(wResponse.FailedEvents))

	for i := range pResponse.Events {
		data := expectedResponse.Events[i].Output["data"]
		if data != nil && data.(map[string]any)["rudder_event"] != nil {
			require.JSONEq(t, expectedResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string), pResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))
			require.JSONEq(t, expectedResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string), wResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))
			require.JSONEq(t, wResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string), pResponse.Events[i].Output["data"].(map[string]any)["rudder_event"].(string))

			delete(pResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
			delete(wResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
			delete(expectedResponse.Events[i].Output["data"].(map[string]any), "rudder_event")
		}
	}
	for i := range pResponse.Events {
		require.EqualValues(t, expectedResponse.Events[i], pResponse.Events[i])
		require.EqualValues(t, expectedResponse.Events[i], wResponse.Events[i])
		require.EqualValues(t, wResponse.Events[i], pResponse.Events[i])
	}
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

			opts := lo.Map(tc.envOverride, func(item string, index int) transformertest.Option {
				return transformertest.WithEnv(item)
			})
			opts = append(opts, transformertest.WithRepository("rudderstack/develop-rudder-transformer"))
			transformerResource, err := transformertest.Setup(pool, t, opts...)
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

func FuzzTransformer(f *testing.F) {
	pool, err := dockertest.NewPool("")
	require.NoError(f, err)

	transformerResource, err := transformertest.Setup(pool, f)
	require.NoError(f, err)

	c := config.New()
	c.Set("DEST_TRANSFORM_URL", transformerResource.TransformerURL)
	c.Set("USER_TRANSFORM_URL", transformerResource.TransformerURL)

	types := []string{"track", "page", "identify", "group", "alias", "", "unknown"}

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
		p.Type = types[int(eventIndex)%len(types)]

		err = fz.GenerateStruct(&p)
		if err != nil {
			return
		}
		p.Type = types[int(eventIndex)%len(types)]

		eventPayload, err := json.Marshal(p)
		require.NoError(t, err)

		eventsInfos := []eventsInfo{
			{
				payload: eventPayload,
				metadata: ptrans.Metadata{
					EventType:       p.Type,
					DestinationType: "POSTGRES",
					ReceivedAt:      "2021-09-01T00:00:00.000Z",
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
