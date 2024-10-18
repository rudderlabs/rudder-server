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
