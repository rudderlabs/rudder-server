package transformer

import (
	"bytes"
	"context"
	js "encoding/json"
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func BenchmarkTransform(b *testing.B) {
	initData()
	t := NewTransformer(config.New(), logger.NOP, stats.NOP)
	for i := 0; i < b.N; i++ {
		_ = t.Transform(context.Background(), data, 100)
	}
}

var data []TransformerEvent

func initData() {
	for i := 0; i < 1000; i++ {
		data = append(data, TransformerEvent{
			Message: map[string]interface{}{
				"key":            i,
				"some_other_key": "some_other_value-" + fmt.Sprint(2*i),
			},
			Metadata: Metadata{
				SourceID:      "source_id",
				WorkspaceID:   "workspace_id",
				InstanceID:    "instance_id",
				DestinationID: "destination_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "destination_id",
				Config: map[string]interface{}{
					"key": "value",
					"some_other_key": map[string]interface{}{
						"key2": "value2",
					},
				},
				Name:               "destination_name",
				Enabled:            true,
				IsProcessorEnabled: true,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "destination_definition_id",
					Name:        "destination_definition_name",
					DisplayName: "destination_definition_display_name",
					Config: map[string]interface{}{
						"key": "value",
						"some_other_key": map[string]interface{}{
							"key2": "value2",
						},
					},
					ResponseRules: map[string]interface{}{
						"key": "value",
					},
				},
			},
		})
	}
}

var responseData = []byte(
	`[
    {
        "metadata": {
            "destinationDefinitionId": "",
            "destinationId": "someDestID",
            "destinationName": "",
            "destinationType": "WEBHOOK",
            "eventName": "Demo Track",
            "eventType": "track",
            "instanceId": "",
            "jobId": 4,
            "mergedTpConfig": null,
            "messageId": "someMessageID",
            "messageIds": null,
            "namespace": "",
            "oauthAccessToken": "",
            "originalSourceId": "",
            "receivedAt": "2024-08-28T18:07:36.648+05:30",
            "recordId": null,
            "rudderId": "<<>>anon_id<<>>",
            "sourceCategory": "",
            "sourceDefinitionId": "someSourceDefID",
            "sourceId": "someSourceID",
            "sourceJobId": "",
            "sourceJobRunId": "",
            "sourceName": "jsdev",
            "sourceTaskRunId": "",
            "sourceTpConfig": null,
            "sourceType": "Javascript",
            "traceparent": "",
            "trackingPlanId": "",
            "trackingPlanVersion": 0,
            "transformationId": "",
            "transformationVersionId": "",
            "workspaceId": "someWorkspaceID"
        },
        "output": {
            "body": {
                "FORM": {},
                "JSON": {
                    "anonymousId": "anon_id",
                    "channel": "android-sdk",
                    "context": {
                        "app": {
                            "build": "1",
                            "name": "RudderAndroidClient",
                            "namespace": "com.rudderlabs.android.sdk",
                            "version": "1.0"
                        },
                        "device": {
                            "id": "49e4bdd1c280bc00",
                            "manufacturer": "Google",
                            "model": "Android SDK built for x86",
                            "name": "generic_x86"
                        },
                        "ip": "[::1]",
                        "library": {
                            "name": "com.rudderstack.android.sdk.core"
                        },
                        "locale": "en-US",
                        "network": {
                            "carrier": "Android"
                        },
                        "screen": {
                            "density": 420,
                            "height": 1794,
                            "width": 1080
                        },
                        "traits": {
                            "anonymousId": "49e4bdd1c280bc00"
                        },
                        "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
                    },
                    "event": "Demo Track",
                    "integrations": {
                        "All": true
                    },
                    "messageId": "someMessageID",
                    "originalTimestamp": "2019-08-12T05:08:30.909Z",
                    "properties": {
                        "category": "Demo Category",
                        "floatVal": 4.501,
                        "label": "Demo Label",
                        "testArray": [
                            {
                                "id": "elem1",
                                "value": "e1"
                            },
                            {
                                "id": "elem2",
                                "value": "e2"
                            }
                        ],
                        "testMap": {
                            "t1": "a",
                            "t2": 4
                        },
                        "value": 5
                    },
                    "receivedAt": "2024-08-28T18:07:36.648+05:30",
                    "request_ip": "[::1]",
                    "rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
                    "sentAt": "2019-08-12T05:08:30.909Z",
                    "timestamp": "2024-08-28T18:07:36.648+05:30",
                    "type": "track"
                },
                "JSON_ARRAY": {},
                "XML": {}
            },
            "endpoint": "http://httpstat.us/200",
            "files": {},
            "headers": {
                "content-type": "application/json"
            },
            "method": "POST",
            "params": {},
            "type": "REST",
            "userId": "anon_id",
            "version": "1"
        },
        "statusCode": 200
    },
    {
        "metadata": {
            "destinationDefinitionId": "",
            "destinationId": "someDestID",
            "destinationName": "",
            "destinationType": "WEBHOOK",
            "eventName": "Demo Track",
            "eventType": "track",
            "instanceId": "",
            "jobId": 4,
            "mergedTpConfig": null,
            "messageId": "someMessageID",
            "messageIds": null,
            "namespace": "",
            "oauthAccessToken": "",
            "originalSourceId": "",
            "receivedAt": "2024-08-28T18:07:36.648+05:30",
            "recordId": null,
            "rudderId": "<<>>anon_id<<>>",
            "sourceCategory": "",
            "sourceDefinitionId": "someSourceDefID",
            "sourceId": "someSourceID",
            "sourceJobId": "",
            "sourceJobRunId": "",
            "sourceName": "jsdev",
            "sourceTaskRunId": "",
            "sourceTpConfig": null,
            "sourceType": "Javascript",
            "traceparent": "",
            "trackingPlanId": "",
            "trackingPlanVersion": 0,
            "transformationId": "",
            "transformationVersionId": "",
            "workspaceId": "someWorkspaceID"
        },
        "output": {
            "body": {
                "FORM": {},
                "JSON": {
                    "anonymousId": "anon_id",
                    "channel": "android-sdk",
                    "context": {
                        "app": {
                            "build": "1",
                            "name": "RudderAndroidClient",
                            "namespace": "com.rudderlabs.android.sdk",
                            "version": "1.0"
                        },
                        "device": {
                            "id": "49e4bdd1c280bc00",
                            "manufacturer": "Google",
                            "model": "Android SDK built for x86",
                            "name": "generic_x86"
                        },
                        "ip": "[::1]",
                        "library": {
                            "name": "com.rudderstack.android.sdk.core"
                        },
                        "locale": "en-US",
                        "network": {
                            "carrier": "Android"
                        },
                        "screen": {
                            "density": 420,
                            "height": 1794,
                            "width": 1080
                        },
                        "traits": {
                            "anonymousId": "49e4bdd1c280bc00"
                        },
                        "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
                    },
                    "event": "Demo Track",
                    "integrations": {
                        "All": true
                    },
                    "messageId": "someMessageID",
                    "originalTimestamp": "2019-08-12T05:08:30.909Z",
                    "properties": {
                        "category": "Demo Category",
                        "floatVal": 4.501,
                        "label": "Demo Label",
                        "testArray": [
                            {
                                "id": "elem1",
                                "value": "e1"
                            },
                            {
                                "id": "elem2",
                                "value": "e2"
                            }
                        ],
                        "testMap": {
                            "t1": "a",
                            "t2": 4
                        },
                        "value": 5
                    },
                    "receivedAt": "2024-08-28T18:07:36.648+05:30",
                    "request_ip": "[::1]",
                    "rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
                    "sentAt": "2019-08-12T05:08:30.909Z",
                    "timestamp": "2024-08-28T18:07:36.648+05:30",
                    "type": "track"
                },
                "JSON_ARRAY": {},
                "XML": {}
            },
            "endpoint": "http://httpstat.us/200",
            "files": {},
            "headers": {
                "content-type": "application/json"
            },
            "method": "POST",
            "params": {},
            "type": "REST",
            "userId": "anon_id",
            "version": "1"
        },
        "statusCode": 200
    },
    {
        "metadata": {
            "destinationDefinitionId": "",
            "destinationId": "someDestID",
            "destinationName": "",
            "destinationType": "WEBHOOK",
            "eventName": "Demo Track",
            "eventType": "track",
            "instanceId": "",
            "jobId": 4,
            "mergedTpConfig": null,
            "messageId": "someMessageID",
            "messageIds": null,
            "namespace": "",
            "oauthAccessToken": "",
            "originalSourceId": "",
            "receivedAt": "2024-08-28T18:07:36.648+05:30",
            "recordId": null,
            "rudderId": "<<>>anon_id<<>>",
            "sourceCategory": "",
            "sourceDefinitionId": "someSourceDefID",
            "sourceId": "someSourceID",
            "sourceJobId": "",
            "sourceJobRunId": "",
            "sourceName": "jsdev",
            "sourceTaskRunId": "",
            "sourceTpConfig": null,
            "sourceType": "Javascript",
            "traceparent": "",
            "trackingPlanId": "",
            "trackingPlanVersion": 0,
            "transformationId": "",
            "transformationVersionId": "",
            "workspaceId": "someWorkspaceID"
        },
        "output": {
            "body": {
                "FORM": {},
                "JSON": {
                    "anonymousId": "anon_id",
                    "channel": "android-sdk",
                    "context": {
                        "app": {
                            "build": "1",
                            "name": "RudderAndroidClient",
                            "namespace": "com.rudderlabs.android.sdk",
                            "version": "1.0"
                        },
                        "device": {
                            "id": "49e4bdd1c280bc00",
                            "manufacturer": "Google",
                            "model": "Android SDK built for x86",
                            "name": "generic_x86"
                        },
                        "ip": "[::1]",
                        "library": {
                            "name": "com.rudderstack.android.sdk.core"
                        },
                        "locale": "en-US",
                        "network": {
                            "carrier": "Android"
                        },
                        "screen": {
                            "density": 420,
                            "height": 1794,
                            "width": 1080
                        },
                        "traits": {
                            "anonymousId": "49e4bdd1c280bc00"
                        },
                        "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
                    },
                    "event": "Demo Track",
                    "integrations": {
                        "All": true
                    },
                    "messageId": "someMessageID",
                    "originalTimestamp": "2019-08-12T05:08:30.909Z",
                    "properties": {
                        "category": "Demo Category",
                        "floatVal": 4.501,
                        "label": "Demo Label",
                        "testArray": [
                            {
                                "id": "elem1",
                                "value": "e1"
                            },
                            {
                                "id": "elem2",
                                "value": "e2"
                            }
                        ],
                        "testMap": {
                            "t1": "a",
                            "t2": 4
                        },
                        "value": 5
                    },
                    "receivedAt": "2024-08-28T18:07:36.648+05:30",
                    "request_ip": "[::1]",
                    "rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
                    "sentAt": "2019-08-12T05:08:30.909Z",
                    "timestamp": "2024-08-28T18:07:36.648+05:30",
                    "type": "track"
                },
                "JSON_ARRAY": {},
                "XML": {}
            },
            "endpoint": "http://httpstat.us/200",
            "files": {},
            "headers": {
                "content-type": "application/json"
            },
            "method": "POST",
            "params": {},
            "type": "REST",
            "userId": "anon_id",
            "version": "1"
        },
        "statusCode": 200
    },
    {
        "metadata": {
            "destinationDefinitionId": "",
            "destinationId": "someDestID",
            "destinationName": "",
            "destinationType": "WEBHOOK",
            "eventName": "Demo Track",
            "eventType": "track",
            "instanceId": "",
            "jobId": 4,
            "mergedTpConfig": null,
            "messageId": "someMessageID",
            "messageIds": null,
            "namespace": "",
            "oauthAccessToken": "",
            "originalSourceId": "",
            "receivedAt": "2024-08-28T18:07:36.648+05:30",
            "recordId": null,
            "rudderId": "<<>>anon_id<<>>",
            "sourceCategory": "",
            "sourceDefinitionId": "someSourceDefID",
            "sourceId": "someSourceID",
            "sourceJobId": "",
            "sourceJobRunId": "",
            "sourceName": "jsdev",
            "sourceTaskRunId": "",
            "sourceTpConfig": null,
            "sourceType": "Javascript",
            "traceparent": "",
            "trackingPlanId": "",
            "trackingPlanVersion": 0,
            "transformationId": "",
            "transformationVersionId": "",
            "workspaceId": "someWorkspaceID"
        },
        "output": {
            "body": {
                "FORM": {},
                "JSON": {
                    "anonymousId": "anon_id",
                    "channel": "android-sdk",
                    "context": {
                        "app": {
                            "build": "1",
                            "name": "RudderAndroidClient",
                            "namespace": "com.rudderlabs.android.sdk",
                            "version": "1.0"
                        },
                        "device": {
                            "id": "49e4bdd1c280bc00",
                            "manufacturer": "Google",
                            "model": "Android SDK built for x86",
                            "name": "generic_x86"
                        },
                        "ip": "[::1]",
                        "library": {
                            "name": "com.rudderstack.android.sdk.core"
                        },
                        "locale": "en-US",
                        "network": {
                            "carrier": "Android"
                        },
                        "screen": {
                            "density": 420,
                            "height": 1794,
                            "width": 1080
                        },
                        "traits": {
                            "anonymousId": "49e4bdd1c280bc00"
                        },
                        "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
                    },
                    "event": "Demo Track",
                    "integrations": {
                        "All": true
                    },
                    "messageId": "someMessageID",
                    "originalTimestamp": "2019-08-12T05:08:30.909Z",
                    "properties": {
                        "category": "Demo Category",
                        "floatVal": 4.501,
                        "label": "Demo Label",
                        "testArray": [
                            {
                                "id": "elem1",
                                "value": "e1"
                            },
                            {
                                "id": "elem2",
                                "value": "e2"
                            }
                        ],
                        "testMap": {
                            "t1": "a",
                            "t2": 4
                        },
                        "value": 5
                    },
                    "receivedAt": "2024-08-28T18:07:36.648+05:30",
                    "request_ip": "[::1]",
                    "rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
                    "sentAt": "2019-08-12T05:08:30.909Z",
                    "timestamp": "2024-08-28T18:07:36.648+05:30",
                    "type": "track"
                },
                "JSON_ARRAY": {},
                "XML": {}
            },
            "endpoint": "http://httpstat.us/200",
            "files": {},
            "headers": {
                "content-type": "application/json"
            },
            "method": "POST",
            "params": {},
            "type": "REST",
            "userId": "anon_id",
            "version": "1"
        },
        "statusCode": 200
    }

]`,
)

/*
goos: darwin
goarch: arm64
pkg: github.com/rudderlabs/rudder-server/processor/transformer
=== RUN   BenchmarkUnmarshalTransformerResponse
BenchmarkUnmarshalTransformerResponse
BenchmarkUnmarshalTransformerResponse-10           18355             64911 ns/op           54091 B/op       1253 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/processor/transformer       2.293s
*/
func BenchmarkUnmarshalTransformerResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var response []TransformerResponse
		_ = json.Unmarshal(responseData, &response)
	}
}

type TransformerResponse2 struct {
	// Not marking this Singular Event, since this not a RudderEvent
	Output           js.RawMessage     `json:"output"`
	Metadata         Metadata          `json:"metadata"`
	StatusCode       int               `json:"statusCode"`
	Error            string            `json:"error"`
	ValidationErrors []ValidationError `json:"validationErrors"`
}

/*
goos: darwin
goarch: arm64
pkg: github.com/rudderlabs/rudder-server/processor/transformer
=== RUN   BenchmarkUnmarshalTransformerResponse2
BenchmarkUnmarshalTransformerResponse2
BenchmarkUnmarshalTransformerResponse2-10          39862             29601 ns/op           23613 B/op        464 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/processor/transformer       2.089s
*/
func BenchmarkUnmarshalTransformerResponse2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var response []TransformerResponse2
		_ = json.Unmarshal(responseData, &response)
	}
}

/*
goos: darwin
goarch: arm64
pkg: github.com/rudderlabs/rudder-server/processor/transformer
BenchmarkFastestUnmarshal-10    	   19616	     60979 ns/op	   51469 B/op	    1113 allocs/op
PASS
ok  	github.com/rudderlabs/rudder-server/processor/transformer	2.262s
*/
func BenchmarkFastestUnmarshal(b *testing.B) {
	jsonFastest := jsoniter.ConfigFastest
	for i := 0; i < b.N; i++ {
		var response []TransformerResponse
		iter := jsonFastest.BorrowIterator(responseData)
		iter.ReadVal(&response)
		if iter.Error != nil {
			b.Fatal(iter.Error)
		}
		jsonFastest.ReturnIterator(iter)
	}
}

/*
goos: darwin
goarch: arm64
pkg: github.com/rudderlabs/rudder-server/processor/transformer
=== RUN   BenchmarkFastestUnmarshalTransformerResponse2
BenchmarkFastestUnmarshalTransformerResponse2
BenchmarkFastestUnmarshalTransformerResponse2-10           43570             27036 ns/op           21501 B/op        324 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/processor/transformer       1.960s
*/
func BenchmarkFastestUnmarshalTransformerResponse2(b *testing.B) {
	jsonFastest := jsoniter.ConfigFastest
	for i := 0; i < b.N; i++ {
		var response []TransformerResponse2
		iter := jsonFastest.BorrowIterator(responseData)
		iter.ReadVal(&response)
		if iter.Error != nil {
			b.Fatal(iter.Error)
		}
		jsonFastest.ReturnIterator(iter)
	}
}

func createNewResponse(b testing.TB, responseData, itemSep, fieldSep []byte) []byte {
	b.Log(string(itemSep))
	b.Log(string(fieldSep))
	var response []TransformerResponse
	_ = json.Unmarshal(responseData, &response)
	res := make([]byte, 0, len(responseData)*2)
	for i, r := range response {
		output, _ := json.Marshal(r.Output)
		res = append(res, output...)
		res = append(res, fieldSep...)
		metadata, _ := json.Marshal(r.Metadata)
		res = append(res, metadata...)
		res = append(res, fieldSep...)
		res = append(res, []byte(fmt.Sprint(r.StatusCode))...)
		if i != len(response)-1 {
			res = append(res, itemSep...)
		}
	}
	return res
}

type TResponse struct {
	Output     []byte
	Metadata   []byte
	StatusCode string
}

func parseNewResponse(data, itemSep, fieldSep []byte) []TResponse {
	var response []TResponse
	items := bytes.Split(data, itemSep)
	for _, item := range items {
		fields := bytes.Split(item, fieldSep)
		response = append(response, TResponse{
			Output:     fields[0],
			Metadata:   fields[1],
			StatusCode: string(fields[2]),
		})
	}
	return response
}

func TestParseNewResponse(t *testing.T) {
	itemSep := []byte(nuid.Next())
	fieldSep := []byte(nuid.Next())
	responseBody := createNewResponse(t, responseData, itemSep, fieldSep)
	res := parseNewResponse(responseBody, itemSep, fieldSep)
	if len(res) != 4 {
		t.Errorf("expected 4 responses, got %d", len(res))
	}
}

/*
=== RUN   BenchmarkParseNewResponse/parseNewResponse
BenchmarkParseNewResponse/parseNewResponse
BenchmarkParseNewResponse/parseNewResponse-10             233391              4570 ns/op             876 B/op         12 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/processor/transformer       1.569s
*/
func BenchmarkParseNewResponse(b *testing.B) {
	// bytePool := &sync.Pool{
	// 	New: func() interface{} {
	// 		return make([]byte, 0, 4096)
	// 	},
	// }
	itemSep := []byte(nuid.Next())
	fieldSep := []byte(nuid.Next())
	responseBody := createNewResponse(b, responseData, itemSep, fieldSep)
	b.ResetTimer()
	b.Run("parseNewResponse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = parseNewResponse(responseBody, itemSep, fieldSep)
		}
	})
}
