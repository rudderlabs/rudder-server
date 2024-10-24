package processor

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/utils/types"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func Benchmark_makeCommonMetadataFromSingularEvent(b *testing.B) {
	proc := &Handle{}
	for i := 0; i < b.N; i++ {
		_ = proc.makeCommonMetadataFromSingularEvent(
			dummySingularEvent, dummyBatchEvent.UserID, dummyBatchEvent.JobID, time.Now(), &backendconfig.SourceT{
				WorkspaceID: "test",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name:     "test_def",
					Category: "eventStream",
					ID:       "testDefId",
				},
			},
			types.EventParams{
				SourceTaskRunId: "source_task_run_id",
				SourceJobRunId:  "source_job_run_id",
				SourceId:        "source_id",
			})
	}
}

var dummySingularEvent = map[string]interface{}{
	"type":      "track",
	"channel":   "android-srk",
	"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
	"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
	"properties": map[string]interface{}{
		"lbael":    "",
		"value":    float64(1),
		"testMap":  nil,
		"category": "",
		"floatVal": float64(4.51),
	},
	"originalTimestamp": "2019-03-10T10:10:10.10Z",
	"event":             "Demo Track",
	"sentAt":            "2019-03-10T10:10:10.10Z",
	"context": map[string]interface{}{
		"app":        nil,
		"device":     map[string]interface{}{},
		"locale":     "",
		"screen":     map[string]interface{}{},
		"traits":     map[string]interface{}{},
		"library":    map[string]interface{}{},
		"network":    map[string]interface{}{},
		"user_agent": "",
	},
	"anonymousId": "anon_id",
	"integrations": map[string]interface{}{
		"All": true,
	},
}

var dummyBatchEvent = jobsdb.JobT{
	UUID:          uuid.New(),
	JobID:         1,
	UserID:        "anon_id",
	CreatedAt:     time.Now(),
	ExpireAt:      time.Now(),
	CustomVal:     "GW",
	EventCount:    1,
	EventPayload:  payload,
	LastJobStatus: jobsdb.JobStatusT{},
	Parameters:    gwParameters,
	WorkspaceId:   "test",
}

var gwParameters = []byte(`{"batch_id": 1, "source_id": "1rNMpysD4lTuzglyfmPzsmihAbK", "source_job_run_id": ""}`)

var payload = []byte(
	`{
		"batch": [
		  {
			"type": "track",
			"event": "Demo Track",
			"sentAt": "2019-08-12T05:08:30.909Z",
			"channel": "android-sdk",
			"context": {
			  "app": {
				"name": "RudderAndroidClient",
				"build": "1",
				"version": "1.0",
				"namespace": "com.rudderlabs.android.sdk"
			  },
			  "device": {
				"id": "49e4bdd1c280bc00",
				"name": "generic_x86",
				"model": "Android SDK built for x86",
				"manufacturer": "Google"
			  },
			  "locale": "en-US",
			  "screen": {
				"width": 1080,
				"height": 1794,
				"density": 420
			  },
			  "traits": {
				"anonymousId": "49e4bdd1c280bc00"
			  },
			  "library": {
				"name": "com.rudderstack.android.sdk.core"
			  },
			  "network": {
				"carrier": "Android"
			  },
			  "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
			},
			"rudderId": "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
			"messageId": "dc269a2f-fb5a-41ea-ad00-7afc28a02cbc",
			"properties": {
			  "label": "Demo Label",
			  "value": 5,
			  "testMap": {
				"t1": "a",
				"t2": 4
			  },
			  "category": "Demo Category",
			  "floatVal": 4.501,
			  "testArray": [
				{
				  "id": "elem1",
				  "value": "e1"
				},
				{
				  "id": "elem2",
				  "value": "e2"
				}
			  ]
			},
			"anonymousId": "anon_id",
			"integrations": {
			  "All": true
			},
			"originalTimestamp": "2019-08-12T05:08:30.909Z"
		  }
		],
		"writeKey": "1rNMpxFxVdoaAdItcXTbVVWdonD",
		"requestIP": "127.0.0.1",
		"receivedAt": "2022-04-08T21:31:11.254+05:30"
	  }`)
