package transformer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type fakeTransformer struct {
	requests [][]transformer.TransformerEventT
}

func (t *fakeTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []transformer.TransformerEventT
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		panic(err)
	}

	t.requests = append(t.requests, reqBody)
	resps := make([]transformer.TransformerResponseT, len(reqBody))
	for i := range reqBody {
		statusCode := int(reqBody[i].Message["forceStatusCode"].(float64))
		delete(reqBody[i].Message, "forceStatusCode")
		reqBody[i].Message["echo-key-1"] = reqBody[i].Message["src-key-1"]

		resps[i] = transformer.TransformerResponseT{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: statusCode,
			Error:      "",
		}
		if statusCode >= 400 {
			resps[i].Error = "error"
		}
	}
	w.Header().Set("apiVersion", "2")
	if err := json.NewEncoder(w).Encode(resps); err != nil {
		panic(err)
	}
}

func Test_Transformer(t *testing.T) {
	config.Reset()
	logger.Reset()
	transformer.Init()

	ft := &fakeTransformer{}

	srv := httptest.NewServer(ft)
	defer srv.Close()

	tr := transformer.NewTransformer()
	tr.Client = srv.Client()

	tr.Setup()

	tc := []struct {
		batchSize   int
		eventsCount int
		failEvery   int
	}{
		{batchSize: 10, eventsCount: 100},
		{batchSize: 10, eventsCount: 9},
		{batchSize: 10, eventsCount: 91},
		{batchSize: 10, eventsCount: 99},
		{batchSize: 10, eventsCount: 1},
		{batchSize: 10, eventsCount: 80, failEvery: 4},
		{batchSize: 10, eventsCount: 80, failEvery: 1},
	}

	for _, tt := range tc {
		batchSize := tt.batchSize
		eventsCount := tt.eventsCount
		failEvery := tt.failEvery

		events := make([]transformer.TransformerEventT, eventsCount)
		expectedResponse := transformer.ResponseT{}

		for i := range events {
			msgID := fmt.Sprintf("messageID-%d", i)
			statusCode := 200

			if failEvery != 0 && i%failEvery == 0 {
				statusCode = 400
			}

			events[i] = transformer.TransformerEventT{
				Metadata: transformer.MetadataT{
					MessageID: msgID,
				},
				Message: map[string]interface{}{
					"src-key-1":       msgID,
					"forceStatusCode": statusCode,
				},
			}

			tresp := transformer.TransformerResponseT{
				Metadata: transformer.MetadataT{
					MessageID: msgID,
				},
				StatusCode: statusCode,
				Output: map[string]interface{}{
					"src-key-1":  msgID,
					"echo-key-1": msgID,
				},
			}

			if statusCode < 400 {
				expectedResponse.Events = append(expectedResponse.Events, tresp)
			} else {
				tresp.Error = "error"
				expectedResponse.FailedEvents = append(expectedResponse.FailedEvents, tresp)
			}

		}

		rsp := tr.Transform(context.TODO(), events, srv.URL, batchSize)
		require.Equal(t, expectedResponse, rsp)
	}
}

func TestTransformerEventsDeepCopy(t *testing.T) {
	te := transformer.TransformerEvents{
		{
			Message: types.SingularEventT{"foo": "bar"},
			Metadata: transformer.MetadataT{
				SourceID:            "SourceID",
				WorkspaceID:         "WorkspaceID",
				Namespace:           "Namespace",
				InstanceID:          "InstanceID",
				SourceType:          "SourceType",
				SourceCategory:      "SourceCategory",
				TrackingPlanId:      "TrackingPlanId",
				TrackingPlanVersion: 1,
				SourceTpConfig: map[string]map[string]interface{}{
					"sourceTpConfig": {
						"sourceTpConfig": "sourceTpConfig",
					},
				},
				MergedTpConfig: map[string]interface{}{
					"mergedTpConfig": "mergedTpConfig",
				},
				DestinationID:           "DestinationID",
				JobRunID:                "JobRunID",
				JobID:                   1,
				SourceBatchID:           "SourceBatchID",
				SourceJobID:             "SourceJobID",
				SourceJobRunID:          "SourceJobRunID",
				SourceTaskID:            "SourceTaskID",
				SourceTaskRunID:         "SourceTaskRunID",
				RecordID:                nil,
				DestinationType:         "DestinationType",
				MessageID:               "MessageID",
				OAuthAccessToken:        "OAuthAccessToken",
				MessageIDs:              nil,
				RudderID:                "RudderID",
				ReceivedAt:              "ReceivedAt",
				EventName:               "EventName",
				EventType:               "EventType",
				SourceDefinitionID:      "SourceDefinitionID",
				DestinationDefinitionID: "DestinationDefinitionID",
			},
			Destination: backendconfig.DestinationT{
				ID:   "ID",
				Name: "Name",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "ID",
					Name:        "Name",
					DisplayName: "DisplayName",
					Config: map[string]interface{}{
						"config": "config",
					},
					ResponseRules: map[string]interface{}{
						"responseRules": "responseRules",
					},
				},
				Config: map[string]interface{}{
					"config": "config",
				},
				Enabled:     true,
				WorkspaceID: "WorkspaceID",
				Transformations: []backendconfig.TransformationT{
					{
						VersionID: "VersionID",
						ID:        "ID",
						Config: map[string]interface{}{
							"config": "config",
						},
					},
				},
				IsProcessorEnabled: false,
				RevisionID:         "RevisionID",
			},
			Libraries: []backendconfig.LibraryT{
				{VersionID: "VersionID"},
			},
		},
		{
			Message: types.SingularEventT{"foo": "bar2"},
			Metadata: transformer.MetadataT{
				SourceID:            "SourceID2",
				WorkspaceID:         "WorkspaceID2",
				Namespace:           "Namespace2",
				InstanceID:          "InstanceID2",
				SourceType:          "SourceType2",
				SourceCategory:      "SourceCategory2",
				TrackingPlanId:      "TrackingPlanId2",
				TrackingPlanVersion: 2,
				SourceTpConfig: map[string]map[string]interface{}{
					"sourceTpConfig2": {
						"sourceTpConfig2": "sourceTpConfig2",
					},
				},
				MergedTpConfig: map[string]interface{}{
					"mergedTpConfig2": "mergedTpConfig2",
				},
				DestinationID:           "DestinationID2",
				JobRunID:                "JobRunID2",
				JobID:                   2,
				SourceBatchID:           "SourceBatchID2",
				SourceJobID:             "SourceJobID2",
				SourceJobRunID:          "SourceJobRunID2",
				SourceTaskID:            "SourceTaskID2",
				SourceTaskRunID:         "SourceTaskRunID2",
				RecordID:                nil,
				DestinationType:         "DestinationType2",
				MessageID:               "MessageID2",
				OAuthAccessToken:        "OAuthAccessToken2",
				MessageIDs:              nil,
				RudderID:                "RudderID2",
				ReceivedAt:              "ReceivedAt2",
				EventName:               "EventName2",
				EventType:               "EventType2",
				SourceDefinitionID:      "SourceDefinitionID2",
				DestinationDefinitionID: "DestinationDefinitionID2",
			},
			Destination: backendconfig.DestinationT{
				ID:   "ID2",
				Name: "Name2",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "ID2",
					Name:        "Name2",
					DisplayName: "DisplayName2",
					Config: map[string]interface{}{
						"config2": "config2",
					},
					ResponseRules: map[string]interface{}{
						"responseRules2": "responseRules2",
					},
				},
				Config: map[string]interface{}{
					"config2": "config2",
				},
				Enabled:     true,
				WorkspaceID: "WorkspaceID2",
				Transformations: []backendconfig.TransformationT{
					{
						VersionID: "VersionID2",
						ID:        "ID2",
						Config: map[string]interface{}{
							"config": "config2",
						},
					},
				},
				IsProcessorEnabled: true,
				RevisionID:         "RevisionID2",
			},
			Libraries: []backendconfig.LibraryT{
				{VersionID: "VersionID2"},
			},
		},
	}

	teCopy, err := te.Copy()
	require.NoError(t, err)

	teJSON, err := json.Marshal(te)
	require.NoError(t, err)

	teCopyJSON, err := json.Marshal(teCopy)
	require.NoError(t, err)

	require.JSONEq(t, string(teJSON), string(teCopyJSON))
}

func Test_EndlessLoopIf809(t *testing.T) {
	transformer.Init()

	ft := &endlessLoopTransformer{
		maxRetryCount: 3,
		t:             t,
	}

	srv := httptest.NewServer(ft)
	defer srv.Close()

	tr := transformer.NewTransformer()
	tr.Client = srv.Client()

	tr.Setup()

	msgID := "messageID-0"
	events := make([]transformer.TransformerEventT, 1)
	events[0] = transformer.TransformerEventT{
		Metadata: transformer.MetadataT{
			MessageID: msgID,
		},
		Message: map[string]interface{}{
			"src-key-1": msgID,
		},
	}

	rsp := tr.Transform(context.TODO(), events, srv.URL, 10)
	require.Equal(
		t,
		transformer.ResponseT{
			Events: []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MessageID: msgID,
					},
					StatusCode: 200,
					Output: map[string]interface{}{
						"src-key-1": msgID,
					},
				},
			},
		},
		rsp,
	)
	require.Equal(t, ft.retryCount, ft.maxRetryCount)
}

type endlessLoopTransformer struct {
	retryCount    int
	maxRetryCount int
	t             *testing.T
}

func (elt *endlessLoopTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []transformer.TransformerEventT
	require.NoError(elt.t, json.NewDecoder(r.Body).Decode(&reqBody))

	resps := make([]transformer.TransformerResponseT, len(reqBody))
	var statusCode int
	if elt.retryCount < elt.maxRetryCount {
		elt.retryCount++
		http.Error(w, response.MakeResponse("control plane not reachable"), 809)
		return
	} else {
		for i := range reqBody {
			statusCode = 200
			resps[i] = transformer.TransformerResponseT{
				Output:     reqBody[i].Message,
				Metadata:   reqBody[i].Metadata,
				StatusCode: statusCode,
				Error:      "",
			}
		}
	}
	w.Header().Set("apiVersion", "2")
	require.NoError(elt.t, json.NewEncoder(w).Encode(resps))
}
