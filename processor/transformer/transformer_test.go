package transformer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/stretchr/testify/require"
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
