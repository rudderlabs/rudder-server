package testhelper

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type EventInfo struct {
	Payload     []byte
	Metadata    ptrans.Metadata
	Destination backendconfig.DestinationT
}

func ValidateEvents(t *testing.T, infos []EventInfo, pTransformer, dTransformer ptrans.DestinationTransformer, expectedResponse ptrans.Response) {
	t.Helper()

	events := prepareEvents(t, infos)

	ctx := context.Background()
	batchSize := 100

	pResponse := pTransformer.Transform(ctx, events, batchSize)
	wResponse := dTransformer.Transform(ctx, events, batchSize)

	validateResponseLengths(t, expectedResponse, pResponse, wResponse)
	validateRudderEventIfExists(t, expectedResponse, pResponse, wResponse)
	validateEventEquality(t, expectedResponse, pResponse, wResponse)
	validateFailedEventEquality(t, expectedResponse, pResponse, wResponse)
}

func prepareEvents(t *testing.T, infos []EventInfo) []ptrans.TransformerEvent {
	var events []ptrans.TransformerEvent
	for _, info := range infos {
		var singularEvent types.SingularEventT
		err := json.Unmarshal(info.Payload, &singularEvent)
		require.NoError(t, err)

		events = append(events, ptrans.TransformerEvent{
			Message:     singularEvent,
			Metadata:    info.Metadata,
			Destination: info.Destination,
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

func validateRudderEventIfExists(t *testing.T, expectedResponse, pResponse, wResponse ptrans.Response) {
	for i := range pResponse.Events {
		data, ok := expectedResponse.Events[i].Output["data"].(map[string]interface{})
		if !ok {
			continue // No data to validate
		}

		rudderEvent, ok := data["rudder_event"].(string)
		if !ok {
			continue // No rudder_event key, skip validation
		}

		pEventData, ok := pResponse.Events[i].Output["data"].(map[string]interface{})
		require.True(t, ok, "pResponse data must be a map")
		pRudderEvent, ok := pEventData["rudder_event"].(string)
		require.True(t, ok, "pResponse rudder_event must be a string")
		require.JSONEq(t, rudderEvent, pRudderEvent)

		wEventData, ok := wResponse.Events[i].Output["data"].(map[string]interface{})
		require.True(t, ok, "wResponse data must be a map")
		wRudderEvent, ok := wEventData["rudder_event"].(string)
		require.True(t, ok, "wResponse rudder_event must be a string")
		require.JSONEq(t, rudderEvent, wRudderEvent)

		require.JSONEq(t, pRudderEvent, wRudderEvent)

		delete(pEventData, "rudder_event")
		delete(wEventData, "rudder_event")
		delete(data, "rudder_event")
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
		require.NotEmpty(t, pResponse.FailedEvents[i].Error)
		require.NotEmpty(t, wResponse.FailedEvents[i].Error)
		require.NotEmpty(t, expectedResponse.FailedEvents[i].Error)

		require.NotZero(t, pResponse.FailedEvents[i].StatusCode)
		require.NotZero(t, wResponse.FailedEvents[i].StatusCode)
		require.NotZero(t, expectedResponse.FailedEvents[i].StatusCode)
	}
}
