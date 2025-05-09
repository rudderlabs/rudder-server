package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse"
	"github.com/rudderlabs/rudder-server/processor/types"
)

type EventContext struct {
	Payload     []byte
	Metadata    types.Metadata
	Destination backendconfig.DestinationT
}

func ValidateEvents(t *testing.T, eventContexts []EventContext, pTransformer *destination_transformer.Client, dTransformer *warehouse.Transformer, expectedResponse types.Response) {
	t.Helper()

	events := prepareEvents(t, eventContexts)

	ctx := context.Background()

	legacyResponse := pTransformer.Transform(ctx, events)
	embeddedResponse := dTransformer.Transform(ctx, events)

	validateResponseLengths(t, expectedResponse, legacyResponse, embeddedResponse)
	validateRudderEventIfExists(t, expectedResponse, legacyResponse, embeddedResponse)
	validateEventEquality(t, expectedResponse, legacyResponse, embeddedResponse)
	validateFailedEventEquality(t, legacyResponse, embeddedResponse)
}

func prepareEvents(t *testing.T, eventContexts []EventContext) []types.TransformerEvent {
	t.Helper()

	events := make([]types.TransformerEvent, 0, len(eventContexts))
	for _, eventContext := range eventContexts {
		var singularEvent types.SingularEventT
		err := jsonrs.Unmarshal(eventContext.Payload, &singularEvent)
		require.NoError(t, err)

		events = append(events, types.TransformerEvent{
			Message:     singularEvent,
			Metadata:    eventContext.Metadata,
			Destination: eventContext.Destination,
		})
	}
	return events
}

func validateResponseLengths(t *testing.T, expectedResponse, legacyResponse, embeddedResponse types.Response) {
	t.Helper()

	require.Equal(t, len(expectedResponse.Events), len(legacyResponse.Events))
	require.Equal(t, len(expectedResponse.Events), len(embeddedResponse.Events))
	require.Equal(t, len(expectedResponse.FailedEvents), len(legacyResponse.FailedEvents))
	require.Equal(t, len(expectedResponse.FailedEvents), len(embeddedResponse.FailedEvents))
}

func validateRudderEventIfExists(t *testing.T, expectedResponse, legacyResponse, embeddedResponse types.Response) {
	t.Helper()

	for i := range legacyResponse.Events {
		data, ok := expectedResponse.Events[i].Output["data"].(map[string]interface{})
		if !ok {
			continue // No data to validate
		}

		rudderEvent, ok := data["rudder_event"].(string)
		if !ok {
			continue // No rudder_event key, skip validation
		}

		pEventData, ok := legacyResponse.Events[i].Output["data"].(map[string]interface{})
		require.True(t, ok, "legacyResponse data must be a map")
		pRudderEvent, ok := pEventData["rudder_event"].(string)
		require.True(t, ok, "legacyResponse rudder_event must be a string")
		require.JSONEq(t, rudderEvent, pRudderEvent)

		wEventData, ok := embeddedResponse.Events[i].Output["data"].(map[string]interface{})
		require.True(t, ok, "embeddedResponse data must be a map")
		wRudderEvent, ok := wEventData["rudder_event"].(string)
		require.True(t, ok, "embeddedResponse rudder_event must be a string")
		require.JSONEq(t, rudderEvent, wRudderEvent)

		require.JSONEq(t, pRudderEvent, wRudderEvent)

		delete(pEventData, "rudder_event")
		delete(wEventData, "rudder_event")
		delete(data, "rudder_event")
	}
}

func validateEventEquality(t *testing.T, expectedResponse, legacyResponse, embeddedResponse types.Response) {
	t.Helper()

	for i := range legacyResponse.Events {
		require.EqualValues(t, expectedResponse.Events[i], legacyResponse.Events[i])
		require.EqualValues(t, expectedResponse.Events[i], embeddedResponse.Events[i])
	}
}

func validateFailedEventEquality(t *testing.T, legacyResponse, embeddedResponse types.Response) {
	t.Helper()

	for i := range legacyResponse.FailedEvents {
		require.NotEmpty(t, legacyResponse.FailedEvents[i].Error)
		require.NotEmpty(t, embeddedResponse.FailedEvents[i].Error)

		require.NotZero(t, legacyResponse.FailedEvents[i].StatusCode)
		require.NotZero(t, embeddedResponse.FailedEvents[i].StatusCode)
	}
}
