package testhelper

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func ValidateExpectedEvents(t testing.TB, expectedResponse, embeddedResponse, legacyResponse types.Response) {
	t.Helper()
	expectedResponse, embeddedResponse, legacyResponse = deepCopyResponse(t, expectedResponse), deepCopyResponse(t, embeddedResponse), deepCopyResponse(t, legacyResponse)
	checkForMarshalledFieldsAndRemove(t, expectedResponse.Events, embeddedResponse.Events, legacyResponse.Events, []string{"data.rudder_event"}...)
	cmpEvents(t, expectedResponse.Events, embeddedResponse.Events)
	cmpFailedEvents(t, expectedResponse.FailedEvents, embeddedResponse.FailedEvents)
	cmpEvents(t, expectedResponse.Events, legacyResponse.Events)
	cmpFailedEvents(t, expectedResponse.FailedEvents, legacyResponse.FailedEvents)
}

func ValidateEvents(t *testing.T, embeddedResponse, legacyResponse types.Response) {
	t.Helper()
	embeddedResponse, legacyResponse = deepCopyResponse(t, embeddedResponse), deepCopyResponse(t, legacyResponse)
	cmpEvents(t, embeddedResponse.Events, legacyResponse.Events)
	cmpFailedEvents(t, embeddedResponse.FailedEvents, legacyResponse.FailedEvents)
}

func cmpEvents(t testing.TB, expected, actual []types.TransformerResponse) {
	t.Helper()
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.EqualValues(t, expected[i].Output, actual[i].Output)
		require.EqualValues(t, expected[i].Metadata, actual[i].Metadata)
		require.EqualValues(t, expected[i].StatusCode, actual[i].StatusCode)
		require.EqualValues(t, expected[i].Error, actual[i].Error)
		require.EqualValues(t, expected[i].ValidationErrors, actual[i].ValidationErrors)
	}
}

func checkForMarshalledFieldsAndRemove(t testing.TB, expected, embedded, legacy []types.TransformerResponse, fields ...string) {
	t.Helper()

	for i := range expected {
		for _, field := range fields {
			keys := strings.Split(field, ".")
			fieldMap := misc.MapLookup(expected[i].Output, keys...)
			if fieldMap == nil {
				continue
			}
			embeddedMap := misc.MapLookup(embedded[i].Output, keys...)
			require.NotNil(t, embeddedMap)
			require.JSONEq(t, fieldMap.(string), embeddedMap.(string))
			legacyMap := misc.MapLookup(legacy[i].Output, keys...)
			require.NotNil(t, legacyMap)
			require.JSONEq(t, fieldMap.(string), legacyMap.(string))
		}
	}
	for i := range embedded {
		for _, field := range fields {
			lastKey := field[strings.LastIndex(field, ".")+1:]
			removeLastKey := field[:strings.LastIndex(field, ".")]
			keys := strings.Split(removeLastKey, ".")
			fieldMap := misc.MapLookup(expected[i].Output, keys...)
			if fieldMap == nil {
				continue
			}
			embeddedMap := misc.MapLookup(embedded[i].Output, keys...)
			require.NotNil(t, embeddedMap)
			legacyMap := misc.MapLookup(legacy[i].Output, keys...)
			require.NotNil(t, legacyMap)
			delete(fieldMap.(map[string]any), lastKey)
			delete(embeddedMap.(map[string]any), lastKey)
			delete(legacyMap.(map[string]any), lastKey)
		}
	}
}

func cmpFailedEvents(t testing.TB, expected, actual []types.TransformerResponse) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.EqualValues(t, expected[i].Output, actual[i].Output)
		require.EqualValues(t, expected[i].Metadata, actual[i].Metadata)
		require.EqualValues(t, expected[i].StatusCode, actual[i].StatusCode)
		require.EqualValues(t, expected[i].ValidationErrors, actual[i].ValidationErrors)

		require.NotNil(t, expected[i].Error)
		require.NotNil(t, actual[i].Error)
	}
}

func deepCopyResponse(t testing.TB, res types.Response) types.Response {
	t.Helper()

	resBytes, err := jsonrs.Marshal(res)
	require.NoError(t, err, "failed to marshal response")

	var out types.Response
	require.NoError(t, jsonrs.Unmarshal(resBytes, &out), "failed to unmarshal response")
	require.NotNil(t, out)
	return out
}
