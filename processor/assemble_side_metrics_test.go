package processor

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// assembleTestConnKey is the single connection key used across these tests.
const assembleTestConnKey = "conn1"

// statusDetailsMapWith builds a single-connection, single-event status-details map, the
// shape assembleSideStatusDetailMetrics expects for each of its eight map arguments.
func statusDetailsMapWith(eventName string, count int64) map[string]map[string]*reportingtypes.StatusDetail {
	return map[string]map[string]*reportingtypes.StatusDetail{
		assembleTestConnKey: {
			eventName: {
				Status:    "succeeded",
				Count:     count,
				EventName: eventName,
				EventType: "track",
			},
		},
	}
}

func TestAssembleSideStatusDetailMetrics(t *testing.T) {
	cd := &reportingtypes.ConnectionDetails{SourceID: "src1", DestinationID: "dest1"}

	t.Run("each status-details map produces rows under its own PU", func(t *testing.T) {
		testCases := []struct {
			name             string
			mapName          string
			expectedInPU     string
			expectedPU       string
			expectedInitial  bool
			expectedTerminal bool
		}{
			{
				name:             "statusDetailsMap emits a GATEWAY row",
				mapName:          "statusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.GATEWAY,
				expectedInitial:  true,
				expectedTerminal: false,
			},
			{
				name:             "enricherStatusDetailsMap emits under GATEWAY, not its own PU",
				mapName:          "enricherStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.GATEWAY,
				expectedInitial:  true,
				expectedTerminal: false,
			},
			{
				name:             "botManagementStatusDetailsMap emits a BOT_MANAGEMENT row",
				mapName:          "botManagementStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.BOT_MANAGEMENT,
				expectedInitial:  false,
				expectedTerminal: false,
			},
			{
				name:             "eventBlockingStatusDetailsMap emits an EVENT_BLOCKING row",
				mapName:          "eventBlockingStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.EVENT_BLOCKING,
				expectedInitial:  false,
				expectedTerminal: false,
			},
			{
				name:             "userSuppressionStatusDetailsMap emits a USER_SUPPRESSION row",
				mapName:          "userSuppressionStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.USER_SUPPRESSION,
				expectedInitial:  false,
				expectedTerminal: false,
			},
			{
				name:             "dedupStatusDetailsMap emits a DEDUP row",
				mapName:          "dedupStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.DEDUP,
				expectedInitial:  false,
				expectedTerminal: false,
			},
			{
				name:             "gatewayIngestedStatusDetailsMap emits a GATEWAY_INGESTED row",
				mapName:          "gatewayIngestedStatusDetailsMap",
				expectedInPU:     "",
				expectedPU:       reportingtypes.GATEWAY_INGESTED,
				expectedInitial:  false,
				expectedTerminal: false,
			},
			{
				name:             "destFilterStatusDetailMap emits a DESTINATION_FILTER row chained from GATEWAY",
				mapName:          "destFilterStatusDetailMap",
				expectedInPU:     reportingtypes.GATEWAY,
				expectedPU:       reportingtypes.DESTINATION_FILTER,
				expectedInitial:  false,
				expectedTerminal: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				connectionDetailsMap := map[string]*reportingtypes.ConnectionDetails{assembleTestConnKey: cd}
				populated := statusDetailsMapWith("evt", 5)
				empty := map[string]map[string]*reportingtypes.StatusDetail{}

				args := map[string]map[string]map[string]*reportingtypes.StatusDetail{
					"statusDetailsMap":                empty,
					"enricherStatusDetailsMap":        empty,
					"botManagementStatusDetailsMap":   empty,
					"eventBlockingStatusDetailsMap":   empty,
					"userSuppressionStatusDetailsMap": empty,
					"dedupStatusDetailsMap":           empty,
					"gatewayIngestedStatusDetailsMap": empty,
					"destFilterStatusDetailMap":       empty,
				}
				args[tc.mapName] = populated

				proc := &Handle{}
				metrics := proc.assembleSideStatusDetailMetrics(
					connectionDetailsMap,
					args["statusDetailsMap"],
					args["enricherStatusDetailsMap"],
					args["botManagementStatusDetailsMap"],
					args["eventBlockingStatusDetailsMap"],
					args["userSuppressionStatusDetailsMap"],
					args["dedupStatusDetailsMap"],
					args["gatewayIngestedStatusDetailsMap"],
					args["destFilterStatusDetailMap"],
				)

				require.Len(t, metrics, 1)
				require.Equal(t, *cd, metrics[0].ConnectionDetails)
				require.Equal(t, tc.expectedInPU, metrics[0].InPU)
				require.Equal(t, tc.expectedPU, metrics[0].PU)
				require.Equal(t, tc.expectedInitial, metrics[0].InitialPU)
				require.Equal(t, tc.expectedTerminal, metrics[0].TerminalPU)
				require.Equal(t, int64(5), metrics[0].StatusDetail.Count)
			})
		}
	})

	t.Run("all eight maps populated for one connection key emit exactly eight rows, one per PU, with counts carried through", func(t *testing.T) {
		connectionDetailsMap := map[string]*reportingtypes.ConnectionDetails{assembleTestConnKey: cd}

		statusDetailsMap := statusDetailsMapWith("gw-evt", 1)
		enricherStatusDetailsMap := statusDetailsMapWith("enricher-evt", 2)
		botManagementStatusDetailsMap := statusDetailsMapWith("bot-evt", 3)
		eventBlockingStatusDetailsMap := statusDetailsMapWith("block-evt", 4)
		userSuppressionStatusDetailsMap := statusDetailsMapWith("suppress-evt", 5)
		dedupStatusDetailsMap := statusDetailsMapWith("dedup-evt", 6)
		gatewayIngestedStatusDetailsMap := statusDetailsMapWith("ingest-evt", 7)
		destFilterStatusDetailMap := statusDetailsMapWith("filter-evt", 8)

		proc := &Handle{}
		metrics := proc.assembleSideStatusDetailMetrics(
			connectionDetailsMap,
			statusDetailsMap,
			enricherStatusDetailsMap,
			botManagementStatusDetailsMap,
			eventBlockingStatusDetailsMap,
			userSuppressionStatusDetailsMap,
			dedupStatusDetailsMap,
			gatewayIngestedStatusDetailsMap,
			destFilterStatusDetailMap,
		)

		require.Len(t, metrics, 8)

		expected := map[string]struct {
			inPU     string
			pu       string
			initial  bool
			terminal bool
			count    int64
		}{
			"gw-evt":       {"", reportingtypes.GATEWAY, true, false, 1},
			"enricher-evt": {"", reportingtypes.GATEWAY, true, false, 2},
			"bot-evt":      {"", reportingtypes.BOT_MANAGEMENT, false, false, 3},
			"block-evt":    {"", reportingtypes.EVENT_BLOCKING, false, false, 4},
			"suppress-evt": {"", reportingtypes.USER_SUPPRESSION, false, false, 5},
			"dedup-evt":    {"", reportingtypes.DEDUP, false, false, 6},
			"ingest-evt":   {"", reportingtypes.GATEWAY_INGESTED, false, false, 7},
			"filter-evt":   {reportingtypes.GATEWAY, reportingtypes.DESTINATION_FILTER, false, false, 8},
		}

		seen := make(map[string]bool, len(expected))
		for _, m := range metrics {
			eventName := m.StatusDetail.EventName
			exp, ok := expected[eventName]
			require.Truef(t, ok, "unexpected event name %q in emitted metrics", eventName)
			require.False(t, seen[eventName], "duplicate row for event name %q", eventName)
			seen[eventName] = true

			require.Equal(t, *cd, m.ConnectionDetails, "connection details for %q", eventName)
			require.Equal(t, exp.inPU, m.InPU, "inPU for %q", eventName)
			require.Equal(t, exp.pu, m.PU, "PU for %q", eventName)
			require.Equal(t, exp.initial, m.InitialPU, "initial for %q", eventName)
			require.Equal(t, exp.terminal, m.TerminalPU, "terminal for %q", eventName)
			require.Equal(t, exp.count, m.StatusDetail.Count, "count for %q", eventName)
		}
		require.Len(t, seen, 8)

		// ConnectionDetails must be copied per row, not shared: mutating one row's
		// ConnectionDetails must not leak into any other row.
		sort.Slice(metrics, func(i, j int) bool {
			return metrics[i].StatusDetail.EventName < metrics[j].StatusDetail.EventName
		})
		metrics[0].SourceID = "mutated"
		for i := 1; i < len(metrics); i++ {
			require.Equal(t, "src1", metrics[i].SourceID, "row %d must not observe mutation of row 0's ConnectionDetails", i)
		}
	})
}
