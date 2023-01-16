package stats

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	trand "github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/stretchr/testify/require"
)

func getSourceStat(statMap map[string]*SourceStat, sourceTag string) {
	statMap[sourceTag] = &SourceStat{
		Source:      trand.String(10),
		SourceID:    trand.String(10),
		WorkspaceID: trand.String(10),
		WriteKey:    trand.String(10),
		ReqType:     trand.String(10),
	}
}

func TestReport(t *testing.T) {
	// populate some SourceStats
	statMap := make(map[string]*SourceStat)
	for i := 0; i < 10; i++ {
		getSourceStat(statMap, fmt.Sprint(i))
	}

	// populate some request, event counts
	// keep track using some counters
	counterMap := make(map[string]*counter)
	for i := 0; i < 10; i++ {
		counterMap[fmt.Sprint(i)] = &counter{}
	}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		sourceTag := fmt.Sprint(i)
		randInt := rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestSucceeded()
		}
		counterMap[sourceTag].succeeded += randInt
		counterMap[sourceTag].total += randInt
		randInt = rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestDropped()
		}
		counterMap[sourceTag].dropped += randInt
		counterMap[sourceTag].total += randInt
		randInt = rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestSuppressed()
		}
		counterMap[sourceTag].suppressed += randInt
		counterMap[sourceTag].total += randInt
		randInt = rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestFailed("reason")
		}
		counterMap[sourceTag].failed += randInt
		counterMap[sourceTag].total += randInt
		randInt = rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestEventsSucceeded(10)
		}
		counterMap[sourceTag].eventsSucceeded += randInt * 10
		counterMap[sourceTag].eventsTotal += randInt * 10
		counterMap[sourceTag].total += randInt
		counterMap[sourceTag].succeeded += randInt
		randInt = rand.Int() % 10 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			statMap[sourceTag].RequestEventsFailed(10, "reason")
		}
		counterMap[sourceTag].eventsFailed += randInt * 10
		counterMap[sourceTag].eventsTotal += randInt * 10
		counterMap[sourceTag].total += randInt
		counterMap[sourceTag].failed += randInt
	}

	// report
	statsStore := memstats.New()
	for _, v := range statMap {
		v.Report(statsStore)
	}

	// check
	for i := 0; i < 10; i++ {
		sourceTag := fmt.Sprint(i)
		tags := map[string]string{
			"source":      statMap[sourceTag].Source,
			"sourceID":    statMap[sourceTag].SourceID,
			"workspaceId": statMap[sourceTag].WorkspaceID,
			"writeKey":    statMap[sourceTag].WriteKey,
			"reqType":     statMap[sourceTag].ReqType,
		}
		failedTags := map[string]string{
			"source":      statMap[sourceTag].Source,
			"sourceID":    statMap[sourceTag].SourceID,
			"workspaceId": statMap[sourceTag].WorkspaceID,
			"writeKey":    statMap[sourceTag].WriteKey,
			"reqType":     statMap[sourceTag].ReqType,
			"reason":      "reason",
		}
		require.Equal(t,
			float64(counterMap[sourceTag].total),
			statsStore.Get(
				"gateway.write_key_requests",
				tags,
			).LastValue(),
		)
		require.Equal(t,
			float64(counterMap[sourceTag].succeeded),
			statsStore.Get(
				"gateway.write_key_successful_requests",
				tags,
			).LastValue(),
		)
		require.Equal(t,
			float64(counterMap[sourceTag].dropped),
			statsStore.Get(
				"gateway.write_key_dropped_requests",
				tags,
			).LastValue(),
		)
		require.Equal(t,
			float64(counterMap[sourceTag].suppressed),
			statsStore.Get(
				"gateway.write_key_suppressed_requests",
				tags,
			).LastValue(),
		)
		require.Equal(t,
			float64(counterMap[sourceTag].failed),
			statsStore.Get(
				"gateway.write_key_failed_requests",
				failedTags,
			).LastValue(),
		)
		if counterMap[sourceTag].eventsTotal > 0 {
			require.Equal(t,
				float64(counterMap[sourceTag].eventsTotal),
				statsStore.Get(
					"gateway.write_key_events",
					tags,
				).LastValue(),
			)
			require.Equal(t,
				float64(counterMap[sourceTag].eventsSucceeded),
				statsStore.Get(
					"gateway.write_key_successful_events",
					tags,
				).LastValue(),
			)
			require.Equal(t,
				float64(counterMap[sourceTag].eventsFailed),
				statsStore.Get(
					"gateway.write_key_failed_events",
					failedTags,
				).LastValue(),
			)
		}
	}
}

type counter struct {
	total, succeeded, failed, dropped, suppressed int
	eventsTotal, eventsSucceeded, eventsFailed    int
}
