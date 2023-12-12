package stats

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

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
	newRand := rand.New(rand.NewSource(time.Now().UnixNano())) // skipcq: GSC-G404
	for i := 0; i < 10; i++ {
		sourceTag := fmt.Sprint(i)
		sourceStat := statMap[sourceTag]

		randInt := 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestSucceeded()
		}
		counterMap[sourceTag].succeeded += randInt
		counterMap[sourceTag].total += randInt

		randInt = 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestDropped()
		}
		counterMap[sourceTag].dropped += randInt
		counterMap[sourceTag].total += randInt

		randInt = 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestSuppressed()
		}
		counterMap[sourceTag].suppressed += randInt
		counterMap[sourceTag].total += randInt

		randInt = 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestFailed("reason")
		}
		counterMap[sourceTag].failed += randInt
		counterMap[sourceTag].total += randInt

		randInt = 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestEventsSucceeded(10)
		}
		counterMap[sourceTag].eventsSucceeded += randInt * 10
		counterMap[sourceTag].eventsTotal += randInt * 10
		counterMap[sourceTag].total += randInt
		counterMap[sourceTag].succeeded += randInt

		randInt = 1 + newRand.Int()%9 // skipcq: GSC-G404
		for j := 0; j < randInt; j++ {
			sourceStat.RequestEventsFailed(10, "reason")
		}
		counterMap[sourceTag].eventsFailed += randInt * 10
		counterMap[sourceTag].eventsTotal += randInt * 10
		counterMap[sourceTag].total += randInt
		counterMap[sourceTag].failed += randInt
	}

	// report
	statsStore, err := memstats.New()
	require.NoError(t, err)
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
			"sourceType":  statMap[sourceTag].SourceType,
			"sdkVersion":  statMap[sourceTag].Version,
		}
		failedTags := map[string]string{
			"source":      statMap[sourceTag].Source,
			"sourceID":    statMap[sourceTag].SourceID,
			"workspaceId": statMap[sourceTag].WorkspaceID,
			"writeKey":    statMap[sourceTag].WriteKey,
			"reqType":     statMap[sourceTag].ReqType,
			"sourceType":  statMap[sourceTag].SourceType,
			"sdkVersion":  statMap[sourceTag].Version,
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

func getSourceStat(statMap map[string]*SourceStat, sourceTag string) {
	statMap[sourceTag] = &SourceStat{
		Source:      sourceTag,
		SourceID:    trand.String(10),
		WorkspaceID: trand.String(10),
		WriteKey:    trand.String(10),
		ReqType:     trand.String(10),
		SourceType:  trand.String(10),
		Version:     trand.String(10),
	}
}

type counter struct {
	total, succeeded, failed, dropped, suppressed int
	eventsTotal, eventsSucceeded, eventsFailed    int
}
