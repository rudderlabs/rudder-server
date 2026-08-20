package stats

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	"github.com/stretchr/testify/require"
)

func TestReport(t *testing.T) {
	// populate some SourceStats
	statMap := make(map[string]*SourceStat)
	for i := range 10 {
		getSourceStat(statMap, fmt.Sprint(i))
	}

	// populate some request, event counts
	// keep track using some counters
	counterMap := make(map[string]*counter)
	for i := range 10 {
		counterMap[fmt.Sprint(i)] = &counter{}
	}
	newRand := rand.New(rand.NewSource(time.Now().UnixNano())) // skipcq: GSC-G404
	for i := range 10 {
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
			sourceStat.RequestDropped(gwtypes.ReasonRateLimit)
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
			sourceStat.RequestFailed(gwtypes.ReasonInvalidJSON)
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
			sourceStat.RequestEventsFailed(10, gwtypes.ReasonInvalidJSON)
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
	for i := range 10 {
		sourceTag := fmt.Sprint(i)
		tags := map[string]string{
			"source":        statMap[sourceTag].Source,
			"sourceID":      statMap[sourceTag].SourceID,
			"workspaceId":   statMap[sourceTag].WorkspaceID,
			"writeKey":      statMap[sourceTag].WriteKey,
			"reqType":       statMap[sourceTag].ReqType,
			"sourceType":    statMap[sourceTag].SourceType,
			"sdkVersion":    statMap[sourceTag].Version,
			"sourceDefName": strings.ToLower(statMap[sourceTag].SourceDefName),
		}
		failedTags := map[string]string{
			"source":        statMap[sourceTag].Source,
			"sourceID":      statMap[sourceTag].SourceID,
			"workspaceId":   statMap[sourceTag].WorkspaceID,
			"writeKey":      statMap[sourceTag].WriteKey,
			"reqType":       statMap[sourceTag].ReqType,
			"sourceType":    statMap[sourceTag].SourceType,
			"sdkVersion":    statMap[sourceTag].Version,
			"reason":        gwtypes.ReasonInvalidJSON.Value(),
			"sourceDefName": strings.ToLower(statMap[sourceTag].SourceDefName),
		}
		// a drop now carries its own reason, kept apart from the failure reason: one SourceStat collects both
		droppedTags := map[string]string{
			"source":        statMap[sourceTag].Source,
			"sourceID":      statMap[sourceTag].SourceID,
			"workspaceId":   statMap[sourceTag].WorkspaceID,
			"writeKey":      statMap[sourceTag].WriteKey,
			"reqType":       statMap[sourceTag].ReqType,
			"sourceType":    statMap[sourceTag].SourceType,
			"sdkVersion":    statMap[sourceTag].Version,
			"reason":        gwtypes.ReasonRateLimit.Value(),
			"sourceDefName": strings.ToLower(statMap[sourceTag].SourceDefName),
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
				droppedTags,
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
		Source:        sourceTag,
		SourceID:      trand.String(10),
		WorkspaceID:   trand.String(10),
		WriteKey:      trand.String(10),
		ReqType:       trand.String(10),
		SourceType:    trand.String(10),
		Version:       trand.String(10),
		SourceDefName: trand.String(10),
	}
}

type counter struct {
	total, succeeded, failed, dropped, suppressed int
	eventsTotal, eventsSucceeded, eventsFailed    int
}
