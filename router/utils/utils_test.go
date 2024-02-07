package utils_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

func TestUpdateProcessedEventsMetrics(t *testing.T) {
	sourceID1 := "test-source-id-1"
	destinationID1 := "test-destination-id-1"
	sourceID2 := "test-source-id-2"
	destinationID2 := "test-destination-id-2"
	destType1 := "test-dest-type-1"
	destType2 := "test-dest-type-2"
	destinationID3 := "test-destination-id-3"
	jobIDConnectionDetailsMap := map[int64]jobsdb.ConnectionDetails{
		1: {
			SourceID:      sourceID1,
			DestinationID: destinationID1,
		},
		2: {
			SourceID:      sourceID2,
			DestinationID: destinationID2,
		},
		3: {
			SourceID:      sourceID1,
			DestinationID: destinationID2,
		},
		4: {
			SourceID:      sourceID2,
			DestinationID: destinationID1,
		},
		5: {
			SourceID:      sourceID1,
			DestinationID: destinationID2,
		},
		6: {
			SourceID:      sourceID2,
			DestinationID: destinationID3,
		},
		7: {
			SourceID:      sourceID1,
			DestinationID: destinationID3,
		},
		8: {
			SourceID:      sourceID1,
			DestinationID: destinationID3,
		},
	}

	var routerStatusList []*jobsdb.JobStatusT
	routerStatusList = append(routerStatusList, &jobsdb.JobStatusT{
		JobID:     1,
		JobState:  jobsdb.Succeeded.State,
		ErrorCode: "200",
	}, &jobsdb.JobStatusT{
		JobID:     2,
		JobState:  jobsdb.Aborted.State,
		ErrorCode: "404",
	}, &jobsdb.JobStatusT{
		JobID:     3,
		JobState:  jobsdb.Failed.State,
		ErrorCode: "429",
	}, &jobsdb.JobStatusT{
		JobID:     4,
		JobState:  jobsdb.Aborted.State,
		ErrorCode: "409",
	}, &jobsdb.JobStatusT{
		JobID:     5,
		JobState:  jobsdb.Failed.State,
		ErrorCode: "500",
	})
	var batchRouterStatusList []*jobsdb.JobStatusT
	batchRouterStatusList = append(batchRouterStatusList, &jobsdb.JobStatusT{
		JobID:     6,
		JobState:  jobsdb.Succeeded.State,
		ErrorCode: "200",
	}, &jobsdb.JobStatusT{
		JobID:     7,
		JobState:  jobsdb.Aborted.State,
		ErrorCode: "404",
	}, &jobsdb.JobStatusT{
		JobID:     8,
		JobState:  jobsdb.Aborted.State,
		ErrorCode: "404",
	},
	)
	statsStore, err := memstats.New()
	require.NoError(t, err)
	routerutils.UpdateProcessedEventsMetrics(statsStore, "router", destType1, routerStatusList, jobIDConnectionDetailsMap)
	routerutils.UpdateProcessedEventsMetrics(statsStore, "batch_router", destType2, batchRouterStatusList, jobIDConnectionDetailsMap)
	require.Equal(t, 7, len(statsStore.GetAll()))
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "router",
		"destType":      destType1,
		"state":         jobsdb.Succeeded.State,
		"code":          "200",
		"sourceId":      sourceID1,
		"destinationId": destinationID1,
	}).LastValue())
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "router",
		"destType":      destType1,
		"state":         jobsdb.Aborted.State,
		"code":          "404",
		"sourceId":      sourceID2,
		"destinationId": destinationID2,
	}).LastValue())
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "router",
		"destType":      destType1,
		"state":         jobsdb.Failed.State,
		"code":          "429",
		"sourceId":      sourceID1,
		"destinationId": destinationID2,
	}).LastValue())
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "router",
		"destType":      destType1,
		"state":         jobsdb.Aborted.State,
		"code":          "409",
		"sourceId":      sourceID2,
		"destinationId": destinationID1,
	}).LastValue())
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "router",
		"destType":      destType1,
		"state":         jobsdb.Failed.State,
		"code":          "500",
		"sourceId":      sourceID1,
		"destinationId": destinationID2,
	}).LastValue())
	require.Equal(t, 1.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "batch_router",
		"destType":      destType2,
		"state":         jobsdb.Succeeded.State,
		"code":          "200",
		"sourceId":      sourceID2,
		"destinationId": destinationID3,
	}).LastValue())
	require.Equal(t, 2.0, statsStore.Get("pipeline_processed_events", map[string]string{
		"module":        "batch_router",
		"destType":      destType2,
		"state":         jobsdb.Aborted.State,
		"code":          "404",
		"sourceId":      sourceID1,
		"destinationId": destinationID3,
	}).LastValue())
}
