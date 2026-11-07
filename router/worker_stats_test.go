package router

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestWorkerPerJobStats(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)

	w := &worker{
		rt: &Handle{
			destType:                   "WEBHOOK",
			transformerFeaturesService: transformerFeaturesService.NewNoOpService(),
			reloadableConfig: &reloadableConfig{
				transformerProxy:                  config.SingleValueLoader(false),
				skipRtAbortAlertForTransformation: config.SingleValueLoader(false),
				skipRtAbortAlertForDelivery:       config.SingleValueLoader(false),
			},
		},
		logger: logger.NOP,
		stats:  newWorkerStats(statsStore),
	}
	dest := &backendconfig.DestinationT{ID: "destination-id", Name: "destination:name"}
	destinationTag := misc.GetTagName(dest.ID, dest.Name)
	meta := &types.JobMetadataT{
		SourceID:       "source-id",
		SourceCategory: "eventStream",
		ReceivedAt:     time.Now().Add(-time.Second).Format(misc.RFC3339Milli),
	}

	const jobs = 3
	for range jobs {
		status := &jobsdb.JobStatusT{JobState: jobsdb.Succeeded.State, ErrorCode: "200", WorkspaceId: "workspace-id", AttemptNum: 2}
		w.sendEventDeliveryStat(meta, status, dest)
		w.sendRouterResponseCountStat(status, dest, "")
	}
	aborted := &jobsdb.JobStatusT{JobState: jobsdb.Aborted.State, ErrorCode: "400", WorkspaceId: "workspace-id", AttemptNum: 1}
	w.sendEventDeliveryStat(meta, aborted, dest)
	w.sendRouterResponseCountStat(aborted, dest, routerutils.ERROR_AT_DEL)

	require.EqualValues(t, jobs, statsStore.Get("event_delivery", stats.Tags{
		"module":      "router",
		"destType":    "WEBHOOK",
		"destID":      dest.ID,
		"destination": destinationTag,
		"workspaceId": "workspace-id",
		"source":      "source-id",
	}).LastValue(), "aborted jobs are not counted as delivered")

	deliveryTime := statsStore.Get("event_delivery_time", stats.Tags{
		"module":         "router",
		"destType":       "WEBHOOK",
		"destID":         dest.ID,
		"destination":    destinationTag,
		"workspaceId":    "workspace-id",
		"sourceId":       "source-id",
		"sourceCategory": "eventStream",
	})
	require.Len(t, deliveryTime.Durations(), jobs)

	require.EqualValues(t, jobs, statsStore.Get("router_response_counts", stats.Tags{
		"destType":       "WEBHOOK",
		"respStatusCode": "200",
		"destination":    destinationTag,
		"destId":         dest.ID,
		"workspaceId":    "workspace-id",
		"alert":          "true",
		"errorAt":        "",
		"retry":          "true",
	}).LastValue())
	require.EqualValues(t, 1, statsStore.Get("router_response_counts", stats.Tags{
		"destType":       "WEBHOOK",
		"respStatusCode": "400",
		"destination":    destinationTag,
		"destId":         dest.ID,
		"workspaceId":    "workspace-id",
		"alert":          "true",
		"errorAt":        routerutils.ERROR_AT_DEL,
		"retry":          "false",
	}).LastValue())

	w.stats.deliveryLatency.Get(deliveryLatencyLabels{
		DestType: "WEBHOOK", DestinationID: dest.ID, DestinationName: dest.Name, WorkspaceID: "workspace-id",
	}).SendTiming(time.Second)
	require.Len(t, statsStore.Get("delivery_latency", stats.Tags{
		"module":      "router",
		"destType":    "WEBHOOK",
		"destination": destinationTag,
		"workspaceId": "workspace-id",
	}).Durations(), 1)

	w.stats.deliveryPayloadSize.Get(deliveryPayloadSizeLabels{
		DestType: "WEBHOOK", WorkspaceID: "workspace-id", DestinationID: dest.ID,
	}).Observe(42)
	require.Equal(t, []float64{42}, statsStore.Get("router_delivery_payload_size_bytes", stats.Tags{
		"destType":      "WEBHOOK",
		"workspaceID":   "workspace-id",
		"destinationID": dest.ID,
	}).Values())
}
