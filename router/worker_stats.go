package router

import (
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/cache"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// workerStats caches the per-job measurements of a worker, so that the hot path doesn't
// need to build a new measurement (tags map, tag sanitisation, client lookup) for every job.
type workerStats struct {
	deliveryLatency     *cache.StatsCache[deliveryLatencyLabels]
	deliveryPayloadSize *cache.StatsCache[deliveryPayloadSizeLabels]
	routerResponseCount *cache.StatsCache[routerResponseCountLabels]
	eventDelivery       *cache.StatsCache[eventDeliveryLabels]
	eventDeliveryTime   *cache.StatsCache[eventDeliveryTimeLabels]
}

func newWorkerStats(s stats.Stats) *workerStats {
	return &workerStats{
		deliveryLatency: cache.NewStatsCache(func(l deliveryLatencyLabels) stats.Measurement {
			return s.NewTaggedStat("delivery_latency", stats.TimerType, l.ToStatTags())
		}),
		deliveryPayloadSize: cache.NewStatsCache(func(l deliveryPayloadSizeLabels) stats.Measurement {
			return s.NewTaggedStat("router_delivery_payload_size_bytes", stats.HistogramType, l.ToStatTags())
		}),
		routerResponseCount: cache.NewStatsCache(func(l routerResponseCountLabels) stats.Measurement {
			return s.NewTaggedStat("router_response_counts", stats.CountType, l.ToStatTags())
		}),
		eventDelivery: cache.NewStatsCache(func(l eventDeliveryLabels) stats.Measurement {
			return s.NewTaggedStat("event_delivery", stats.CountType, l.ToStatTags())
		}),
		eventDeliveryTime: cache.NewStatsCache(func(l eventDeliveryTimeLabels) stats.Measurement {
			return s.NewTaggedStat("event_delivery_time", stats.TimerType, l.ToStatTags())
		}),
	}
}

type deliveryLatencyLabels struct {
	DestType        string
	DestinationID   string
	DestinationName string
	WorkspaceID     string
}

func (l deliveryLatencyLabels) ToStatTags() stats.Tags {
	return stats.Tags{
		"module":      "router",
		"destType":    l.DestType,
		"destination": misc.GetTagName(l.DestinationID, l.DestinationName),
		"workspaceId": l.WorkspaceID,
	}
}

type deliveryPayloadSizeLabels struct {
	DestType      string
	WorkspaceID   string
	DestinationID string
}

func (l deliveryPayloadSizeLabels) ToStatTags() stats.Tags {
	return stats.Tags{
		"destType":      l.DestType,
		"workspaceID":   l.WorkspaceID,
		"destinationID": l.DestinationID,
	}
}

type routerResponseCountLabels struct {
	DestType        string
	RespStatusCode  string
	DestinationID   string
	DestinationName string
	WorkspaceID     string
	Alert           bool
	ErrorAt         string
	Retry           bool
}

func (l routerResponseCountLabels) ToStatTags() stats.Tags {
	return stats.Tags{
		"destType":       l.DestType,
		"respStatusCode": l.RespStatusCode,
		"destination":    misc.GetTagName(l.DestinationID, l.DestinationName),
		"destId":         l.DestinationID,
		"workspaceId":    l.WorkspaceID,
		// To indicate if the failure should be alerted for router-aborted-count
		"alert": strconv.FormatBool(l.Alert),
		// To specify at which point failure happened
		"errorAt": l.ErrorAt,
		"retry":   strconv.FormatBool(l.Retry),
	}
}

type eventDeliveryLabels struct {
	DestType        string
	DestinationID   string
	DestinationName string
	WorkspaceID     string
	SourceID        string
}

func (l eventDeliveryLabels) ToStatTags() stats.Tags {
	return stats.Tags{
		"module":      "router",
		"destType":    l.DestType,
		"destID":      l.DestinationID,
		"destination": misc.GetTagName(l.DestinationID, l.DestinationName),
		"workspaceId": l.WorkspaceID,
		"source":      l.SourceID,
	}
}

type eventDeliveryTimeLabels struct {
	DestType        string
	DestinationID   string
	DestinationName string
	WorkspaceID     string
	SourceID        string
	SourceCategory  string
}

func (l eventDeliveryTimeLabels) ToStatTags() stats.Tags {
	return stats.Tags{
		"module":         "router",
		"destType":       l.DestType,
		"destID":         l.DestinationID,
		"destination":    misc.GetTagName(l.DestinationID, l.DestinationName),
		"workspaceId":    l.WorkspaceID,
		"sourceId":       l.SourceID,
		"sourceCategory": l.SourceCategory,
	}
}
