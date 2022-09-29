package stats

import (
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/metric"
	"gopkg.in/alexcesaro/statsd.v2"
)

type statsdConfig struct {
	enabled         bool
	tagsFormat      string
	excludedTags    []string
	statsdServerURL string
	instanceID      string
	samplingRate    float32
	periodic        periodicStatsConfig
}
type periodicStatsConfig struct {
	enabled                 bool
	statsCollectionInterval int64
	enableCPUStats          bool
	enableMemStats          bool
	enableGCStats           bool
	metricManager           metric.Manager
}

// statsdDefaultTags returns the default tags to use for statsd
func (c *statsdConfig) statsdDefaultTags() statsd.Option {
	tags := []string{"instanceName", c.instanceID}
	if len(config.GetKubeNamespace()) > 0 {
		tags = append(tags, "namespace", config.GetKubeNamespace())
	}
	return statsd.Tags(tags...)
}

// statsdTagsFormat returns the tags format to use for statsd
func (c *statsdConfig) statsdTagsFormat() statsd.Option {
	switch c.tagsFormat {
	case "datadog":
		return statsd.TagsFormat(statsd.Datadog)
	default:
		return statsd.TagsFormat(statsd.InfluxDB)
	}
}

type statsdState struct {
	conn            statsd.Option
	client          *statsdClient
	connEstablished bool
	rc              runtimeStatsCollector
	mc              metricStatsCollector

	clientsLock    sync.RWMutex
	clients        map[string]*statsdClient
	pendingClients map[string]*statsdClient
}

// statsdClient is a wrapper around statsd.Client.
// We use this wrapper to allow for filling the actual statsd client at a later stage,
// in case a connection cannot be established immediately at startup.
type statsdClient struct {
	samplingRate float32
	tags         []string
	statsd       *statsd.Client
}

// ready returns true if the statsd client is ready to be used (not nil).
func (sc *statsdClient) ready() bool {
	return sc.statsd != nil
}
