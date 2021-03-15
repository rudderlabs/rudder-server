package stats

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"gopkg.in/alexcesaro/statsd.v2"
)

const (
	CountType = "count"
	TimerType = "timer"
	GaugeType = "gauge"
)

var client *statsd.Client
var taggedClientsMap = make(map[string]*statsd.Client)
var statsEnabled bool
var statsTagsFormat string
var statsdServerURL string
var instanceID string
var conn statsd.Option
var taggedClientsMapLock sync.RWMutex
var enabled bool
var statsCollectionInterval int64
var enableCPUStats bool
var enableMemStats bool
var enableGCStats bool
var rc runtimeStatsCollector
var pkgLogger logger.LoggerI
var statsSamplingRate float32

// DefaultStats is a common implementation of StatsD stats managements
var DefaultStats Stats

func init() {
	statsEnabled = config.GetBool("enableStats", false)
	statsTagsFormat = config.GetString("statsTagsFormat", "influxdb")
	statsdServerURL = config.GetEnv("STATSD_SERVER_URL", "localhost:8125")
	instanceID = config.GetEnv("INSTANCE_ID", "")
	enabled = config.GetBool("RuntimeStats.enabled", true)
	statsCollectionInterval = config.GetInt64("RuntimeStats.statsCollectionInterval", 10)
	enableCPUStats = config.GetBool("RuntimeStats.enableCPUStats", true)
	enableMemStats = config.GetBool("RuntimeStats.enabledMemStats", true)
	enableGCStats = config.GetBool("RuntimeStats.enableGCStats", true)
	statsSamplingRate = float32(config.GetFloat64("statsSamplingRate", 1))

	pkgLogger = logger.NewLogger().Child("stats")

}

type Tags map[string]string

// Stats manages provisioning of RudderStats
type Stats interface {
	NewStat(Name string, StatType string) (rStats RudderStats)
	NewTaggedStat(Name string, StatType string, tags Tags) RudderStats
	NewSampledTaggedStat(Name string, StatType string, tags Tags) RudderStats
}

// HandleT is the default implementation of Stats
type HandleT struct {
}

// RudderStats provides functions to interact with StatsD stats
type RudderStats interface {
	Count(n int)
	Increment()

	Gauge(value interface{})

	Start()
	End()
	DeferredTimer()
	SendTiming(duration time.Duration)
}

// RudderStatsT is the default implementation of a StatsD stat
type RudderStatsT struct {
	Name        string
	StatType    string
	Timing      statsd.Timing
	DestID      string
	Client      *statsd.Client
	dontProcess bool
}

//Setup creates a new statsd client
func Setup() {
	var err error
	conn = statsd.Address(statsdServerURL)
	//TODO: Add tags by calling a function...
	client, err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), defaultTags())
	if err != nil {
		// If nothing is listening on the target port, an error is returned and
		// the returned client does nothing but is still usable. So we can
		// just log the error and go on.
		pkgLogger.Error(err)
	}
	if client != nil {
		rruntime.Go(func() {
			collectRuntimeStats(client)
		})
	}

	DefaultStats = &HandleT{}
}

// NewStat creates a new RudderStats with provided Name and Type
func (s *HandleT) NewStat(Name string, StatType string) (rStats RudderStats) {
	return &RudderStatsT{
		Name:     Name,
		StatType: StatType,
		Client:   client,
	}
}

// NewStat creates a new RudderStats with provided Name and Type
// Deprecated: Use DefaultStats for managing stats instead
func NewStat(Name string, StatType string) (rStats RudderStats) {
	return DefaultStats.NewStat(Name, StatType)
}

func (s *HandleT) NewTaggedStat(Name string, StatType string, tags Tags) (rStats RudderStats) {
	return newTaggedStat(Name, StatType, tags, 1)
}

func (s *HandleT) NewSampledTaggedStat(Name string, StatType string, tags Tags) (rStats RudderStats) {
	return newTaggedStat(Name, StatType, tags, statsSamplingRate)
}

func newTaggedStat(Name string, StatType string, tags Tags, samplingRate float32) (rStats RudderStats) {
	tagStr := StatType
	for tagName, tagVal := range tags {
		tagName = strings.ReplaceAll(tagName, ":", "-")
		tagStr += fmt.Sprintf(`|%s|%s`, tagName, tagVal)
	}

	taggedClientsMapLock.RLock()
	taggedClient, found := taggedClientsMap[tagStr]
	taggedClientsMapLock.RUnlock()

	if !found {
		taggedClientsMapLock.Lock()
		tagVals := make([]string, 0, len(tags)*2)
		for tagName, tagVal := range tags {
			tagName = strings.ReplaceAll(tagName, ":", "-")
			tagVal = strings.ReplaceAll(tagVal, ":", "-")
			tagVals = append(tagVals, tagName, tagVal)
		}
		var err error
		taggedClient, err = statsd.New(conn, statsd.TagsFormat(getTagsFormat()), defaultTags(), statsd.Tags(tagVals...), statsd.SampleRate(samplingRate))
		taggedClientsMap[tagStr] = taggedClient
		taggedClientsMapLock.Unlock()
		if err != nil {
			pkgLogger.Error(err)
		}
	}

	return &RudderStatsT{
		Name:        Name,
		StatType:    StatType,
		Client:      taggedClient,
		dontProcess: false,
	}
}

func NewTaggedStat(Name string, StatType string, tags Tags) (rStats RudderStats) {
	return DefaultStats.NewTaggedStat(Name, StatType, tags)
}

// Count increases the stat by n. Only applies to CountType stats
func (rStats *RudderStatsT) Count(n int) {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != CountType {
		panic(fmt.Errorf("rStats.StatType:%s is not count", rStats.StatType))
	}
	rStats.Client.Count(rStats.Name, n)
}

// Increment increases the stat by 1. Is the Equivalent of Count(1). Only applies to CountType stats
func (rStats *RudderStatsT) Increment() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != CountType {
		panic(fmt.Errorf("rStats.StatType:%s is not count", rStats.StatType))
	}
	rStats.Client.Increment(rStats.Name)
}

// Gauge records an absolute value for this stat. Only applies to GaugeType stats
func (rStats *RudderStatsT) Gauge(value interface{}) {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != GaugeType {
		panic(fmt.Errorf("rStats.StatType:%s is not gauge", rStats.StatType))
	}
	rStats.Client.Gauge(rStats.Name, value)
}

// Start starts a new timing for this stat. Only applies to TimerType stats
func (rStats *RudderStatsT) Start() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != TimerType {
		panic(fmt.Errorf("rStats.StatType:%s is not timer", rStats.StatType))
	}
	rStats.Timing = rStats.Client.NewTiming()
}

// End send the time elapsed since the Start()  call of this stat. Only applies to TimerType stats
func (rStats *RudderStatsT) End() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != TimerType {
		panic(fmt.Errorf("rStats.StatType:%s is not timer", rStats.StatType))
	}
	rStats.Timing.Send(rStats.Name)
}

func (rStats *RudderStatsT) DeferredTimer() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	rStats.Client.NewTiming().Send(rStats.Name)
}

// Timing sends a timing for this stat. Only applies to TimerType stats
func (rStats *RudderStatsT) SendTiming(duration time.Duration) {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	if rStats.StatType != TimerType {
		panic(fmt.Errorf("rStats.StatType:%s is not timer", rStats.StatType))
	}
	rStats.Client.Timing(rStats.Name, int(duration/time.Millisecond))
}

func collectRuntimeStats(client *statsd.Client) {
	gaugeFunc := func(key string, val uint64) {
		client.Gauge("runtime_"+key, val)
	}
	rc = newRuntimeStatsCollector(gaugeFunc)
	rc.PauseDur = time.Duration(statsCollectionInterval) * time.Second
	rc.EnableCPU = enableCPUStats
	rc.EnableMem = enableMemStats
	rc.EnableGC = enableGCStats
	if enabled {
		rc.run()
	}

}

// StopRuntimeStats stops collection of runtime stats.
func StopRuntimeStats() {
	close(rc.Done)
}

func getTagsFormat() statsd.TagFormat {
	switch statsTagsFormat {
	case "datadog":
		return statsd.Datadog
	case "influxdb":
		return statsd.InfluxDB
	default:
		return statsd.InfluxDB
	}
}

// returns default Tags to the telegraf request
func defaultTags() statsd.Option {
	if len(config.GetKubeNamespace()) > 0 {
		return statsd.Tags("instanceName", instanceID, "namespace", config.GetKubeNamespace())
	}
	return statsd.Tags("instanceName", instanceID)
}
