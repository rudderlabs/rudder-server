package stats

//go:generate mockgen -destination=../../mocks/stats/mock_stats.go -package=mocks_stats github.com/rudderlabs/rudder-server/services/stats Stats
//go:generate mockgen -destination=../../mocks/stats/mock_rudderstats.go -package=mocks_stats github.com/rudderlabs/rudder-server/services/stats RudderStats

import (
	"fmt"
	"strconv"
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
var writeKeyClientsMap = make(map[string]*statsd.Client)
var batchDestClientsMap = make(map[string]*statsd.Client)
var destClientsMap = make(map[string]*statsd.Client)
var routerClientsMap = make(map[string]*statsd.Client)
var taggedClientsMap = make(map[string]*statsd.Client)
var procErrorClientsMap = make(map[string]*statsd.Client)
var jobsdbClientsMap = make(map[string]*statsd.Client)
var migratorsMap = make(map[string]*statsd.Client)
var statsEnabled bool
var statsdServerURL string
var instanceID string
var conn statsd.Option
var writeKeyClientsMapLock sync.Mutex
var batchDestClientsMapLock sync.Mutex
var destClientsMapLock sync.Mutex
var routerClientsMapLock sync.Mutex
var taggedClientsMapLock sync.Mutex
var procErrorClientsMapLock sync.Mutex
var jobsdbClientsMapLock sync.Mutex
var migratorsMapLock sync.Mutex
var enabled bool
var statsCollectionInterval int64
var enableCPUStats bool
var enableMemStats bool
var enableGCStats bool
var rc runtimeStatsCollector

// DefaultStats is a common implementation of StatsD stats managements
var DefaultStats Stats

func init() {
	statsEnabled = config.GetBool("enableStats", false)
	statsdServerURL = config.GetEnv("STATSD_SERVER_URL", "localhost:8125")
	instanceID = config.GetEnv("INSTANCE_ID", "")
	enabled = config.GetBool("RuntimeStats.enabled", true)
	statsCollectionInterval = config.GetInt64("RuntimeStats.statsCollectionInterval", 10)
	enableCPUStats = config.GetBool("RuntimeStats.enableCPUStats", true)
	enableMemStats = config.GetBool("RuntimeStats.enabledMemStats", true)
	enableGCStats = config.GetBool("RuntimeStats.enableGCStats", true)
}

// Stats manages provisioning of RudderStats
type Stats interface {
	NewStat(Name string, StatType string) (rStats RudderStats)
	NewLatencyStat(Name string, StatType string) (rStats RudderStats)
	NewBatchStat(Name string, StatType string, index int) (rStats RudderStats)
	NewWriteKeyStat(Name string, StatType string, writeKey string) (rStats RudderStats)
	NewBatchDestStat(Name string, StatType string, destID string) RudderStats
	NewDestStat(Name string, StatType string, destID string) RudderStats
	NewTaggedStat(Name string, StatType string, tags map[string]string) RudderStats
	GetRouterStat(Name string, StatType string, destName string, respStatusCode int) RudderStats
	GetProcErrorStat(Name string, StatType string, destName string, statusCode int, stage string) RudderStats
	NewJobsDBStat(Name string, StatType string, customVal string) RudderStats
	NewMigratorStat(Name string, StatType string, customVal string) RudderStats
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
}

// RudderStatsT is the default implementation of a StatsD stat
type RudderStatsT struct {
	Name        string
	StatType    string
	Timing      statsd.Timing
	writeKey    string
	DestID      string
	Client      *statsd.Client
	dontProcess bool
}

//Setup creates a new statsd client
func Setup() {
	var err error
	conn = statsd.Address(statsdServerURL)
	client, err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID))
	if err != nil {
		// If nothing is listening on the target port, an error is returned and
		// the returned client does nothing but is still usable. So we can
		// just log the error and go on.
		logger.Error(err)
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

// NewLatencyStat creates a new RudderStats with provided Name and Type
// This function is introduced with the intention to sample stats in the future.
func (s *HandleT) NewLatencyStat(Name string, StatType string) (rStats RudderStats) {
	return s.NewStat(Name, StatType)
}

// NewBatchStat creates a new RudderStats with provided Name and Type
func (s *HandleT) NewBatchStat(Name string, StatType string, index int) (rStats RudderStats) {
	return s.NewStat(Name, StatType)
}

// NewStat creates a new RudderStats with provided Name and Type
// Deprecated: Use DefaultStats for managing stats instead
func NewStat(Name string, StatType string) (rStats RudderStats) {
	return DefaultStats.NewStat(Name, StatType)
}

func (s *HandleT) NewTaggedStat(Name string, StatType string, tags map[string]string) (rStats RudderStats) {
	taggedClientsMapLock.Lock()
	defer taggedClientsMapLock.Unlock()

	tags["instanceName"] = instanceID
	tagStr := StatType
	tagVals := make([]string, 0, len(tags)*2)
	for tagName, tagVal := range tags {
		tagStr += fmt.Sprintf(`|%s|%s`, tagName, tagVal)
		tagVals = append(tagVals, tagName, tagVal)
	}
	if _, found := taggedClientsMap[tagStr]; !found {
		var err error
		taggedClientsMap[tagStr], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags(tagVals...))
		if err != nil {
			logger.Error(err)
		}
	}

	return &RudderStatsT{
		Name:        Name,
		StatType:    StatType,
		Client:      taggedClientsMap[tagStr],
		dontProcess: false,
	}
}

func NewTaggedStat(Name string, StatType string, tags map[string]string) (rStats RudderStats) {
	return DefaultStats.NewTaggedStat(Name, StatType, tags)
}

/*
NewWriteKeyStat is used to create new writekey specific stat.
Writekey is added as the value of 'writekey' tags in this case.
If writekey has been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) NewWriteKeyStat(Name string, StatType string, writeKey string) (rStats RudderStats) {
	writeKeyClientsMapLock.Lock()
	defer writeKeyClientsMapLock.Unlock()
	if _, found := writeKeyClientsMap[writeKey]; !found {
		var err error
		writeKeyClientsMap[writeKey], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "writekey", writeKey))
		if err != nil {
			// If nothing is listening on the target port, an error is returned and
			// the returned client does nothing but is still usable. So we can
			// just log the error and go on.
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:     Name,
		StatType: StatType,
		writeKey: writeKey,
		Client:   writeKeyClientsMap[writeKey],
	}
}

// NewWriteKeyStat is used to create new writekey specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func NewWriteKeyStat(Name string, StatType string, writeKey string) (rStats RudderStats) {
	return DefaultStats.NewWriteKeyStat(Name, StatType, writeKey)
}

/*
NewBatchDestStat is used to create new destination specific stat.
Destination id Writekey is added as the value of 'destID' tag in this case.
If destination id has been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) NewBatchDestStat(Name string, StatType string, destID string) RudderStats {
	batchDestClientsMapLock.Lock()
	defer batchDestClientsMapLock.Unlock()
	if _, found := batchDestClientsMap[destID]; !found {
		var err error
		batchDestClientsMap[destID], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destID", destID))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:     Name,
		StatType: StatType,
		DestID:   destID,
		Client:   batchDestClientsMap[destID],
	}
}

// NewBatchDestStat is used to create new destination specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func NewBatchDestStat(Name string, StatType string, destID string) RudderStats {
	return DefaultStats.NewBatchDestStat(Name, StatType, destID)
}

/*
NewDestStat is used to create new destination specific stat.
Destination id Writekey is added as the value of 'destID' tag in this case.
If destination id has been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) NewDestStat(Name string, StatType string, destID string) RudderStats {
	destClientsMapLock.Lock()
	defer destClientsMapLock.Unlock()
	if _, found := destClientsMap[destID]; !found {
		var err error
		destClientsMap[destID], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destID", destID))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:        Name,
		StatType:    StatType,
		DestID:      destID,
		Client:      destClientsMap[destID],
		dontProcess: false,
	}
}

// NewDestStat is used to create new destination specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func NewDestStat(Name string, StatType string, destID string) RudderStats {
	return DefaultStats.NewDestStat(Name, StatType, destID)
}

/*
GetRouterStat is used to create new destination specific stat.
Destination name and response status are added as tags in this case.
If Destination name and response status have been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) GetRouterStat(Name string, StatType string, destName string, respStatusCode int) RudderStats {
	routerClientsMapLock.Lock()
	defer routerClientsMapLock.Unlock()
	key := fmt.Sprintf("%s|%d", destName, respStatusCode)
	if _, found := routerClientsMap[key]; !found {
		var err error
		routerClientsMap[key], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destName", destName, "respStatusCode", strconv.Itoa(respStatusCode)))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:        Name,
		StatType:    StatType,
		Client:      routerClientsMap[key],
		dontProcess: false,
	}
}

// GetRouterStat is used to create new destination specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func GetRouterStat(Name string, StatType string, destName string, respStatusCode int) RudderStats {
	return DefaultStats.GetRouterStat(Name, StatType, destName, respStatusCode)
}

/*
GetProcErrorStat is used to create new destination specific stat.
Destination name and response status are added as tags in this case.
If Destination name and response status have been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) GetProcErrorStat(Name string, StatType string, destName string, statusCode int, stage string) RudderStats {
	procErrorClientsMapLock.Lock()
	defer procErrorClientsMapLock.Unlock()
	key := fmt.Sprintf("%s|%d|%s", destName, statusCode, stage)
	if _, found := procErrorClientsMap[key]; !found {
		var err error
		procErrorClientsMap[key], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destName", destName, "statusCode", strconv.Itoa(statusCode), "stage", stage))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:        Name,
		StatType:    StatType,
		Client:      procErrorClientsMap[key],
		dontProcess: false,
	}
}

// GetProcErrorStat is used to create new destination specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func GetProcErrorStat(Name string, StatType string, destName string, statusCode int, stage string) RudderStats {
	return DefaultStats.GetProcErrorStat(Name, StatType, destName, statusCode, stage)
}

/*
NewJobsDBStat is used to create new JobsDB specific stat.
JobsDB customVal is added as the value of 'customVal' tag in this case.
If customVal has been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) NewJobsDBStat(Name string, StatType string, customVal string) RudderStats {
	jobsdbClientsMapLock.Lock()
	defer jobsdbClientsMapLock.Unlock()
	if _, found := jobsdbClientsMap[customVal]; !found {
		var err error
		jobsdbClientsMap[customVal], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "customVal", customVal))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:     Name,
		StatType: StatType,
		Client:   jobsdbClientsMap[customVal],
	}
}

// NewJobsDBStat is used to create new JobsDB specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func NewJobsDBStat(Name string, StatType string, customVal string) RudderStats {
	return DefaultStats.NewJobsDBStat(Name, StatType, customVal)
}

/*
NewMigratorStat is used to create new Migrator specific stat.
Migrator migrationType is added as the value of 'migrationType' tag in this case.
If migrationType has been used on this function before, a RudderStats with the same underlying client will be returned.
*/
func (s *HandleT) NewMigratorStat(Name string, StatType string, migrationType string) RudderStats {
	migratorsMapLock.Lock()
	defer migratorsMapLock.Unlock()
	if _, found := migratorsMap[migrationType]; !found {
		var err error
		migratorsMap[migrationType], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "migrationType", migrationType))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStatsT{
		Name:     Name,
		StatType: StatType,
		Client:   migratorsMap[migrationType],
	}

}

// NewMigratorStat is used to create new Migrator specific stat.
// Deprecated: Use DefaultStats for managing stats instead
func NewMigratorStat(Name string, StatType string, customVal string) RudderStats {
	return DefaultStats.NewMigratorStat(Name, StatType, customVal)
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
