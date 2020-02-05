package stats

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
var jobsdbClientsMap = make(map[string]*statsd.Client)
var statsEnabled bool
var statsdServerURL string
var instanceID string
var conn statsd.Option
var writeKeyClientsMapLock sync.Mutex
var batchDestClientsMapLock sync.Mutex
var destClientsMapLock sync.Mutex
var jobsdbClientsMapLock sync.Mutex

func init() {
	config.Initialize()
	statsEnabled = config.GetBool("enableStats", false)
	statsdServerURL = config.GetEnv("STATSD_SERVER_URL", "localhost:8125")
	instanceID = config.GetEnv("INSTANCE_ID", "")

	var err error
	conn = statsd.Address(statsdServerURL)
	client, err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID))
	if err != nil {
		// If nothing is listening on the target port, an error is returned and
		// the returned client does nothing but is still usable. So we can
		// just log the error and go on.
		fmt.Println(err)
	}
}

func NewStat(Name string, StatType string) (rStats *RudderStats) {
	return &RudderStats{
		Name:     Name,
		StatType: StatType,
		Client:   client,
	}
}

// NewWriteKeyStat is used to create new writekey specific stat. Writekey is added as one of the tags in this case
func NewWriteKeyStat(Name string, StatType string, writeKey string) (rStats *RudderStats) {
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
	return &RudderStats{
		Name:     Name,
		StatType: StatType,
		writeKey: writeKey,
		Client:   writeKeyClientsMap[writeKey],
	}
}

func NewBatchDestStat(Name string, StatType string, destID string) *RudderStats {
	batchDestClientsMapLock.Lock()
	defer batchDestClientsMapLock.Unlock()
	if _, found := batchDestClientsMap[destID]; !found {
		var err error
		batchDestClientsMap[destID], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destID", destID))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStats{
		Name:     Name,
		StatType: StatType,
		DestID:   destID,
		Client:   batchDestClientsMap[destID],
	}
}

func NewDestStat(Name string, StatType string, destID string) *RudderStats {
	destClientsMapLock.Lock()
	defer destClientsMapLock.Unlock()
	if _, found := destClientsMap[destID]; !found {
		var err error
		destClientsMap[destID], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "destID", destID))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStats{
		Name:        Name,
		StatType:    StatType,
		DestID:      destID,
		Client:      destClientsMap[destID],
		dontProcess: true,
	}
}

func NewJobsDBStat(Name string, StatType string, customVal string) *RudderStats {
	jobsdbClientsMapLock.Lock()
	defer jobsdbClientsMapLock.Unlock()
	if _, found := jobsdbClientsMap[customVal]; !found {
		var err error
		jobsdbClientsMap[customVal], err = statsd.New(conn, statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("instanceName", instanceID, "customVal", customVal))
		if err != nil {
			logger.Error(err)
		}
	}
	return &RudderStats{
		Name:     Name,
		StatType: StatType,
		Client:   jobsdbClientsMap[customVal],
	}

}

func (rStats *RudderStats) Count(n int) {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	misc.Assert(rStats.StatType == CountType)
	rStats.Client.Count(rStats.Name, n)
}

func (rStats *RudderStats) Increment() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	misc.Assert(rStats.StatType == CountType)
	rStats.Client.Increment(rStats.Name)
}

func (rStats *RudderStats) Gauge(value interface{}) {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	misc.Assert(rStats.StatType == GaugeType)
	rStats.Client.Gauge(rStats.Name, value)
}

func (rStats *RudderStats) Start() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing = rStats.Client.NewTiming()
}

func (rStats *RudderStats) End() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing.Send(rStats.Name)
}

func (rStats *RudderStats) DeferredTimer() {
	if !statsEnabled || rStats.dontProcess {
		return
	}
	rStats.Client.NewTiming().Send(rStats.Name)
}

type RudderStats struct {
	Name        string
	StatType    string
	Timing      statsd.Timing
	writeKey    string
	DestID      string
	Client      *statsd.Client
	dontProcess bool
}
