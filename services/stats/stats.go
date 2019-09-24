package stats

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
var statsEnabled bool

func init() {
	config.Initialize()
	statsEnabled = config.GetBool("enableStats", false)

	var err error
	client, err = statsd.New()
	if err != nil {
		// If nothing is listening on the target port, an error is returned and
		// the returned client does nothing but is still usable. So we can
		// just log the error and go on.
		logger.Error(err)
	}
}

func NewStat(Name string, StatType string) (rStats *RudderStats) {
	return &RudderStats{
		Name:     Name,
		StatType: StatType,
	}
}

// NewWriteKeyStat is used to create new writekey specific stat. Writekey is added as one of the tags in this case
func NewWriteKeyStat(Name string, StatType string, writeKey string) (rStats *RudderStats) {
	if _, found := writeKeyClientsMap[writeKey]; !found {
		var err error
		writeKeyClientsMap[writeKey], err = statsd.New(statsd.TagsFormat(statsd.InfluxDB), statsd.Tags("writekey", writeKey))
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
	}
}

func (rStats *RudderStats) Count(n int) {
	if !statsEnabled {
		return
	}
	misc.Assert(rStats.StatType == CountType)
	if rStats.writeKey != "" {
		writeKeyClientsMap[rStats.writeKey].Count(rStats.Name, n)
	} else {
		client.Count(rStats.Name, n)
	}
}
func (rStats *RudderStats) Increment() {
	if !statsEnabled {
		return
	}
	misc.Assert(rStats.StatType == CountType)
	client.Increment(rStats.Name)
}

func (rStats *RudderStats) Guage(value interface{}) {
	if !statsEnabled {
		return
	}
	misc.Assert(rStats.StatType == GaugeType)
	client.Gauge(rStats.Name, value)
}

func (rStats *RudderStats) Start() {
	if !statsEnabled {
		return
	}
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing = client.NewTiming()
}

func (rStats *RudderStats) End() {
	if !statsEnabled {
		return
	}
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing.Send(rStats.Name)
}

func (rStats *RudderStats) DeferredTimer() {
	if !statsEnabled {
		return
	}
	client.NewTiming().Send(rStats.Name)
}

type RudderStats struct {
	Name     string
	StatType string
	Timing   statsd.Timing
	writeKey string
}
