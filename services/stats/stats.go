package stats

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/misc"
	"gopkg.in/alexcesaro/statsd.v2"
)

const (
	CountType = "count"
	TimerType = "timer"
	GaugeType = "gauge"
)

var client *statsd.Client

func init() {
	var err error
	client, err = statsd.New()
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
	}
}

func (rStats *RudderStats) Count(n int) {
	misc.Assert(rStats.StatType == CountType)
	client.Count(rStats.Name, n)
}
func (rStats *RudderStats) Increment() {
	misc.Assert(rStats.StatType == CountType)
	client.Increment(rStats.Name)
}

func (rStats *RudderStats) Guage(value interface{}) {
	misc.Assert(rStats.StatType == GaugeType)
	client.Gauge(rStats.Name, value)
}

func (rStats *RudderStats) Start() {
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing = client.NewTiming()
}

func (rStats *RudderStats) End() {
	misc.Assert(rStats.StatType == TimerType)
	rStats.Timing.Send(rStats.Name)
}

func (rStats *RudderStats) DeferredTimer() {
	client.NewTiming().Send(rStats.Name)
}

type RudderStats struct {
	Name     string
	StatType string
	Timing   statsd.Timing
}
