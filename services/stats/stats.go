package stats

import (
	"log"

	"github.com/rudderlabs/rudder-server/misc"
	"gopkg.in/alexcesaro/statsd.v2"
)

const (
	CountType = "count"
	TimerType = "timer"
	GaugeType = "gauge"
)

var client *statsd.Client

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
	client.Count(rStats.Name, 1)
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

func Setup() {
	client, err := statsd.New() // Connect to the UDP port 8125 by default.
	if err != nil {
		// If nothing is listening on the target port, an error is returned and
		// the returned client does nothing but is still usable. So we can
		// just log the error and go on.
		log.Print(err)
	}
	defer client.Close()

}

type RudderStats struct {
	Name     string
	StatType string
	Timing   statsd.Timing
}
