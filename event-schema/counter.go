package event_schema

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema/countish"
)

type CounterTypeT string

const (
	LossyCount    CounterTypeT = "LossyCount"
	StickySampler CounterTypeT = "StickySampler"
)

var (
	defaultCounterType                                                          CounterTypeT
	counterSupport, counterErrorTolerance, counterFailureProb, counterThreshold float64
	counterTypeStr                                                              string
)

type FrequencyCounter struct {
	Name        string
	CounterType CounterTypeT
	Counter     countish.LossyCounter
}

func (fc *FrequencyCounter) getCounter() countish.Counter {
	return &fc.Counter
}

func (fc *FrequencyCounter) setCounter(counterType CounterTypeT, counter *countish.LossyCounter) {
	fc.Counter = *counter
	fc.CounterType = counterType
}

func Init() {
	config.RegisterStringConfigVariable("LossyCount", &counterTypeStr, false, "EventSchemas.counterType")
	// Output every elem has appeared at least (N * support) times
	config.RegisterFloat64ConfigVariable(0.01, &counterSupport, false, "EventSchemas.counterSupport")
	// We can start with support/10
	config.RegisterFloat64ConfigVariable(0.001, &counterErrorTolerance, false, "EventSchemas.counterErrorTolerance")
	//
	config.RegisterFloat64ConfigVariable(0.01, &counterFailureProb, false, "EventSchemas.counterFailureProb")

	// Check this?
	config.RegisterFloat64ConfigVariable(0.01, &counterThreshold, false, "EventSchemas.counterThreshold")

	if counterTypeStr == string(StickySampler) {
		defaultCounterType = StickySampler
	} else {
		defaultCounterType = LossyCount
	}
}

func NewFrequencyCounter(name string) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = name
	counter := countish.NewLossyCounter(counterSupport, counterErrorTolerance)
	fc.setCounter(defaultCounterType, counter)
	return &fc
}

func NewPeristedFrequencyCounter(persistedFc *FrequencyCounter) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = persistedFc.Name
	fc.setCounter(LossyCount, &persistedFc.Counter)
	return &fc
}

func (fc *FrequencyCounter) Observe(key *string) {
	fc.getCounter().Observe(*key)
}

func (fc *FrequencyCounter) ItemsAboveThreshold() []countish.Entry {
	return fc.getCounter().ItemsAboveThreshold(counterThreshold)
}
