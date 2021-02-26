package event_schema

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema/countish"
)

type CounterTypeT string

const (
	LossyCount    CounterTypeT = "LossyCount"
	StickySampler CounterTypeT = "StickySampler"
)

var defaultCounterType CounterTypeT
var counterSupport, counterErrorTolerance, counterFailureProb, counterThreshold float64

type FrequencyCounter struct {
	Name        string
	CounterType CounterTypeT
	Counter     interface{}
}

func (fc *FrequencyCounter) getCounter() countish.Counter {
	switch fc.CounterType {
	case LossyCount:
		return fc.Counter.(*countish.LossyCounter)
	case StickySampler:
		return fc.Counter.(*countish.StickySampler)
	default:
		panic("Unexpected countertype") //TODO: Handle it in a better way
	}
}

func (fc *FrequencyCounter) setCounter(counterType CounterTypeT, counter countish.Counter) {
	fc.Counter = counter
	fc.CounterType = counterType
}

func init() {
	counterTypeStr := config.GetString("EventSchemas.counterType", "LossyCount")

	// Output every elem has appeared at least (N * support) times
	counterSupport = config.GetFloat64("EventSchemas.counterSupport", 0.01)

	// We can start with support/10
	counterErrorTolerance = config.GetFloat64("EventSchemas.counterErrorTolerance", 0.001)

	//
	counterFailureProb = config.GetFloat64("EventSchemas.counterFailureProb", 0.01)

	// Check this?
	counterThreshold = config.GetFloat64("EventSchemas.counterThreshold", 0.01)

	if counterTypeStr == string(StickySampler) {
		defaultCounterType = StickySampler
	} else {
		defaultCounterType = LossyCount
	}

}

func NewFrequencyCounter(name string) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = name
	var counter countish.Counter
	if defaultCounterType == LossyCount {
		counter = countish.NewLossyCounter(counterSupport, counterErrorTolerance)
	} else {
		counter = countish.NewSampler(counterSupport, counterErrorTolerance, counterFailureProb)
	}
	fc.setCounter(defaultCounterType, counter)
	return &fc
}

func NewPeristedFrequencyCounter(persistedFc *FrequencyCounter) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = persistedFc.Name
	var cType CounterTypeT
	var counter countish.Counter

	if persistedFc.CounterType == LossyCount {
		var lc countish.LossyCounter
		persistedFcJSON, _ := json.Marshal(persistedFc.Counter)
		err := json.Unmarshal(persistedFcJSON, &lc)
		if err != nil {
			panic(err)
		}
		counter = countish.Counter(&lc)
		cType = LossyCount
	} else {
		var ss countish.StickySampler
		persistedFcJSON, _ := json.Marshal(persistedFc.Counter)
		err := json.Unmarshal(persistedFcJSON, &ss)
		if err != nil {
			panic(err)
		}
		counter = countish.Counter(&ss)
		cType = StickySampler
	}

	fc.setCounter(cType, counter)
	return &fc
}

func (fc *FrequencyCounter) Observe(key string) {
	fc.getCounter().Observe(key)
}

// If we add counter support per key, change accordingly
// skipcp: SCC-U1000
func getCounterSupport(key string) float64 {
	return counterSupport
}
func (fc *FrequencyCounter) ItemsAboveThreshold() []countish.Entry {
	return fc.getCounter().ItemsAboveThreshold(counterThreshold)
}
