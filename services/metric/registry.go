package metric

import (
	"fmt"
	"sync"
)

type Tags map[string]string
type TagsWithValue struct {
	Tags  Tags
	Value interface{}
}

// Measurement acts as a key in a Registry.
// Measurements should be backed by comparable struct
type Measurement interface {
	// GetKey gets the key of the measurement
	GetKey() interface{}
	// GetName gets the name of the measurement
	GetName() string
	// GetTags gets the tags of the measurement
	GetTags() map[string]string
}

// Registry is a safe way to capture metrics in a highly concurrent environment.
// The registry is responsible for creating and storing the various measurements and
// guarantees consistency when competing goroutines try to update the same measurement
// at the same time.
//
// E.g.
// assuming that you already have created a new registry
//    registry :=  NewRegistry()
//
// the following is guaranteed to be executed atomically:
//   registry.MustGetCounter("key").Inc()
type Registry interface {

	// GetCounter gets a counter by key. If a value for this key
	// already exists but corresponds to another measurement type,
	// e.g. a Gauge, an error is returned
	GetCounter(Measurement) (Counter, error)

	// MustGetCounter gets a counter by key. If a value for this key
	// already exists but corresponds to another measurement type,
	// e.g. a Gauge, it panics
	MustGetCounter(Measurement) Counter

	// GetGauge gets a gauge by key. If a value for this key
	// already exists but corresponds to another measurement
	// type, e.g. a Counter, an error is returned
	GetGauge(Measurement) (Gauge, error)

	// MustGetGauge gets a gauge by key. If a value for this key
	// already exists but corresponds to another measurement type,
	// e.g. a Counter, it panics
	MustGetGauge(Measurement) Gauge

	// GetMovingAverage gets a moving average by key. If a value for this key
	// already exists but corresponds to another measurement
	// type, e.g. a Counter, an error is returned
	GetSimpleMovingAvg(Measurement) (MovingAverage, error)

	// MustGetMovingAverage gets a moving average by key. If a value for this key
	// already exists but corresponds to another measurement type,
	// e.g. a Counter, it panics
	MustGetSimpleMovingAvg(Measurement) MovingAverage

	// GetMovingAverage gets a moving average by key. If a value for this key
	// already exists but corresponds to another measurement
	// type, e.g. a Counter, an error is returned
	GetVarMovingAvg(m Measurement, age float64) (MovingAverage, error)

	// MustGetMovingAverage gets a moving average by key. If a value for this key
	// already exists but corresponds to another measurement type,
	// e.g. a Counter, it panics
	MustGetVarMovingAvg(m Measurement, age float64) MovingAverage

	// Range scans across all metrics
	Range(f func(key, value interface{}) bool)

	// GetMetricsByName gets all metrics with this name
	GetMetricsByName(name string) []TagsWithValue
}

func NewRegistry() Registry {
	counterGenerator := func() interface{} {
		return NewCounter()
	}
	gaugeGenerator := func() interface{} {
		return NewGauge()
	}
	varEwmaGenerator := func() interface{} {
		return &VariableEWMA{}
	}
	simpleEwmaGenerator := func() interface{} {
		return &SimpleEWMA{}
	}
	indexGenerator := func() interface{} {
		return map[interface{}]TagsWithValue{}
	}
	return &registry{
		counters:    sync.Pool{New: counterGenerator},
		gauges:      sync.Pool{New: gaugeGenerator},
		simpleEwmas: sync.Pool{New: simpleEwmaGenerator},
		varEwmas:    sync.Pool{New: varEwmaGenerator},
		sets:        sync.Pool{New: indexGenerator},
	}
}

type registry struct {
	store       sync.Map
	nameIndex   sync.Map
	counters    sync.Pool
	gauges      sync.Pool
	simpleEwmas sync.Pool
	varEwmas    sync.Pool
	sets        sync.Pool
}

func (r *registry) GetCounter(m Measurement) (Counter, error) {
	newCounter := r.counters.Get()
	res, putBack := r.store.LoadOrStore(m.GetKey(), newCounter)
	if putBack {
		r.counters.Put(newCounter)
	} else {
		r.updateIndex(m, res)
	}
	c, ok := res.(Counter)
	if !ok {
		return nil, fmt.Errorf("a different type of metric exists in the registry with the same key [%+v]: %T", m.GetKey(), res)
	}
	return c, nil
}

func (r *registry) MustGetCounter(m Measurement) Counter {
	c, err := r.GetCounter(m)
	if err != nil {
		panic(err)
	}
	return c
}

func (r *registry) GetGauge(m Measurement) (Gauge, error) {
	newGauge := r.gauges.Get()
	res, putBack := r.store.LoadOrStore(m.GetKey(), newGauge)
	if putBack {
		r.gauges.Put(newGauge)
	} else {
		r.updateIndex(m, res)
	}
	g, ok := res.(Gauge)
	if !ok {
		return nil, fmt.Errorf("a different type of metric exists in the registry with the same key [%+v]: %T", m.GetKey(), res)
	}
	return g, nil
}

func (r *registry) MustGetGauge(m Measurement) Gauge {
	c, err := r.GetGauge(m)
	if err != nil {
		panic(err)
	}
	return c
}

func (r *registry) GetSimpleMovingAvg(m Measurement) (MovingAverage, error) {
	newEwma := r.simpleEwmas.Get()
	res, putBack := r.store.LoadOrStore(m.GetKey(), newEwma)
	if putBack {
		r.simpleEwmas.Put(newEwma)
	} else {
		r.updateIndex(m, res)
	}
	ma, ok := res.(MovingAverage)
	if !ok {
		return nil, fmt.Errorf("a different type of metric exists in the registry with the same key [%+v]: %T", m.GetKey(), res)
	}
	return ma, nil
}

func (r *registry) MustGetSimpleMovingAvg(m Measurement) MovingAverage {
	ma, err := r.GetSimpleMovingAvg(m)
	if err != nil {
		panic(err)
	}
	return ma
}

func (r *registry) GetVarMovingAvg(m Measurement, age float64) (MovingAverage, error) {
	newEwma := r.varEwmas.Get()
	decay := 2 / (age + 1)
	newEwma.(*VariableEWMA).decay = decay
	res, putBack := r.store.LoadOrStore(m.GetKey(), newEwma)
	if putBack {
		r.varEwmas.Put(newEwma)
	} else {
		r.updateIndex(m, res)
	}
	ma, ok := res.(*VariableEWMA)
	if !ok {
		return nil, fmt.Errorf("a different type of metric exists in the registry with the same key [%+v]: %T", m.GetKey(), res)
	}
	if ma.decay != decay {
		currentAge := 2/ma.decay + 1
		return nil, fmt.Errorf("another moving average with age %f instead of %f exists in the registry with the same key [%+v]: %T", currentAge, age, m.GetKey(), res)
	}
	return ma, nil
}

func (r *registry) MustGetVarMovingAvg(m Measurement, age float64) MovingAverage {
	ma, err := r.GetVarMovingAvg(m, age)
	if err != nil {
		panic(err)
	}
	return ma
}

func (r *registry) Range(f func(key interface{}, value interface{}) bool) {
	r.store.Range(f)
}

func (r *registry) GetMetricsByName(name string) []TagsWithValue {
	metricsSet, ok := r.nameIndex.Load(name)
	if !ok {
		return []TagsWithValue{}
	}
	values := []TagsWithValue{}
	for _, value := range metricsSet.(map[interface{}]TagsWithValue) {
		values = append(values, value)
	}
	return values
}

func (r *registry) updateIndex(m Measurement, metric interface{}) {
	name := m.GetName()
	newSet := r.sets.Get()
	res, putBack := r.nameIndex.LoadOrStore(name, newSet)
	if putBack {
		r.sets.Put(newSet)
	}

	res.(map[interface{}]TagsWithValue)[m.GetKey()] = TagsWithValue{m.GetTags(), metric}
}
