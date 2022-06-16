package metric

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testMeasurement struct {
	name string
	tag  string
}

func (r testMeasurement) GetName() string {
	return r.name
}

func (r testMeasurement) GetTags() map[string]string {
	return map[string]string{"tag": r.tag}
}
func TestRegistryGet(t *testing.T) {
	registry := NewRegistry()

	counterKey := testMeasurement{name: "key1"}
	counter := registry.MustGetCounter(counterKey)
	counter.Inc()
	otherCounter := registry.MustGetCounter(counterKey)
	// otherCounter should be the same counter, since we are using the same key
	if expected, got := counter.Value(), otherCounter.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	gaugeKey := testMeasurement{name: "key2"}
	gauge := registry.MustGetCounter(gaugeKey)
	gauge.Inc()
	otherGauge := registry.MustGetCounter(gaugeKey)
	// otherGauge should be the same gauge, since we are using the same key
	if expected, got := gauge.Value(), otherGauge.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	// trying to get a gauge using the counter's key should result in an error
	if g, err := registry.GetGauge(counterKey); err == nil {
		t.Errorf("Expected an error, got %T", g)
	}

	if expected, got := `a different type of metric exists in the registry with the same key [{name:key1 tag:}]: *metric.counter`, mustGetGauge(registry, counterKey).Error(); expected != got {
		t.Errorf("Expected error %q, got %q.", expected, got)
	}

	smaKey := testMeasurement{name: "key3"}
	sma := registry.MustGetSimpleMovingAvg(smaKey)
	sma.Add(1)
	otherSma := registry.MustGetSimpleMovingAvg(smaKey)
	// otherSma should be the same ma, since we are using the same key
	if expected, got := sma.Value(), otherSma.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	vma10Key := testMeasurement{name: "key4"}
	vma10 := registry.MustGetVarMovingAvg(vma10Key, 10)
	vma10.Add(1)
	otherVma10 := registry.MustGetVarMovingAvg(vma10Key, 10)
	// otherVma10 should be the same vma10, since we are using the same key
	if expected, got := vma10.Value(), otherVma10.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	// trying to get a vma of different age using a key that corresponds to another age should result in an error
	if g, err := registry.GetVarMovingAvg(vma10Key, 20); err == nil {
		t.Errorf("Expected an error, got %T", g)
	}

	if expected, got := `another moving average with age 12.000000 instead of 20.000000 exists in the registry with the same key [{name:key4 tag:}]: *metric.VariableEWMA`, mustGetVMA(registry, vma10Key, 20).Error(); expected != got {
		t.Errorf("Expected error %q, got %q.", expected, got)
	}

}

func TestRegistryNameIndex(t *testing.T) {
	registry := NewRegistry()
	m1 := testMeasurement{name: "key1", tag: "tag1"}
	m2 := testMeasurement{name: "key1", tag: "tag2"}
	m3 := testMeasurement{name: "key2", tag: "tag1"}
	registry.MustGetCounter(m1).Inc()
	registry.MustGetCounter(m2).Add(2)
	registry.MustGetCounter(m3).Add(3)

	metrics := registry.GetMetricsByName("key1")

	require.Equal(t, 2, len(metrics), "should receive 2 metrics")
	res := map[string]float64{}
	res[metrics[0].Tags["tag"]] = metrics[0].Value.(Counter).Value()
	res[metrics[1].Tags["tag"]] = metrics[1].Value.(Counter).Value()

	require.Equal(t, res, map[string]float64{"tag1": 1.0, "tag2": 2.0})
}

func TestRegistryGetConcurrently(t *testing.T) {
	const concurrency = 1000
	registry := NewRegistry()
	var key = testMeasurement{name: "key"}
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			registry.MustGetCounter(key).Inc()
			wg.Done()
		}()
	}
	wg.Wait()
	if expected, got := float64(concurrency), registry.MustGetCounter(key).Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}
}

func mustGetGauge(registry Registry, key Measurement) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	registry.MustGetGauge(key)
	return errors.New("")
}

func mustGetVMA(registry Registry, key Measurement, age float64) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	registry.MustGetVarMovingAvg(key, age)
	return errors.New("")
}

func BenchmarkRegistryGetCounterAndInc1(b *testing.B) {
	benchmarkRegistryGetCounterAndInc(b, 1)
}

func BenchmarkRegistryGetCounterAndInc10(b *testing.B) {
	benchmarkRegistryGetCounterAndInc(b, 10)
}

func BenchmarkRegistryGetCounterAndInc100(b *testing.B) {
	benchmarkRegistryGetCounterAndInc(b, 100)
}

func benchmarkRegistryGetCounterAndInc(b *testing.B, concurrency int) {
	b.StopTimer()
	var start, end sync.WaitGroup
	start.Add(1)
	n := b.N / concurrency
	registry := NewRegistry()
	var key = testMeasurement{name: "key"}
	end.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			start.Wait()
			for i := 0; i < n; i++ {
				counter := registry.MustGetCounter(key)
				counter.Inc()
			}
			end.Done()
		}()
	}

	b.StartTimer()
	start.Done()
	end.Wait()
}

func BenchmarkMutexMapGetIntAndInc1(b *testing.B) {
	benchmarkMutexMapGetIntAndInc(b, 1)
}

func BenchmarkMutexMapGetIntAndInc10(b *testing.B) {
	benchmarkMutexMapGetIntAndInc(b, 10)
}

func BenchmarkMutexMapGetIntAndInc100(b *testing.B) {
	benchmarkMutexMapGetIntAndInc(b, 100)
}

func benchmarkMutexMapGetIntAndInc(b *testing.B, concurrency int) {
	b.StopTimer()
	var mutex sync.RWMutex
	var start, end sync.WaitGroup
	start.Add(1)
	n := b.N / concurrency
	registry := map[string]int{"key": 0}
	end.Add(concurrency)
	for i := 0; i < concurrency; i++ {

		go func() {

			for i := 0; i < n; i++ {
				mutex.Lock()
				registry["key"] += 1
				mutex.Unlock()
			}
			end.Done()
		}()
	}

	b.StartTimer()
	start.Done()
	end.Wait()
}
