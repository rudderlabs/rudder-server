package metric

import (
	"sync"
	"testing"
	"time"
)

func TestGaugeAddSub(t *testing.T) {
	gauge := NewGauge()
	gauge.Inc()
	if expected, got := 1.0, gauge.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}
	gauge.Add(42)
	if expected, got := 43.0, gauge.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	gauge.Add(24.42)
	if expected, got := 67.42, gauge.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	gauge.Dec()
	if expected, got := 66.42, gauge.Value(); expected != got {
		t.Errorf("Expected error %f, got %f.", expected, got)
	}

	gauge.Sub(24.42)
	if expected, got := 42.0, gauge.Value(); expected != got {
		t.Errorf("Expected error %f, got %f.", expected, got)
	}
}

func TestGaugeSetGetTime(t *testing.T) {
	gauge := NewGauge().(*gauge)
	now := time.Now()
	f := func() time.Time { return now }
	gauge.now = f

	gauge.SetToCurrentTime()
	if expected, got := now.Round(1*time.Millisecond), gauge.ValueAsTime().Round(1*time.Millisecond); expected != got {
		t.Errorf("Expected error %s, got %s.", expected, got)
	}
}

func TestGaugeAddSubConcurrently(t *testing.T) {
	const concurrency = 1000
	const addAmt = 10
	const subAmt = 2
	gauge := NewGauge()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency/2; i++ {
		go func() {
			gauge.Add(addAmt)
			gauge.Dec() // for every dec we do an equivalent sub below (*)
			wg.Done()
		}()
	}
	for i := 0; i < concurrency/2; i++ {
		go func() {
			gauge.Inc()
			gauge.Sub(subAmt) // (*)
			wg.Done()
		}()
	}
	wg.Wait()
	if expected, got := float64(addAmt*(concurrency/2)-subAmt*(concurrency/2)), gauge.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}
}
