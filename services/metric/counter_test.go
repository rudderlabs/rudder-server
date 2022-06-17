package metric

import (
	"fmt"
	"sync"
	"testing"
)

func TestCounterAdd(t *testing.T) {
	counter := NewCounter()
	counter.Inc()
	if expected, got := 1.0, counter.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}
	counter.Add(42)
	if expected, got := 43.0, counter.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	counter.Add(24.42)
	if expected, got := 67.42, counter.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}

	if expected, got := "counter cannot decrease in value", decreaseCounter(counter).Error(); expected != got {
		t.Errorf("Expected error %q, got %q.", expected, got)
	}
}

func TestCounterAddConcurrently(t *testing.T) {
	const concurrency = 1000
	counter := NewCounter()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			counter.Inc()
			wg.Done()
		}()
	}
	wg.Wait()
	if expected, got := float64(concurrency), counter.Value(); expected != got {
		t.Errorf("Expected %f, got %f.", expected, got)
	}
}

func decreaseCounter(c Counter) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	c.Add(-1)
	return nil
}
