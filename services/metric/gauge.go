package metric

import (
	"math"
	"sync/atomic"
	"time"
)

// Gauge keeps track of increasing/decreasing or generally arbitrary values.
// You can even use a gauge to keep track of time, e.g. when was the last time
// an event happened!
type Gauge interface {
	// Set sets the given value to the gauge.
	Set(float64)
	// Inc increments the gauge by 1. Use Add to increment it by arbitrary
	// values.
	Inc()
	// Dec decrements the gauge by 1. Use Sub to decrement it by arbitrary
	// values.
	Dec()
	// Add adds the given value to the counter.
	Add(val float64)
	// Sub subtracts the given value from the counter.
	Sub(float64)
	// SetToCurrentTime sets the current UNIX time as the gauge's value
	SetToCurrentTime()
	// Value gets the current value of the counter.
	Value() float64
	// IntValue gets the current value of the counter as an int.
	IntValue() int
	// ValueAsTime gets the current value of the counter as time.
	ValueAsTime() time.Time
}

func NewGauge() Gauge {
	result := &gauge{now: time.Now}
	return result
}

type gauge struct {
	valBits uint64

	now func() time.Time // To mock out time.Now() for testing.
}

func (g *gauge) Set(val float64) {
	atomic.StoreUint64(&g.valBits, math.Float64bits(val))
}

func (g *gauge) SetToCurrentTime() {
	g.Set(float64(g.now().UnixNano()) / 1e9)
}

func (g *gauge) Inc() {
	g.Add(1)
}

func (g *gauge) Dec() {
	g.Add(-1)
}

func (g *gauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + val)
		if atomic.CompareAndSwapUint64(&g.valBits, oldBits, newBits) {
			return
		}
	}
}

func (g *gauge) Sub(val float64) {
	g.Add(val * -1)
}

func (g *gauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.valBits))
}

func (g *gauge) IntValue() int {
	return int(g.Value())
}

func (g *gauge) ValueAsTime() time.Time {
	return time.Unix(0, int64(g.Value()*1e9))
}
