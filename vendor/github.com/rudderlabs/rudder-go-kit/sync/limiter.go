package sync

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/queue"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// LimiterPriorityValue defines the priority values supported by Limiter.
// Greater priority value means higher priority
type LimiterPriorityValue int

const (
	_ LimiterPriorityValue = iota
	// LimiterPriorityValueLow Priority....
	LimiterPriorityValueLow
	// LimiterPriorityValueMedium Priority....
	LimiterPriorityValueMedium
	// LimiterPriorityValueMediumHigh Priority....
	LimiterPriorityValueMediumHigh
	// LimiterPriorityValueHigh Priority.....
	LimiterPriorityValueHigh
)

// Limiter limits the number of concurrent operations that can be performed
type Limiter interface {
	// Do executes the function f, but only if there are available slots.
	// Otherwise blocks until a slot becomes available
	Do(key string, f func())

	// DoWithPriority executes the function f, but only if there are available slots.
	// Otherwise blocks until a slot becomes available, respecting the priority
	DoWithPriority(key string, priority LimiterPriorityValue, f func())

	// Begin starts a new operation, blocking until a slot becomes available.
	// Caller is expected to call the returned function to end the operation, otherwise
	// the slot will be reserved indefinitely. End can be called multiple times without any side effects
	Begin(key string) (end func())

	// BeginWithPriority starts a new operation, blocking until a slot becomes available, respecting the priority.
	// Caller is expected to call the returned function to end the operation, otherwise
	// the slot will be reserved indefinitely. End can be called multiple times without any side effects
	BeginWithPriority(key string, priority LimiterPriorityValue) (end func())
}

var WithLimiterStatsTriggerFunc = func(triggerFunc func() <-chan time.Time) func(*limiter) {
	return func(l *limiter) {
		l.stats.triggerFunc = triggerFunc
	}
}

var WithLimiterDynamicPeriod = func(dynamicPeriod time.Duration) func(*limiter) {
	return func(l *limiter) {
		l.dynamicPeriod = dynamicPeriod
	}
}

var WithLimiterTags = func(tags stats.Tags) func(*limiter) {
	return func(l *limiter) {
		l.tags = tags
	}
}

// NewLimiter creates a new limiter
func NewLimiter(ctx context.Context, wg *sync.WaitGroup, name string, limit int, statsf stats.Stats, opts ...func(*limiter)) Limiter {
	if limit <= 0 {
		panic(fmt.Errorf("limit for %q must be greater than 0", name))
	}
	l := &limiter{
		name:     name,
		limit:    limit,
		tags:     stats.Tags{},
		waitList: make(queue.PriorityQueue[chan struct{}], 0),
	}
	heap.Init(&l.waitList)
	l.stats.triggerFunc = func() <-chan time.Time {
		return time.After(15 * time.Second)
	}
	for _, opt := range opts {
		opt(l)
	}

	l.stats.stat = statsf
	l.stats.waitGauge = statsf.NewTaggedStat(name+"_limiter_waiting_routines", stats.GaugeType, l.tags)
	l.stats.activeGauge = statsf.NewTaggedStat(name+"_limiter_active_routines", stats.GaugeType, l.tags)
	l.stats.availabilityGauge = statsf.NewTaggedStat(name+"_limiter_availability", stats.GaugeType, l.tags)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.stats.triggerFunc():
			}
			l.mu.Lock()
			l.stats.activeGauge.Gauge(l.count)
			l.stats.waitGauge.Gauge(len(l.waitList))
			availability := float64(l.limit-l.count) / float64(l.limit)
			l.stats.availabilityGauge.Gauge(availability)
			l.mu.Unlock()
		}
	}()
	return l
}

type limiter struct {
	name          string
	limit         int
	tags          stats.Tags
	dynamicPeriod time.Duration

	mu       sync.Mutex // protects count and waitList below
	count    int
	waitList queue.PriorityQueue[chan struct{}]

	stats struct {
		triggerFunc       func() <-chan time.Time
		stat              stats.Stats
		waitGauge         stats.Measurement // gauge showing number of operations waiting in the queue
		activeGauge       stats.Measurement // gauge showing active number of operations
		availabilityGauge stats.Measurement // gauge showing availability percentage of limiter (0.0 to 1.0)
	}
}

func (l *limiter) Do(key string, f func()) {
	l.DoWithPriority(key, LimiterPriorityValueLow, f)
}

func (l *limiter) DoWithPriority(key string, priority LimiterPriorityValue, f func()) {
	defer l.BeginWithPriority(key, priority)()
	f()
}

func (l *limiter) Begin(key string) (end func()) {
	return l.BeginWithPriority(key, LimiterPriorityValueLow)
}

func (l *limiter) BeginWithPriority(key string, priority LimiterPriorityValue) (end func()) {
	start := time.Now()
	l.wait(priority)
	tags := lo.Assign(l.tags, stats.Tags{"key": key})
	l.stats.stat.NewTaggedStat(l.name+"_limiter_waiting", stats.TimerType, tags).Since(start)
	start = time.Now()
	var endOnce sync.Once

	end = func() {
		endOnce.Do(func() {
			defer l.stats.stat.NewTaggedStat(l.name+"_limiter_working", stats.TimerType, tags).Since(start)
			l.mu.Lock()
			l.count--
			if len(l.waitList) == 0 {
				l.mu.Unlock()
				return
			}
			next := heap.Pop(&l.waitList).(*queue.Item[chan struct{}])
			l.count++
			l.mu.Unlock()
			next.Value <- struct{}{}
			close(next.Value)
		})
	}
	return end
}

// wait until a slot becomes available
func (l *limiter) wait(priority LimiterPriorityValue) {
	l.mu.Lock()
	if l.count < l.limit {
		l.count++
		l.mu.Unlock()
		return
	}
	w := &queue.Item[chan struct{}]{
		Priority: int(priority),
		Value:    make(chan struct{}),
	}
	heap.Push(&l.waitList, w)
	l.mu.Unlock()

	// no dynamic priority
	if l.dynamicPeriod == 0 || priority == LimiterPriorityValueHigh {
		<-w.Value
		return
	}

	// dynamic priority (increment priority every dynamicPeriod)
	ticker := time.NewTicker(l.dynamicPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-w.Value:
			ticker.Stop()
			return
		case <-ticker.C:
			if w.Priority < int(LimiterPriorityValueHigh) {
				l.mu.Lock()
				l.waitList.Update(w, w.Priority+1)
				l.mu.Unlock()
			} else {
				ticker.Stop()
				<-w.Value
				return
			}
		}
	}
}
