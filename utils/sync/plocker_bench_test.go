package sync_test

import (
	"strconv"
	gsync "sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/sync"
	"github.com/stretchr/testify/require"
)

// Running tool: go test -benchmem -run=^$ -bench ^BenchmarkPartitionLocker$ github.com/rudderlabs/rudder-server/utils/sync -v -count=1

// goos: darwin
// goarch: arm64
// pkg: github.com/rudderlabs/rudder-server/utils/sync
// BenchmarkPartitionLocker
// BenchmarkPartitionLocker/mutex
// BenchmarkPartitionLocker/mutex-10         	      20	  57351981 ns/op	  162918 B/op	    2121 allocs/op
// BenchmarkPartitionLocker/cond
// BenchmarkPartitionLocker/cond-10          	      20	  56779356 ns/op	  149106 B/op	    2030 allocs/op
// PASS
// ok  	github.com/rudderlabs/rudder-server/utils/sync	3.080s
func BenchmarkPartitionLocker(b *testing.B) {
	const numKeys = 20
	const numGoroutines = 1000

	run := func(b *testing.B, l locker) {
		for i := 0; i < b.N; i++ {
			var countersMu gsync.Mutex
			counters := map[string]int{}
			var wg gsync.WaitGroup
			for j := 0; j < numGoroutines; j++ {
				key := strconv.Itoa(j % numKeys)
				wg.Add(1)
				go func() {
					defer wg.Done()
					l.Lock(key)
					countersMu.Lock()
					counters[key]++
					countersMu.Unlock()
					time.Sleep(time.Millisecond)
					l.Unlock(key)
				}()
			}
			wg.Wait()
			for _, counter := range counters {
				require.Equal(b, counter, numGoroutines/numKeys)
			}
		}
	}

	b.Run("mutex", func(b *testing.B) {
		run(b, &mutexLocker{s: make(map[string]*muState)})
	})

	b.Run("cond", func(b *testing.B) {
		run(b, sync.NewPartitionLocker())
	})
}

type locker interface {
	Lock(key string)
	Unlock(key string)
}

type muState struct {
	mu  gsync.Mutex
	cnt int
}
type mutexLocker struct {
	mu gsync.Mutex
	s  map[string]*muState
}

func (l *mutexLocker) Lock(key string) {
	l.mu.Lock()
	mu := l.mutex(key)
	mu.cnt++
	l.mu.Unlock()
	mu.mu.Lock()
}

func (l *mutexLocker) Unlock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	mu := l.mutex(key)
	mu.mu.Unlock()
	mu.cnt--
	if mu.cnt == 0 {
		delete(l.s, key)
	}
}

func (l *mutexLocker) mutex(key string) *muState {
	mu, ok := l.s[key]
	if !ok {
		mu = &muState{}
		l.s[key] = mu
	}
	return mu
}
