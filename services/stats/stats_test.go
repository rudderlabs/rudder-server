package stats_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func Test_Measurement_Invalid_Operations(t *testing.T) {
	c := config.New()
	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := stats.NewStats(c, l, m)

	t.Run("counter invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", stats.CountType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.CountType).Observe(1.2)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.CountType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.CountType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.CountType).Since(time.Now())
		})
	})

	t.Run("gauge invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).Observe(1.2)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.GaugeType).Since(time.Now())
		})
	})

	t.Run("histogram invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.HistogramType).Since(time.Now())
		})
	})

	t.Run("timer invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", stats.TimerType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.TimerType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.TimerType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", stats.TimerType).Observe(1.2)
		})
	})
}

func Test_Measurement_Operations(t *testing.T) {
	var lastReceived string
	server := newStatsdServer(t, func(s string) { lastReceived = s })
	defer server.Close()

	c := config.New()
	c.Set("STATSD_SERVER_URL", server.addr)
	c.Set("INSTANCE_ID", "test")
	c.Set("RuntimeStats.enabled", false)
	c.Set("statsSamplingRate", 0.5)

	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := stats.NewStats(c, l, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start stats
	s.Start(ctx)
	defer s.Stop()

	t.Run("counter increment", func(t *testing.T) {
		s.NewStat("test-counter", stats.CountType).Increment()

		require.Eventually(t, func() bool {
			return lastReceived == "test-counter,instanceName=test:1|c"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("counter count", func(t *testing.T) {
		s.NewStat("test-counter", stats.CountType).Count(10)

		require.Eventually(t, func() bool {
			return lastReceived == "test-counter,instanceName=test:10|c"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("gauge", func(t *testing.T) {
		s.NewStat("test-gauge", stats.GaugeType).Gauge(1234)

		require.Eventually(t, func() bool {
			return lastReceived == "test-gauge,instanceName=test:1234|g"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("timer send timing", func(t *testing.T) {
		s.NewStat("test-timer-1", stats.TimerType).SendTiming(10 * time.Second)

		require.Eventually(t, func() bool {
			return lastReceived == "test-timer-1,instanceName=test:10000|ms"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("timer since", func(t *testing.T) {
		s.NewStat("test-timer-2", stats.TimerType).Since(time.Now())

		require.Eventually(t, func() bool {
			return lastReceived == "test-timer-2,instanceName=test:0|ms"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("timer RecordDuration", func(t *testing.T) {
		func() {
			defer s.NewStat("test-timer-4", stats.TimerType).RecordDuration()()
		}()

		require.Eventually(t, func() bool {
			return lastReceived == "test-timer-4,instanceName=test:0|ms"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("histogram", func(t *testing.T) {
		s.NewStat("test-hist-1", stats.HistogramType).Observe(1.2)
		require.Eventually(t, func() bool {
			return lastReceived == "test-hist-1,instanceName=test:1.2|h"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("tagged stats", func(t *testing.T) {
		s.NewTaggedStat("test-tagged", stats.CountType, stats.Tags{"key": "value"}).Increment()
		require.Eventually(t, func() bool {
			return lastReceived == "test-tagged,instanceName=test,key=value:1|c"
		}, 2*time.Second, time.Millisecond)

		// same measurement name, different measurement type
		s.NewTaggedStat("test-tagged", stats.GaugeType, stats.Tags{"key": "value"}).Gauge(22)
		require.Eventually(t, func() bool {
			return lastReceived == "test-tagged,instanceName=test,key=value:22|g"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("sampled stats", func(t *testing.T) {
		lastReceived = ""
		// use the same, non-sampled counter first to make sure we don't get it from cache when we request the sampled one
		counter := s.NewTaggedStat("test-tagged-sampled", stats.CountType, stats.Tags{"key": "value"})
		counter.Increment()

		require.Eventually(t, func() bool {
			return lastReceived == "test-tagged-sampled,instanceName=test,key=value:1|c"
		}, 2*time.Second, time.Millisecond)

		counterSampled := s.NewSampledTaggedStat("test-tagged-sampled", stats.CountType, stats.Tags{"key": "value"})
		counterSampled.Increment()
		require.Eventually(t, func() bool {
			if lastReceived == "test-tagged-sampled,instanceName=test,key=value:1|c|@0.5" {
				return true
			}
			// playing with probabilities, we might or might not get the sample (0.5 -> 50% chance)
			counterSampled.Increment()
			return false
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("measurement with empty name", func(t *testing.T) {
		s.NewStat("", stats.CountType).Increment()

		require.Eventually(t, func() bool {
			return lastReceived == "novalue,instanceName=test:1|c"
		}, 2*time.Second, time.Millisecond)
	})

	t.Run("measurement with empty name and empty tag key", func(t *testing.T) {
		s.NewTaggedStat(" ", stats.GaugeType, stats.Tags{"key": "value", "": "value2"}).Gauge(22)

		require.Eventually(t, func() bool {
			return lastReceived == "novalue,instanceName=test,key=value:22|g"
		}, 2*time.Second, time.Millisecond)
	})
}

func Test_Periodic_stats(t *testing.T) {
	runTest := func(t *testing.T, prepareFunc func(c *config.Config, m metric.Manager), expected []string) {
		var received []string
		server := newStatsdServer(t, func(s string) {
			if i := strings.Index(s, ":"); i > 0 {
				s = s[:i]
			}
			received = append(received, s)
		})
		defer server.Close()

		c := config.New()
		m := metric.NewManager()
		c.Set("STATSD_SERVER_URL", server.addr)
		c.Set("INSTANCE_ID", "test")
		c.Set("RuntimeStats.enabled", true)
		c.Set("RuntimeStats.statsCollectionInterval", 60)
		prepareFunc(c, m)

		l := logger.NewFactory(c)
		s := stats.NewStats(c, l, m)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// start stats
		s.Start(ctx)
		defer s.Stop()

		require.Eventually(t, func() bool {
			if len(received) != len(expected) {
				return false
			}
			return reflect.DeepEqual(received, expected)
		}, 10*time.Second, time.Millisecond)
	}

	t.Run("CPU stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", true)
			c.Set("RuntimeStats.enabledMemStats", false)
			c.Set("RuntimeStats.enableGCStats", false)
		}, []string{
			"runtime_cpu.goroutines,instanceName=test",
			"runtime_cpu.cgo_calls,instanceName=test",
		})
	})

	t.Run("Mem stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", true)
			c.Set("RuntimeStats.enableGCStats", false)
		}, []string{
			"runtime_mem.alloc,instanceName=test",
			"runtime_mem.total,instanceName=test",
			"runtime_mem.sys,instanceName=test",
			"runtime_mem.lookups,instanceName=test",
			"runtime_mem.malloc,instanceName=test",
			"runtime_mem.frees,instanceName=test",
			"runtime_mem.heap.alloc,instanceName=test",
			"runtime_mem.heap.sys,instanceName=test",
			"runtime_mem.heap.idle,instanceName=test",
			"runtime_mem.heap.inuse,instanceName=test",
			"runtime_mem.heap.released,instanceName=test",
			"runtime_mem.heap.objects,instanceName=test",
			"runtime_mem.stack.inuse,instanceName=test",
			"runtime_mem.stack.sys,instanceName=test",
			"runtime_mem.stack.mspan_inuse,instanceName=test",
			"runtime_mem.stack.mspan_sys,instanceName=test",
			"runtime_mem.stack.mcache_inuse,instanceName=test",
			"runtime_mem.stack.mcache_sys,instanceName=test",
			"runtime_mem.othersys,instanceName=test",
		})
	})

	t.Run("MemGC stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", true)
			c.Set("RuntimeStats.enableGCStats", true)
		}, []string{
			"runtime_mem.alloc,instanceName=test",
			"runtime_mem.total,instanceName=test",
			"runtime_mem.sys,instanceName=test",
			"runtime_mem.lookups,instanceName=test",
			"runtime_mem.malloc,instanceName=test",
			"runtime_mem.frees,instanceName=test",
			"runtime_mem.heap.alloc,instanceName=test",
			"runtime_mem.heap.sys,instanceName=test",
			"runtime_mem.heap.idle,instanceName=test",
			"runtime_mem.heap.inuse,instanceName=test",
			"runtime_mem.heap.released,instanceName=test",
			"runtime_mem.heap.objects,instanceName=test",
			"runtime_mem.stack.inuse,instanceName=test",
			"runtime_mem.stack.sys,instanceName=test",
			"runtime_mem.stack.mspan_inuse,instanceName=test",
			"runtime_mem.stack.mspan_sys,instanceName=test",
			"runtime_mem.stack.mcache_inuse,instanceName=test",
			"runtime_mem.stack.mcache_sys,instanceName=test",
			"runtime_mem.othersys,instanceName=test",
			"runtime_mem.gc.sys,instanceName=test",
			"runtime_mem.gc.next,instanceName=test",
			"runtime_mem.gc.last,instanceName=test",
			"runtime_mem.gc.pause_total,instanceName=test",
			"runtime_mem.gc.pause,instanceName=test",
			"runtime_mem.gc.count,instanceName=test",
			"runtime_mem.gc.cpu_percent,instanceName=test",
		})
	})

	t.Run("Pending events", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", false)
			c.Set("RuntimeStats.enableGCStats", false)
			m.GetRegistry(metric.PUBLISHED_METRICS).MustGetGauge(metric.PendingEventsMeasurement("table", "workspace", "destType")).Set(1.0)
		}, []string{
			"jobsdb_table_pending_events_count,instanceName=test,destType=destType,workspace=workspace,workspaceId=workspace",
		})
	})
}

func Test_Tags_Type(t *testing.T) {
	tags := stats.Tags{
		"b": "value1",
		"a": "value2",
	}

	t.Run("strings method", func(t *testing.T) {
		for i := 0; i < 100; i++ { // just making sure we are not just lucky with the order
			require.Equal(t, []string{"a", "value2", "b", "value1"}, tags.Strings())
		}
	})

	t.Run("string method", func(t *testing.T) {
		require.Equal(t, "a,value2,b,value1", tags.String())
	})

	t.Run("special character replacement", func(t *testing.T) {
		specialTags := stats.Tags{
			"b:1": "value1:1",
			"a:1": "value2:2",
		}
		require.Equal(t, []string{"a-1", "value2-2", "b-1", "value1-1"}, specialTags.Strings())
	})

	t.Run("empty tags", func(t *testing.T) {
		emptyTags := stats.Tags{}
		require.Nil(t, emptyTags.Strings())
		require.Equal(t, "", emptyTags.String())
	})
}

func TestExcludedTags(t *testing.T) {
	var lastReceived string
	server := newStatsdServer(t, func(s string) { lastReceived = s })
	defer server.Close()

	c := config.New()
	c.Set("STATSD_SERVER_URL", server.addr)
	c.Set("statsExcludedTags", []string{"workspaceId"})
	c.Set("INSTANCE_ID", "test")
	c.Set("RuntimeStats.enabled", false)

	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := stats.NewStats(c, l, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start stats
	s.Start(ctx)
	defer s.Stop()

	c.Set("statsExcludedTags", []string{"workspaceId"})
	s.NewTaggedStat("test-workspaceId", stats.CountType, stats.Tags{"workspaceId": "value"}).Increment()
	require.Eventually(t, func() bool {
		fmt.Println(lastReceived)
		return lastReceived == "test-workspaceId,instanceName=test:1|c"
	}, 2*time.Second, time.Millisecond)
}

type statsdServer struct {
	t      *testing.T
	addr   string
	closer io.Closer
	closed chan bool
}

func newStatsdServer(t *testing.T, f func(string)) *statsdServer {
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)
	addr := net.JoinHostPort("localhost", strconv.Itoa(port))
	s := &statsdServer{t: t, closed: make(chan bool)}
	laddr, err := net.ResolveUDPAddr("udp", addr)
	require.NoError(t, err)
	conn, err := net.ListenUDP("udp", laddr)
	require.NoError(t, err)
	s.closer = conn
	s.addr = conn.LocalAddr().String()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				s.closed <- true
				return
			}
			s := string(buf[:n])
			lines := strings.Split(s, "\n")
			if n > 0 {
				for _, line := range lines {
					f(line)
				}
			}
		}
	}()

	return s
}

func (s *statsdServer) Close() {
	require.NoError(s.t, s.closer.Close())
	<-s.closed
}
