//go:generate mockgen -destination=../../mocks/services/stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-server/services/stats Stats,Measurement

package stats

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"gopkg.in/alexcesaro/statsd.v2"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	CountType     = "count"
	TimerType     = "timer"
	GaugeType     = "gauge"
	HistogramType = "histogram"
)

func init() {
	Default = NewStats(config.Default, logger.Default, metric.Instance)
}

// Default is the default (singleton) Stats instance
var Default Stats

// Stats manages stat Measurements
type Stats interface {
	// NewStat creates an untagged measurement
	NewStat(name, statType string) (m Measurement)

	// NewTaggedStat creates a tagged measurement with a sampling rate of 1.0
	NewTaggedStat(name, statType string, tags Tags) Measurement

	// NewSampledTaggedStat creates a tagged measurement with the configured sampling rate (statsSamplingRate)
	NewSampledTaggedStat(name, statType string, tags Tags) Measurement

	// Start starts the stats service. It tries to connect to statsd server once and returns immediately.
	// If it fails to connect, it will retry in the background indefinitely, however any stats published in the meantime will be lost.
	Start(ctx context.Context)

	// Stop stops the service and collection of periodic stats.
	Stop()
}

// Tags is a map of key value pairs
type Tags map[string]string

// Strings returns all key value pairs as an ordered list of strings, sorted by increasing key order
func (t Tags) Strings() []string {
	if len(t) == 0 {
		return nil
	}
	res := make([]string, 0, len(t)*2)
	// sorted by tag name (!important for consistent map iteration order)
	tagNames := make([]string, 0, len(t))
	for n := range t {
		tagNames = append(tagNames, n)
	}
	sort.Strings(tagNames)
	for _, tagName := range tagNames {
		tagVal := t[tagName]
		res = append(res, strings.ReplaceAll(tagName, ":", "-"), strings.ReplaceAll(tagVal, ":", "-"))
	}
	return res
}

// String returns all key value pairs as a single string, separated by commas, sorted by increasing key order
func (t Tags) String() string {
	return strings.Join(t.Strings(), ",")
}

// NewStats create a new Stats instance using the provided config, logger factory and metric manager as dependencies
func NewStats(config *config.Config, loggerFactory *logger.Factory, metricManager metric.Manager) Stats {
	s := &statsdStats{
		log: loggerFactory.NewLogger().Child("stats"),
		conf: &statsdConfig{
			enabled:         config.GetBool("enableStats", true),
			tagsFormat:      config.GetString("statsTagsFormat", "influxdb"),
			excludedTags:    config.GetStringSlice("statsExcludedTags", nil),
			statsdServerURL: config.GetString("STATSD_SERVER_URL", "localhost:8125"),
			instanceID:      config.GetString("INSTANCE_ID", ""),
			samplingRate:    float32(config.GetFloat64("statsSamplingRate", 1)),
			periodic: periodicStatsConfig{
				enabled:                 config.GetBool("RuntimeStats.enabled", true),
				statsCollectionInterval: config.GetInt64("RuntimeStats.statsCollectionInterval", 10),
				enableCPUStats:          config.GetBool("RuntimeStats.enableCPUStats", true),
				enableMemStats:          config.GetBool("RuntimeStats.enabledMemStats", true),
				enableGCStats:           config.GetBool("RuntimeStats.enableGCStats", true),
				metricManager:           metricManager,
			},
		},
		state: &statsdState{
			client:         &statsdClient{},
			clients:        make(map[string]*statsdClient),
			pendingClients: make(map[string]*statsdClient),
		},
	}
	return s
}

// statsdStats is the statsd-specific implementation of Stats
type statsdStats struct {
	log   logger.Logger
	conf  *statsdConfig
	state *statsdState
}

func (s *statsdStats) Start(ctx context.Context) {
	if !s.conf.enabled {
		return
	}
	s.state.conn = statsd.Address(s.conf.statsdServerURL)
	// since, we don't want setup to be a blocking call, creating a separate `go routine`` for retry to get statsd client.
	var err error
	// NOTE: this is to get atleast a dummy client, even if there is a failure. So, that nil pointer error is not received when client is called.
	s.state.client.statsd, err = statsd.New(s.state.conn, s.conf.statsdTagsFormat(), s.conf.statsdDefaultTags())
	if err == nil {
		s.state.clientsLock.Lock()
		s.state.connEstablished = true
		s.state.clientsLock.Unlock()
	}
	rruntime.Go(func() {
		if err != nil {
			s.state.client.statsd, err = s.getNewStatsdClientWithExpoBackoff(ctx, s.state.conn, s.conf.statsdTagsFormat(), s.conf.statsdDefaultTags())
			if err != nil {
				s.conf.enabled = false
				s.log.Errorf("error while creating new statsd client: %v", err)
			} else {
				s.state.clientsLock.Lock()
				for _, client := range s.state.pendingClients {
					client.statsd = s.state.client.statsd.Clone(s.state.conn, s.conf.statsdTagsFormat(), s.conf.statsdDefaultTags(), statsd.Tags(client.tags...), statsd.SampleRate(client.samplingRate))
				}

				s.log.Info("statsd client setup succeeded.")
				s.state.connEstablished = true
				s.state.pendingClients = nil
				s.state.clientsLock.Unlock()
			}
		}
		if err == nil && ctx.Err() == nil {
			s.collectPeriodicStats()
		}
	})
}

func (s *statsdStats) getNewStatsdClientWithExpoBackoff(ctx context.Context, opts ...statsd.Option) (*statsd.Client, error) {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = 0
	boCtx := backoff.WithContext(bo, ctx)
	var err error
	var c *statsd.Client
	op := func() error {
		c, err = statsd.New(opts...)
		if err != nil {
			s.log.Errorf("error while setting statsd client: %v", err)
		}
		return err
	}

	err = backoff.Retry(op, boCtx)
	return c, err
}

func (s *statsdStats) collectPeriodicStats() {
	gaugeFunc := func(key string, val uint64) {
		s.state.client.statsd.Gauge("runtime_"+key, val)
	}
	s.state.rc = newRuntimeStatsCollector(gaugeFunc)
	s.state.rc.PauseDur = time.Duration(s.conf.periodic.statsCollectionInterval) * time.Second
	s.state.rc.EnableCPU = s.conf.periodic.enableCPUStats
	s.state.rc.EnableMem = s.conf.periodic.enableMemStats
	s.state.rc.EnableGC = s.conf.periodic.enableGCStats

	s.state.mc = newMetricStatsCollector(s, s.conf.periodic.metricManager)
	if s.conf.periodic.enabled {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.state.rc.run()
		}()
		go func() {
			defer wg.Done()
			s.state.mc.run()
		}()
		wg.Wait()
	}
}

// Stop stops periodic collection of stats.
func (s *statsdStats) Stop() {
	s.state.clientsLock.RLock()
	defer s.state.clientsLock.RUnlock()
	if !s.conf.enabled || !s.state.connEstablished {
		return
	}
	if s.state.rc.done != nil {
		close(s.state.rc.done)
	}
	if s.state.mc.done != nil {
		close(s.state.mc.done)
	}
}

// NewStat creates a new Measurement with provided Name and Type
func (s *statsdStats) NewStat(name, statType string) (m Measurement) {
	return newStatsdMeasurement(s.conf, s.log, name, statType, s.state.client)
}

func (s *statsdStats) NewTaggedStat(Name, StatType string, tags Tags) (m Measurement) {
	return s.internalNewTaggedStat(Name, StatType, tags, 1)
}

func (s *statsdStats) NewSampledTaggedStat(Name, StatType string, tags Tags) (m Measurement) {
	return s.internalNewTaggedStat(Name, StatType, tags, s.conf.samplingRate)
}

func (s *statsdStats) internalNewTaggedStat(name, statType string, tags Tags, samplingRate float32) (m Measurement) {
	// If stats is not enabled, returning a dummy struct
	if !s.conf.enabled {
		return newStatsdMeasurement(s.conf, s.log, name, statType, &statsdClient{})
	}

	// Clean up tags based on deployment type. No need to send workspace id tag for free tier customers.
	for _, excludedTag := range s.conf.excludedTags {
		delete(tags, excludedTag)
	}
	if tags == nil {
		tags = make(Tags)
	}
	if v, ok := tags[""]; ok {
		s.log.Warnf("removing empty tag key with value %s for measurement %s", v, name)
		delete(tags, "")
	}
	// key comprises of the measurement type plus all tag-value pairs
	taggedClientKey := tags.String() + fmt.Sprintf("%f", samplingRate)

	s.state.clientsLock.RLock()
	taggedClient, found := s.state.clients[taggedClientKey]
	s.state.clientsLock.RUnlock()

	if !found {
		s.state.clientsLock.Lock()
		if taggedClient, found = s.state.clients[taggedClientKey]; !found { // double check for race
			tagVals := tags.Strings()
			taggedClient = &statsdClient{samplingRate: samplingRate, tags: tagVals}
			if s.state.connEstablished {
				taggedClient.statsd = s.state.client.statsd.Clone(s.state.conn, s.conf.statsdTagsFormat(), s.conf.statsdDefaultTags(), statsd.Tags(tagVals...), statsd.SampleRate(samplingRate))
			} else {
				// new statsd clients will be created when connection is established for all pending clients
				s.state.pendingClients[taggedClientKey] = taggedClient
			}
			s.state.clients[taggedClientKey] = taggedClient
		}
		s.state.clientsLock.Unlock()
	}

	return newStatsdMeasurement(s.conf, s.log, name, statType, taggedClient)
}
