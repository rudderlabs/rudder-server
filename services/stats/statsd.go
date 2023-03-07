package stats

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"gopkg.in/alexcesaro/statsd.v2"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// statsdStats is the statsd-specific implementation of Stats
type statsdStats struct {
	config                     statsConfig
	statsdConfig               statsdConfig
	state                      *statsdState
	logger                     logger.Logger
	backgroundCollectionCtx    context.Context
	backgroundCollectionCancel func()
}

func (s *statsdStats) Start(ctx context.Context) (err error) {
	if !s.config.enabled.Load() {
		return nil
	}

	s.state.conn = statsd.Address(s.statsdConfig.statsdServerURL)
	// since, we don't want setup to be a blocking call, creating a separate `go routine` for retry to get statsd client.

	// NOTE: this is to get at least a dummy client, even if there is a failure.
	// So, that nil pointer error is not received when client is called.
	s.state.client.statsd, err = statsd.New(s.state.conn, s.statsdConfig.statsdTagsFormat(), s.statsdConfig.statsdDefaultTags())
	if err == nil {
		s.state.clientsLock.Lock()
		s.state.connEstablished = true
		s.state.clientsLock.Unlock()
	}

	rruntime.Go(func() {
		if err != nil {
			s.state.client.statsd, err = s.getNewStatsdClientWithExpoBackoff(ctx, s.state.conn, s.statsdConfig.statsdTagsFormat(), s.statsdConfig.statsdDefaultTags())
			if err != nil {
				s.config.enabled.Store(false)
				s.logger.Errorf("error while creating new statsd client: %v", err)
			} else {
				s.state.clientsLock.Lock()
				for _, client := range s.state.pendingClients {
					client.statsd = s.state.client.statsd.Clone(s.state.conn, s.statsdConfig.statsdTagsFormat(), s.statsdConfig.statsdDefaultTags(), statsd.Tags(client.tags...), statsd.SampleRate(client.samplingRate))
				}

				s.logger.Info("statsd client setup succeeded.")
				s.state.connEstablished = true
				s.state.pendingClients = nil
				s.state.clientsLock.Unlock()
			}
		}
		if err == nil && ctx.Err() == nil {
			s.collectPeriodicStats()
		}
	})

	s.logger.Infof("Stats started successfully in mode %q with address %q", "StatsD", s.statsdConfig.statsdServerURL)

	return nil
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
			s.logger.Errorf("error while setting statsd client: %v", err)
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
	s.state.rc.PauseDur = time.Duration(s.config.periodicStatsConfig.statsCollectionInterval) * time.Second
	s.state.rc.EnableCPU = s.config.periodicStatsConfig.enableCPUStats
	s.state.rc.EnableMem = s.config.periodicStatsConfig.enableMemStats
	s.state.rc.EnableGC = s.config.periodicStatsConfig.enableGCStats

	s.state.mc = newMetricStatsCollector(s, s.config.periodicStatsConfig.metricManager)
	if s.config.periodicStatsConfig.enabled {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.state.rc.run(s.backgroundCollectionCtx)
		}()
		go func() {
			defer wg.Done()
			s.state.mc.run(s.backgroundCollectionCtx)
		}()
		wg.Wait()
	}
}

// Stop stops periodic collection of stats.
func (s *statsdStats) Stop() {
	s.state.clientsLock.RLock()
	defer s.state.clientsLock.RUnlock()

	if !s.config.enabled.Load() || !s.state.connEstablished {
		return
	}

	s.backgroundCollectionCancel()
	if !s.config.periodicStatsConfig.enabled {
		return
	}

	if s.state.rc.done != nil {
		<-s.state.rc.done
	}
	if s.state.mc.done != nil {
		<-s.state.mc.done
	}
}

// NewStat creates a new Measurement with provided Name and Type
func (s *statsdStats) NewStat(name, statType string) (m Measurement) {
	return s.newStatsdMeasurement(name, statType, s.state.client)
}

func (s *statsdStats) NewTaggedStat(Name, StatType string, tags Tags) (m Measurement) {
	return s.internalNewTaggedStat(Name, StatType, tags, 1)
}

func (s *statsdStats) NewSampledTaggedStat(Name, StatType string, tags Tags) (m Measurement) {
	return s.internalNewTaggedStat(Name, StatType, tags, s.statsdConfig.samplingRate)
}

func (s *statsdStats) internalNewTaggedStat(name, statType string, tags Tags, samplingRate float32) (m Measurement) {
	// If stats is not enabled, returning a dummy struct
	if !s.config.enabled.Load() {
		return s.newStatsdMeasurement(name, statType, &statsdClient{})
	}

	// Clean up tags based on deployment type. No need to send workspace id tag for free tier customers.
	for excludedTag := range s.config.excludedTags {
		delete(tags, excludedTag)
	}
	if tags == nil {
		tags = make(Tags)
	}
	if v, ok := tags[""]; ok {
		s.logger.Warnf("removing empty tag key with value %s for measurement %s", v, name)
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
				taggedClient.statsd = s.state.client.statsd.Clone(s.state.conn, s.statsdConfig.statsdTagsFormat(), s.statsdConfig.statsdDefaultTags(), statsd.Tags(tagVals...), statsd.SampleRate(samplingRate))
			} else {
				// new statsd clients will be created when connection is established for all pending clients
				s.state.pendingClients[taggedClientKey] = taggedClient
			}
			s.state.clients[taggedClientKey] = taggedClient
		}
		s.state.clientsLock.Unlock()
	}

	return s.newStatsdMeasurement(name, statType, taggedClient)
}

// newStatsdMeasurement creates a new measurement of the specific type
func (s *statsdStats) newStatsdMeasurement(name, statType string, client *statsdClient) Measurement {
	if strings.Trim(name, " ") == "" {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		s.logger.Warnf("detected missing stat measurement name, using 'novalue':\n%v", stackTrace)
		name = "novalue"
	}
	baseMeasurement := &statsdMeasurement{
		enabled:            s.config.enabled.Load(),
		name:               name,
		client:             client,
		genericMeasurement: genericMeasurement{statType: statType},
	}
	switch statType {
	case CountType:
		return &statsdCounter{baseMeasurement}
	case GaugeType:
		return &statsdGauge{baseMeasurement}
	case TimerType:
		return &statsdTimer{statsdMeasurement: baseMeasurement}
	case HistogramType:
		return &statsdHistogram{baseMeasurement}
	default:
		panic(fmt.Errorf("unsupported measurement type %s", statType))
	}
}

type statsdConfig struct {
	tagsFormat          string
	statsdServerURL     string
	samplingRate        float32
	instanceName        string
	namespaceIdentifier string
}

// statsdDefaultTags returns the default tags to use for statsd
func (c *statsdConfig) statsdDefaultTags() statsd.Option {
	var tags []string
	if c.instanceName != "" {
		tags = append(tags, "instanceName", c.instanceName)
	}
	if c.namespaceIdentifier != "" {
		tags = append(tags, "namespace", c.namespaceIdentifier)
	}
	return statsd.Tags(tags...)
}

// statsdTagsFormat returns the tags format to use for statsd
func (c *statsdConfig) statsdTagsFormat() statsd.Option {
	switch c.tagsFormat {
	case "datadog":
		return statsd.TagsFormat(statsd.Datadog)
	default:
		return statsd.TagsFormat(statsd.InfluxDB)
	}
}

type statsdState struct {
	conn            statsd.Option
	client          *statsdClient
	connEstablished bool
	rc              runtimeStatsCollector
	mc              metricStatsCollector

	clientsLock    sync.RWMutex
	clients        map[string]*statsdClient
	pendingClients map[string]*statsdClient
}

// statsdClient is a wrapper around statsd.Client.
// We use this wrapper to allow for filling the actual statsd client at a later stage,
// in case a connection cannot be established immediately at startup.
type statsdClient struct {
	samplingRate float32
	tags         []string
	statsd       *statsd.Client
}

// ready returns true if the statsd client is ready to be used (not nil).
func (sc *statsdClient) ready() bool {
	return sc.statsd != nil
}
