package flusher

import (
	"context"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

type Runner interface {
	Run()
	Stop()
}

type NOPCronRunner struct{}

func (c *NOPCronRunner) Run()  {}
func (c *NOPCronRunner) Stop() {}

type CronRunner struct {
	ctx    context.Context
	cancel context.CancelFunc
	g      *errgroup.Group

	stats stats.Stats
	log   logger.Logger

	instanceId    string
	table         string
	module        string
	flusher       *Flusher
	sleepInterval config.ValueLoader[time.Duration]

	flushTimer   stats.Measurement
	reportingLag stats.Measurement

	started atomic.Bool
}

func NewCronRunner(ctx context.Context, log logger.Logger, stats stats.Stats, conf *config.Config, flusher *Flusher, table, module string) *CronRunner {
	sleepInterval := conf.GetReloadableDurationVar(5, time.Second, "Reporting.flusher.sleepInterval")
	instanceId := conf.GetString("INSTANCE_ID", "1")

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	c := &CronRunner{
		ctx:           ctx,
		cancel:        cancel,
		g:             g,
		stats:         stats,
		log:           log,
		instanceId:    instanceId,
		flusher:       flusher,
		sleepInterval: sleepInterval,
		table:         table,
		module:        module,
	}
	c.initStats()

	return c
}

func (c *CronRunner) initStats() {
	commonTags := stats.Tags{
		"instance": c.instanceId,
		"table":    c.table,
		"module":   c.module,
	}
	c.flushTimer = c.stats.NewTaggedStat("reporting_flusher_flush_duration_seconds", stats.TimerType, commonTags)
	c.reportingLag = c.stats.NewTaggedStat("reporting_flusher_lag_seconds", stats.GaugeType, commonTags)
}

func (c *CronRunner) Run() {
	c.g.Go(func() error {
		return c.startFlushing(c.ctx)
	})

	c.started.Store(true)

	if err := c.g.Wait(); err != nil {
		c.log.Errorn("error in cron-runner", obskit.Error(err))
	}
}

func (c *CronRunner) startFlushing(ctx context.Context) error {
	ticker := time.NewTicker(c.sleepInterval.Load())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			s := time.Now()
			if err := c.flusher.Flush(ctx); err != nil {
				c.log.Errorn("error in Flush", obskit.Error(err))
			}
			c.flushTimer.Since(s)

			if !c.flusher.ShouldFlushAggressively(ctx) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	}
}

func (c *CronRunner) Stop() {
	c.cancel()
	err := c.g.Wait()
	if err != nil {
		c.log.Errorn("error in stopping cron-runner", obskit.Error(err))
	}

	err = c.flusher.CleanUp()
	if err != nil {
		c.log.Errorn("error in flusher cleanup", obskit.Error(err))
	}
	c.started.Store(false)
}
