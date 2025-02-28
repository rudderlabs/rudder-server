package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/memstore"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	mathrand "math/rand/v2"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// NewSimple creates a jobsdb bench scenario which emulates basic behaviour:
// 1. It creates a new jobsdb instance.
// 2. It spawns w*s writer go-routines, where [w] is the number of write concurrency and [s] is the number of sources.
// 3. It spawns s reader go-routines and u*s status updating go-routines, where [s] is the number of sources and [u] is the number of update concurrency.
// 4. Readers read from jobsdb and mark jobs as succeeded.
func NewSimple(conf *config.Config, stats stats.Stats, log logger.Logger, db *sql.DB) *simple {
	return &simple{
		stats: stats,
		log:   log,
		conf:  conf,
		db:    db,
	}
}

type simple struct {
	stats stats.Stats
	log   logger.Logger
	conf  *config.Config
	db    *sql.DB
}

func (p *simple) Run(ctx context.Context) error {
	eventPayloadSize := p.conf.GetInt64Var(1*bytesize.KB, 1, "JobsDB.Bench.payloadSize")
	noOfSources := p.conf.GetIntVar(10, 1, "JobsDB.Bench.noOfSources")
	writerConcurrency := p.conf.GetIntVar(2, 1, "JobsDB.Bench.writerConcurrency")
	updateConcurrency := p.conf.GetIntVar(2, 1, "JobsDB.Bench.updateConcurrency")
	writerBatchSize := p.conf.GetIntVar(1000, 1, "JobsDB.Bench.writerBatchSize")
	readerReadSize := p.conf.GetIntVar(10000, 1, "JobsDB.Bench.readerReadSize")
	payloadLimit := p.conf.GetInt64Var(100*bytesize.MB, 1, "JobsDB.Bench.payloadLimit")
	dsLimit := p.conf.GetReloadableIntVar(0, 1, "JobsDB.Bench.dsLimit", "JobsDB.dsLimit")
	backlog := p.conf.GetIntVar(0, 1, "JobsDB.Bench.backlog")

	backlogCh := make(chan struct{})
	if backlog == 0 {
		close(backlogCh)
	}

	rateLimiter := func(limit int) (func(cost int) error, error) {
		var mu sync.Mutex
		if limit == 0 {
			return func(int) error { return nil }, nil
		}
		store, _ := memstore.NewCtx(1)
		rateLimiter, err := throttled.NewGCRARateLimiterCtx(store, throttled.RateQuota{
			MaxRate:  throttled.PerSec(limit),
			MaxBurst: limit,
		})
		if err != nil {
			return nil, fmt.Errorf("could not create rate limiter: %w", err)
		}
		return func(cost int) error {
			mu.Lock()
			defer mu.Unlock()
			var i int
			const quantity = 100
			for i < cost/quantity {
				throttled, info, err := rateLimiter.RateLimitCtx(ctx, "key", quantity)
				if err != nil {
					return err
				}
				if throttled {
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(info.RetryAfter):
					}
				} else {
					i++
				}
			}
			return nil
		}, nil
	}

	insertLimiter, err := rateLimiter(p.conf.GetIntVar(0, 1, "JobsDB.Bench.insertRateLimit"))
	if err != nil {
		return fmt.Errorf("could not create insert rate limiter: %w", err)
	}
	updateLimiter, err := rateLimiter(p.conf.GetIntVar(0, 1, "JobsDB.Bench.updateRateLimit"))
	if err != nil {
		return fmt.Errorf("could not create update rate limiter: %w", err)
	}
	getLimiter, err := rateLimiter(p.conf.GetIntVar(0, 1, "JobsDB.Bench.getRateLimit"))
	if err != nil {
		return fmt.Errorf("could not create get rate limiter: %w", err)
	}

	writerDB := jobsdb.NewForWrite(
		"bench",
		jobsdb.WithClearDB(true),
		jobsdb.WithStats(p.stats),
		jobsdb.WithDBHandle(p.db),
		jobsdb.WithConfig(p.conf),
		jobsdb.WithSkipMaintenanceErr(true),
	)
	defer writerDB.Close()
	if err := writerDB.Start(); err != nil {
		return fmt.Errorf("could not start bench writer jobsdb: %w", err)
	}
	defer writerDB.Stop()

	readerDB := jobsdb.NewForRead(
		"bench",
		jobsdb.WithConfig(p.conf),
		jobsdb.WithDSLimit(dsLimit),
		jobsdb.WithStats(p.stats),
		jobsdb.WithDBHandle(p.db),
		jobsdb.WithDSLimit(dsLimit),
		jobsdb.WithSkipMaintenanceErr(true),
	)
	defer readerDB.Close()
	if err := readerDB.Start(); err != nil {
		return fmt.Errorf("could not start bench writer jobsdb: %w", err)
	}
	defer readerDB.Stop()
	g, ctx := errgroup.WithContext(ctx)

	customVal := "benchmark"
	eventPayload := []byte("{}")
	for actualSize := len(eventPayload); actualSize < int(eventPayloadSize); actualSize = len(eventPayload) {
		var err error
		eventPayload, err = sjson.SetBytes(eventPayload, "values.-1", rand.String(10))
		if err != nil {
			return fmt.Errorf("could not set payload: %w", err)
		}
	}

	var (
		total     atomic.Int64
		processed atomic.Int64

		write  atomic.Int64
		read   atomic.Int64
		update atomic.Int64
	)
	for i := 0; i < noOfSources; i++ {
		sourceID := fmt.Sprintf("source-%d", i)

		for range writerConcurrency {
			g.Go(func() error {
				select { // don't start all writers at the same time
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(mathrand.IntN(5)) * time.Second):
				}
				for {
					select {
					case <-ctx.Done():
						return nil
					default:
						var jobs []*jobsdb.JobT
						for range writerBatchSize {
							jobs = append(jobs, &jobsdb.JobT{
								UUID:        uuid.New(),
								UserID:      uuid.New().String(),
								CreatedAt:   time.Now().UTC(),
								EventCount:  1,
								WorkspaceId: "workspace",
								Parameters: []byte(fmt.Sprintf(`{
									"source_id": %q
									}`, sourceID)),
								CustomVal:    customVal,
								EventPayload: eventPayload,
							})
						}
						if err := insertLimiter(len(jobs)); err != nil {
							return fmt.Errorf("could not check insert rate limit: %w", err)
						}
						if err := writerDB.Store(ctx, jobs); err != nil {
							if ctx.Err() != nil {
								return nil // nolint: nilerr
							}
							return fmt.Errorf("could not write jobs: %w", err)
						}
						write.Add(int64(len(jobs)))
					}
				}
			})
		}
		g.Go(func() error { // we can only have one reader per source
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
					// wait for backlog to be reached
					select {
					case <-ctx.Done():
						return nil
					case <-backlogCh:
					}
					jobs, err := readerDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
						CustomValFilters: []string{customVal},
						JobsLimit:        readerReadSize,
						EventsLimit:      readerReadSize,
						PayloadSizeLimit: payloadLimit,
						ParameterFilters: []jobsdb.ParameterFilterT{{Name: "source_id", Value: sourceID}},
					})
					if err != nil {
						if ctx.Err() != nil {
							return nil // nolint: nilerr
						}
						return fmt.Errorf("could not get jobs: %w", err)
					}
					read.Add(int64(len(jobs.Jobs)))
					if err := getLimiter(len(jobs.Jobs)); err != nil {
						return fmt.Errorf("could not check get rate limit: %w", err)
					}
					g, ctx := errgroup.WithContext(ctx)
					noOfChunks := len(jobs.Jobs) / updateConcurrency
					if noOfChunks == 0 {
						noOfChunks = 1
					}
					chunks := lo.Chunk(jobs.Jobs, noOfChunks)
					for _, chunk := range chunks {
						chunk := chunk
						g.Go(func() error {
							var statusList []*jobsdb.JobStatusT
							for _, job := range chunk {
								statusList = append(statusList, &jobsdb.JobStatusT{
									JobID:         job.JobID,
									JobState:      jobsdb.Succeeded.State,
									AttemptNum:    1,
									ExecTime:      time.Now(),
									RetryTime:     time.Now(),
									ErrorCode:     "200",
									ErrorResponse: []byte(`{"success":"OK"}`),
									Parameters:    []byte(`{}`),
									JobParameters: job.Parameters,
									WorkspaceId:   job.WorkspaceId,
								})
							}
							if err := updateLimiter(len(statusList)); err != nil {
								return fmt.Errorf("could not check update rate limit: %w", err)
							}
							if err := readerDB.UpdateJobStatus(ctx, statusList, []string{customVal}, nil); err != nil {
								if ctx.Err() != nil {
									return nil // nolint: nilerr
								}
								return fmt.Errorf("could not write jobs: %w", err)
							}
							update.Add(int64(len(statusList)))
							return nil
						})
					}
					if err := g.Wait(); err != nil {
						return err
					}
				}
			}
		})
	}

	g.Go(func() error {
		var backlogReached bool
		start := time.Now()
		previousTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
				currentTime := time.Now()
				dur := currentTime.Sub(previousTime)
				previousTime = currentTime

				updated := update.Swap(0)
				written := write.Swap(0)
				total.Add(written)
				if backlog > 0 && !backlogReached && total.Load() > int64(backlog) {
					backlogReached = true
					close(backlogCh)
				}
				processed.Add(updated)
				percentage := float64(processed.Load()) / float64(total.Load()) * 100
				writeRate := float64(written) / dur.Seconds()
				readRate := float64(read.Swap(0)) / dur.Seconds()
				updateRate := float64(updated) / dur.Seconds()
				totalRate := float64(total.Load()) / time.Since(start).Seconds()

				fmt.Printf("[%[1]s] Processed %[2]d/%[3]d events in %[4]s (%.2[5]f%%). total mean rate: %.2[6]f, write: %.2[7]f events/second, read: %.2[8]f events/second, update: %.2[9]f events/second\n",
					currentTime.Format("15:04:05"),
					processed.Load(),
					total.Load(),
					time.Since(start),
					percentage,
					totalRate,
					writeRate,
					readRate,
					updateRate)
			}
		}
	})
	return g.Wait()
}
