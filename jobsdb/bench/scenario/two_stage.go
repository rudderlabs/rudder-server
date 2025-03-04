package scenario

import (
	"context"
	"database/sql"
	"fmt"
	mathrand "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// NewTwoStage creates a jobsdb bench scenario which emulates a two-stage pipeline:
// 1. It creates 2 new jobsdb instances, benchone and benchtwo.
// 2. It spawns w*s writer go-routines on benchone, where [w] is the number of write concurrency and [s] is the number of sources.
// 3. It spawns s reader go-routines from benchone and u*s status updating go-routines, where [s] is the number of sources and [u] is the number of update concurrency.
// 4. benchone readers read from benchone, forward to benchtow and mark jobs as succeeded.
// 5. It spawns s reader go-routines from benchtwo and u*s status updating go-routines, where [s] is the number of sources and [u] is the number of update concurrency.
// 4. benchtwo readers read from benchtwo and mark [100-f] jobs as succeeded and [f] jobs as failed, where [f] is the failure percentage.
func NewTwoStage(conf *config.Config, stats stats.Stats, log logger.Logger, db *sql.DB) *twoStage {
	return &twoStage{
		stats: stats,
		log:   log,
		conf:  conf,
		db:    db,
	}
}

type twoStage struct {
	stats stats.Stats
	log   logger.Logger
	conf  *config.Config
	db    *sql.DB
}

func (p *twoStage) Run(ctx context.Context) error {
	eventPayloadSize := p.conf.GetInt64Var(1*bytesize.KB, 1, "JobsDB.Bench.payloadSize")  // size of the event payload
	noOfSources := p.conf.GetIntVar(10, 1, "JobsDB.Bench.noOfSources")                    // number of sources
	writerConcurrency := p.conf.GetIntVar(2, 1, "JobsDB.Bench.writerConcurrency")         // number of jobs writers go-routines for each source
	updateConcurrency := p.conf.GetIntVar(2, 1, "JobsDB.Bench.updateConcurrency")         // number of jobs status updater go-routines for each reader
	writerBatchSize := p.conf.GetIntVar(1000, 1, "JobsDB.Bench.writerBatchSize")          // number of jobs writers write in one go
	readerReadSize := p.conf.GetIntVar(10000, 1, "JobsDB.Bench.readerReadSize")           // number of jobs readers read in one go
	payloadLimit := p.conf.GetInt64Var(100*bytesize.MB, 1, "JobsDB.Bench.payloadLimit")   // if 0, no limit will be applied on the size of the payload queried
	dsLimit := p.conf.GetReloadableIntVar(0, 1, "JobsDB.Bench.dsLimit", "JobsDB.dsLimit") // if 0, no limit will be applied on the number of data sets queried
	failurePercentage := p.conf.GetIntVar(1, 1, "JobsDB.Bench.failurePercentage")         // percentage of jobs that will fail, i.e. be marked as failed
	backlog := p.conf.GetIntVar(0, 1, "JobsDB.Bench.backlog")                             // if 0, no backlog will be accumulated before processing starts

	backlogCh := make(chan struct{}) // channel used to signal that backlog has been reached
	if backlog == 0 {
		close(backlogCh)
	}

	insertLimiter, err := rateLimiter(p.conf.GetIntVar(0, 1, "JobsDB.Bench.insertRateLimit"))
	if err != nil {
		return fmt.Errorf("could not create insert rate limiter: %w", err)
	}

	writerDB1 := jobsdb.NewForWrite(
		"benchone",
		jobsdb.WithClearDB(true),
		jobsdb.WithStats(p.stats),
		jobsdb.WithDBHandle(p.db),
		jobsdb.WithConfig(p.conf),
		jobsdb.WithSkipMaintenanceErr(true),
	)
	defer writerDB1.Close()
	if err := writerDB1.Start(); err != nil {
		return fmt.Errorf("could not start benchone writer jobsdb: %w", err)
	}
	defer writerDB1.Stop()

	readerDB1 := jobsdb.NewForRead(
		"benchone",
		jobsdb.WithConfig(p.conf),
		jobsdb.WithDSLimit(dsLimit),
		jobsdb.WithStats(p.stats),
		jobsdb.WithDBHandle(p.db),
		jobsdb.WithSkipMaintenanceErr(true),
	)
	if err := readerDB1.Start(); err != nil {
		return fmt.Errorf("could not start benchone writer jobsdb: %w", err)
	}
	defer readerDB1.Close()

	db2 := jobsdb.NewForReadWrite(
		"benchtwo",
		jobsdb.WithClearDB(true),
		jobsdb.WithStats(p.stats),
		jobsdb.WithDBHandle(p.db),
		jobsdb.WithConfig(p.conf),
		jobsdb.WithDSLimit(dsLimit),
		jobsdb.WithSkipMaintenanceErr(true),
	)
	defer db2.Close()
	if err := db2.Start(); err != nil {
		return fmt.Errorf("could not start benchtwo jobsdb: %w", err)
	}
	defer db2.Stop()

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
		total     atomic.Int64 // total number of jobs sent to benchone
		processed atomic.Int64 // total number of jobs processed by benchtwo successfully

		succeeded atomic.Int64 // number of jobs marked as succeeded in the last second
		failed    atomic.Int64 // number of jobs marked as failed in the last second

		write  atomic.Int64 // number of jobs written to benchone in the last second
		read   atomic.Int64 // number of jobs read from benchone in the last second
		update atomic.Int64 // number of jobs updated in benchone in the last second
	)
	for i := range noOfSources {
		sourceID := fmt.Sprintf("source-%d", i)

		for j := 0; j < writerConcurrency; j++ {
			g.Go(func() error { // write jobs in benchone
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
						if err := insertLimiter(ctx, "benchone", len(jobs)); err != nil {
							return fmt.Errorf("could not check insert rate limit: %w", err)
						}
						if err := writerDB1.Store(ctx, jobs); err != nil {
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

		// read and move jobs from benchone to benchtwo
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
					jobs, err := readerDB1.GetUnprocessed(ctx, jobsdb.GetQueryParams{
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

					g, ctx := errgroup.WithContext(ctx)
					noOfChunks := len(jobs.Jobs) / updateConcurrency
					if noOfChunks == 0 {
						noOfChunks = 1
					}
					chunks := lo.Chunk(jobs.Jobs, noOfChunks)
					for _, chunk := range chunks {
						chunk := chunk
						g.Go(func() error {
							// store jobs in benchtwo
							if err := db2.Store(ctx, chunk); err != nil {
								if ctx.Err() != nil {
									return nil // nolint: nilerr
								}
								return fmt.Errorf("could not write jobs: %w", err)
							}

							// mark benchone jobs as complete
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
							if err := readerDB1.UpdateJobStatus(ctx, statusList, []string{customVal}, nil); err != nil {
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

		// read and process jobs from benchtwo
		g.Go(func() error { // we can only have one reader per source
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
					jobs, err := db2.GetToProcess(ctx, jobsdb.GetQueryParams{
						CustomValFilters: []string{customVal},
						JobsLimit:        readerReadSize,
						EventsLimit:      readerReadSize,
						PayloadSizeLimit: payloadLimit,
						ParameterFilters: []jobsdb.ParameterFilterT{{Name: "source_id", Value: sourceID}},
					}, nil)
					if err != nil {
						if ctx.Err() != nil {
							return nil // nolint: nilerr
						}
						return fmt.Errorf("could not get jobs: %w", err)
					}

					// mark benchtwo jobs as complete/aborted/failed
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
							var failedCount int
							var succeededCount int
							for _, job := range chunk {
								status := jobsdb.Succeeded.State
								errorCode := "200"
								// respect failure percentage
								if mathrand.Float64()*100 < float64(failurePercentage) {
									status = jobsdb.Failed.State
									errorCode = "500"
									failedCount++
								} else {
									succeededCount++
								}
								statusList = append(statusList, &jobsdb.JobStatusT{
									JobID:         job.JobID,
									JobState:      status,
									AttemptNum:    1,
									ExecTime:      time.Now(),
									RetryTime:     time.Now(),
									ErrorCode:     errorCode,
									ErrorResponse: []byte(`{"success":"OK"}`),
									Parameters:    []byte(`{}`),
									JobParameters: job.Parameters,
									WorkspaceId:   job.WorkspaceId,
								})
							}
							if err := db2.UpdateJobStatus(ctx, statusList, []string{customVal}, nil); err != nil {
								if ctx.Err() != nil {
									return nil // nolint: nilerr
								}
								return fmt.Errorf("could not write jobs: %w", err)
							}
							succeeded.Add(int64(succeededCount))
							failed.Add(int64(failedCount))
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

	// print stats every second
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
				succeededS := succeeded.Swap(0)
				failedS := failed.Swap(0)
				total.Add(written)
				if backlog > 0 && !backlogReached && total.Load() > int64(backlog) {
					backlogReached = true
					close(backlogCh)
				}
				processed.Add(succeededS)
				percentage := float64(processed.Load()) / float64(total.Load()) * 100
				writeRate := float64(written) / dur.Seconds()
				readRate := float64(read.Swap(0)) / dur.Seconds()
				updateRate := float64(updated) / dur.Seconds()
				failedRate := float64(failedS) / dur.Seconds()
				totalRate := float64(total.Load()) / time.Since(start).Seconds()

				fmt.Printf("[%[1]s] Processed %[2]d/%[3]d events in %[4]s (%.2[5]f%%). total mean rate: %.2[6]f, write: %.2[7]f events/second, read: %.2[8]f events/second, update: %.2[9]f events/second, failed: %.2[10]f events/second\n",
					currentTime.Format("15:04:05"),
					processed.Load(),
					total.Load(),
					time.Since(start),
					percentage,
					totalRate,
					writeRate,
					readRate,
					updateRate,
					failedRate,
				)
			}
		}
	})
	return g.Wait()
}
