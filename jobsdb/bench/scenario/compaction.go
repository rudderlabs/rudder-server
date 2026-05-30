package scenario

import (
	"context"
	"database/sql"
	"fmt"
	mathrand "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// NewCompaction creates a jobsdb bench scenario which compares how the various
// compaction / completed-DS-drop strategies behave under a draining workload.
//
// For every configured flag combination it:
//  1. creates a fresh read-write jobsdb seeded with [datasets] datasets, each
//     holding [jobsPerDataset] jobs (spread evenly across [destinations]),
//  2. spawns one consumer goroutine per destination which repeatedly fetches a
//     batch of pending jobs, marks them as executing and then marks each job as
//     succeeded or failed according to a precomputed pattern,
//  3. measures the wall-clock time it takes for every consumer to drain its
//     destination (i.e. until there are no pending jobs left).
//
// The success/failure pattern is precomputed once and reused verbatim for every
// flag combination so that all scenarios face exactly the same workload.
func NewCompaction(conf *config.Config, stats stats.Stats, log logger.Logger, db *sql.DB) *compaction {
	return &compaction{
		stats: stats,
		log:   log,
		conf:  conf,
		db:    db,
	}
}

type compaction struct {
	stats stats.Stats
	log   logger.Logger
	conf  *config.Config
	db    *sql.DB
}

// flagSet is a single jobsdb compaction configuration under test.
type flagSet struct {
	name                       string
	nonBlockingCompletedDSDrop bool
	nonBlockingCompaction      bool
}

func (p *compaction) Run(ctx context.Context) error {
	var (
		tablePrefix      = "bench"
		customVal        = "benchmark"
		datasets         = p.conf.GetIntVar(10, 1, "JobsDB.Bench.datasets")                 // number of datasets to seed
		jobsPerDataset   = p.conf.GetIntVar(100000, 1, "JobsDB.Bench.jobsPerDataset")       // number of jobs per dataset
		eventPayloadSize = p.conf.GetInt64Var(2*bytesize.KB, 1, "JobsDB.Bench.payloadSize") // size of the event payload
		destinations     = p.conf.GetIntVar(50, 1, "JobsDB.Bench.destinations")             // number of destinations the jobs are spread between
		batchSize        = p.conf.GetIntVar(1000, 1, "JobsDB.Bench.batchSize")              // number of jobs a consumer fetches in one go
		failProbability  = p.conf.GetFloat64Var(0.5, "JobsDB.Bench.failProbability")        // probability a job is marked as failed on a given attempt
		maxFailures      = p.conf.GetIntVar(3, 1, "JobsDB.Bench.maxFailures")               // cap on the number of times a single job may fail before it must succeed
		seedConcurrency  = p.conf.GetIntVar(4, 1, "JobsDB.Bench.seedConcurrency")           // number of concurrent writers used while seeding a dataset
		seedBatchSize    = p.conf.GetIntVar(10000, 1, "JobsDB.Bench.seedBatchSize")         // number of jobs written per Store call while seeding
		payloadLimit     = p.conf.GetInt64Var(100*bytesize.MB, 1, "JobsDB.Bench.payloadLimit")
		maintenanceDSN   = p.conf.GetStringVar("", "JobsDB.Bench.maintenanceDSN")       // if set, jobsdb-internal maintenance ops use a dedicated pool against this DSN
		maintenanceConns = p.conf.GetIntVar(10, 1, "JobsDB.Bench.maintenancePoolConns") // size of the dedicated maintenance pool
	)
	totalJobs := datasets * jobsPerDataset

	// Optional dedicated maintenance connection pool. Isolating jobsdb-internal
	// maintenance ops (compaction setup, post-commit dsList refresh, status-table
	// cleanup/vacuum) from the main pool avoids them contending for the same
	// connections as the seeders/consumers. Opened once and shared across every
	// scenario handle (they run sequentially against the same database).
	var maintenancePool *sql.DB
	if maintenanceDSN != "" {
		var err error
		maintenancePool, err = sql.Open("postgres", maintenanceDSN)
		if err != nil {
			return fmt.Errorf("could not open maintenance pool: %w", err)
		}
		maintenancePool.SetMaxOpenConns(maintenanceConns)
		maintenancePool.SetMaxIdleConns(maintenanceConns)
		defer maintenancePool.Close()
	}

	// Build a single reusable event payload of the requested size.
	eventPayload := []byte("{}")
	for actualSize := len(eventPayload); actualSize < int(eventPayloadSize); actualSize = len(eventPayload) {
		var err error
		eventPayload, err = sjson.SetBytes(eventPayload, "values.-1", rand.String(10))
		if err != nil {
			return fmt.Errorf("could not set payload: %w", err)
		}
	}

	// Precompute, ONCE, the number of times each job (by creation index) should
	// fail before it is allowed to succeed. This pattern is identical for every
	// flag combination, so all scenarios face exactly the same workload.
	rng := mathrand.New(mathrand.NewPCG(0x9E3779B97F4A7C15, 0xBF58476D1CE4E5B9))
	failPattern := make([]uint8, totalJobs)
	for i := range failPattern {
		var fc uint8
		for int(fc) < maxFailures && rng.Float64() < failProbability {
			fc++
		}
		failPattern[i] = fc
	}

	scenarios := []flagSet{
		{name: "baseline (blocking compaction)", nonBlockingCompletedDSDrop: false, nonBlockingCompaction: false},
		{name: "nonBlockingCompletedDSDrop", nonBlockingCompletedDSDrop: true, nonBlockingCompaction: false},
		{name: "nonBlockingCompaction", nonBlockingCompaction: true},
	}

	type result struct {
		name     string
		duration time.Duration
	}
	results := make([]result, 0, len(scenarios))

	for _, s := range scenarios {
		if ctx.Err() != nil {
			return nil
		}
		p.log.Infon("running compaction bench scenario", logger.NewStringField("scenario", s.name))

		// Apply the flag combination under test.
		p.conf.Set("JobsDB."+tablePrefix+".nonBlockingCompletedDSDrop", s.nonBlockingCompletedDSDrop)
		p.conf.Set("JobsDB."+tablePrefix+".nonBlockingCompaction", s.nonBlockingCompaction)

		// Deterministic addNewDS trigger so we can seed exactly [datasets]
		// datasets of [jobsPerDataset] jobs each.
		triggerAddNewDS := make(chan time.Time)
		opts := []jobsdb.OptsFunc{
			jobsdb.WithClearDB(true),
			jobsdb.WithStats(p.stats),
			jobsdb.WithDBHandle(p.db),
			jobsdb.WithConfig(p.conf),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithTriggerAddNewDS(func() <-chan time.Time { return triggerAddNewDS }),
		}
		if maintenancePool != nil {
			opts = append(opts, jobsdb.WithMaintenancePoolDB(maintenancePool))
		}
		db := jobsdb.NewForReadWrite(tablePrefix, opts...)
		if err := db.Start(); err != nil {
			db.Close()
			return fmt.Errorf("could not start bench jobsdb: %w", err)
		}

		// Seed the datasets (not measured).
		seedStart := time.Now()
		if err := p.seed(ctx, db, triggerAddNewDS, customVal, eventPayload, failPattern, datasets, jobsPerDataset, destinations, seedConcurrency, seedBatchSize); err != nil {
			db.Stop()
			db.Close()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("could not seed jobsdb: %w", err)
		}
		p.log.Infon("seeding complete",
			logger.NewStringField("scenario", s.name),
			logger.NewIntField("jobs", int64(totalJobs)),
			logger.NewDurationField("duration", time.Since(seedStart)),
		)

		// Consume (measured).
		duration, err := p.consume(ctx, db, customVal, totalJobs, destinations, batchSize, payloadLimit)
		db.Stop()
		db.Close()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("scenario %q failed: %w", s.name, err)
		}
		results = append(results, result{name: s.name, duration: duration})
		fmt.Printf("\n=== scenario %q drained %d jobs in %s ===\n\n", s.name, totalJobs, duration)
	}

	fmt.Printf("\n================ compaction bench summary (%d jobs) ================\n", totalJobs)
	for _, r := range results {
		fmt.Printf("  %-45s %s\n", r.name, r.duration)
	}
	fmt.Printf("===================================================================\n")
	return nil
}

// seed writes [datasets] datasets of [jobsPerDataset] jobs each, triggering a
// new DS between datasets so that every dataset holds exactly [jobsPerDataset]
// jobs. Job [i] (global creation index) is assigned to destination i%destinations
// and carries its precomputed failure count in its parameters.
func (p *compaction) seed(
	ctx context.Context,
	db *jobsdb.Handle,
	triggerAddNewDS chan time.Time,
	customVal string,
	eventPayload []byte,
	failPattern []uint8,
	datasets, jobsPerDataset, destinations, seedConcurrency, seedBatchSize int,
) error {
	for d := range datasets {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(seedConcurrency)
		for start := 0; start < jobsPerDataset; start += seedBatchSize {
			end := min(start+seedBatchSize, jobsPerDataset)
			g.Go(func() error {
				jobs := make([]*jobsdb.JobT, 0, end-start)
				for pos := start; pos < end; pos++ {
					globalIdx := d*jobsPerDataset + pos
					destID := globalIdx % destinations
					params, err := sjson.SetBytes(
						fmt.Appendf(nil, `{"destination_id":"dest-%d"}`, destID),
						"fc", failPattern[globalIdx],
					)
					if err != nil {
						return fmt.Errorf("could not set fc parameter: %w", err)
					}
					jobs = append(jobs, &jobsdb.JobT{
						UUID:         uuid.New(),
						UserID:       uuid.New().String(),
						CreatedAt:    time.Now().UTC(),
						EventCount:   1,
						WorkspaceId:  "workspace",
						Parameters:   params,
						CustomVal:    customVal,
						EventPayload: eventPayload,
					})
				}
				if err := db.Store(gCtx, jobs); err != nil {
					return fmt.Errorf("could not store seed jobs: %w", err)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		// roll over to a new (empty) dataset, except after the last one so that
		// we end up with exactly [datasets] full datasets.
		if d < datasets-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case triggerAddNewDS <- time.Now():
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case triggerAddNewDS <- time.Now(): // second send waits for the first loop iteration to finish
			}
		}
	}
	return nil
}

// consume spawns one goroutine per destination, each repeatedly fetching a batch
// of pending jobs, marking them executing and then marking each job succeeded or
// failed according to its precomputed failure count. It returns once every
// destination has been fully drained.
func (p *compaction) consume(
	ctx context.Context,
	db *jobsdb.Handle,
	customVal string,
	totalJobs, destinations, batchSize int,
	payloadLimit int64,
) (time.Duration, error) {
	// attempts[jobID] counts how many times a job has been processed so far.
	// Each jobID is owned by exactly one destination => one goroutine, so plain
	// (non-atomic) access would be safe; we keep it simple with a slice.
	attempts := make([]int32, totalJobs+1)

	var (
		succeeded atomic.Int64 // total jobs marked succeeded (terminal)
		failed    atomic.Int64 // total job failures (retries)
		remaining atomic.Int64 // jobs not yet succeeded; the run is over when this hits 0
	)
	remaining.Store(int64(totalJobs))

	// done is closed once every job has reached a terminal succeeded state.
	// We rely on a global completion signal rather than treating an empty
	// GetToProcess result as "drained": with a dsLimit in effect, a destination's
	// pending jobs may live in datasets outside the current query window and only
	// become visible after earlier datasets are compacted/dropped, so an empty
	// result is transient and we must keep polling.
	done := make(chan struct{})

	// emptyPollInterval throttles polling for a destination that currently has no
	// visible pending jobs (drained, or hidden behind the dsLimit window).
	emptyPollInterval := 200 * time.Millisecond

	start := time.Now()

	// progress reporting, stopped explicitly after the consumers finish so it
	// does not deadlock the errgroup (errgroup's context is only cancelled once
	// Wait returns).
	reporterStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reporterStop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("[%s] drained %d/%d (%.1f%%), retries so far: %d\n",
					time.Now().Format("15:04:05"), succeeded.Load(), totalJobs,
					float64(succeeded.Load())/float64(totalJobs)*100, failed.Load())
			}
		}
	}()

	g, gCtx := errgroup.WithContext(ctx)
	for dest := range destinations {
		destID := fmt.Sprintf("dest-%d", dest)
		g.Go(func() error {
			for {
				select {
				case <-done:
					return nil
				case <-gCtx.Done():
					return nil
				default:
				}
				res, err := db.GetToProcess(gCtx, jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal},
					JobsLimit:        batchSize,
					EventsLimit:      batchSize,
					PayloadSizeLimit: payloadLimit,
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destID}},
				}, nil)
				if err != nil {
					if gCtx.Err() != nil {
						return nil // nolint: nilerr
					}
					return fmt.Errorf("could not get jobs for %s: %w", destID, err)
				}
				jobs := res.Jobs
				if len(jobs) == 0 {
					// nothing visible right now: either this destination is fully
					// drained, or its remaining jobs are behind the dsLimit window.
					// Wait a bit and retry until the run is globally complete.
					select {
					case <-done:
						return nil
					case <-gCtx.Done():
						return nil
					case <-time.After(emptyPollInterval):
					}
					continue
				}

				// 1. mark the whole batch as executing
				executing := make([]*jobsdb.JobStatusT, len(jobs))
				for i, job := range jobs {
					executing[i] = newStatus(job, jobsdb.Executing.State, "executing")
				}
				if err := db.UpdateJobStatus(gCtx, executing); err != nil {
					if gCtx.Err() != nil {
						return nil // nolint: nilerr
					}
					return fmt.Errorf("could not mark jobs executing for %s: %w", destID, err)
				}

				// 2. mark each job succeeded or failed per the precomputed pattern
				terminal := make([]*jobsdb.JobStatusT, len(jobs))
				var batchSucceeded, batchFailed int64
				for i, job := range jobs {
					attempt := attempts[job.JobID] + 1
					attempts[job.JobID] = attempt
					fc := int32(gjson.GetBytes(job.Parameters, "fc").Int())
					if attempt <= fc {
						terminal[i] = newStatus(job, jobsdb.Failed.State, "500")
						terminal[i].AttemptNum = int(attempt)
						batchFailed++
					} else {
						terminal[i] = newStatus(job, jobsdb.Succeeded.State, "200")
						terminal[i].AttemptNum = int(attempt)
						batchSucceeded++
					}
				}
				if err := db.UpdateJobStatus(gCtx, terminal); err != nil {
					if gCtx.Err() != nil {
						return nil // nolint: nilerr
					}
					return fmt.Errorf("could not mark jobs terminal for %s: %w", destID, err)
				}
				succeeded.Add(batchSucceeded)
				failed.Add(batchFailed)
				// Each job succeeds exactly once, so remaining decreases
				// monotonically to 0; the goroutine that hits 0 closes done.
				if batchSucceeded > 0 && remaining.Add(-batchSucceeded) == 0 {
					close(done)
					return nil
				}
			}
		})
	}
	err := g.Wait()
	close(reporterStop)
	if err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func newStatus(job *jobsdb.JobT, state, errorCode string) *jobsdb.JobStatusT {
	now := time.Now()
	return &jobsdb.JobStatusT{
		JobID:         job.JobID,
		JobState:      state,
		AttemptNum:    1,
		ExecTime:      now,
		RetryTime:     now,
		ErrorCode:     errorCode,
		ErrorResponse: []byte(`{}`),
		Parameters:    []byte(`{}`),
		JobParameters: job.Parameters,
		WorkspaceId:   job.WorkspaceId,
		PartitionID:   job.PartitionID,
		CustomVal:     job.CustomVal,
	}
}
