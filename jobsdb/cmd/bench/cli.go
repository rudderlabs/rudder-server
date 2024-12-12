package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/schollz/progressbar/v3"
)

const prefix = "bench"

type Bencher struct {
	log  logger.Logger
	conf *config.Config
}

func (bench *Bencher) RunBaseTest(ctx context.Context, b BaseTest) (err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			bench.log.Error(r)
			err = fmt.Errorf("internal error: %v", r)
		}
	}()
	db, err := misc.NewDatabaseConnectionPool(context.Background(), bench.conf, stats.NOP, prefix)
	if err != nil {
		return fmt.Errorf("connect to db: %w", err)
	}
	if b.NumWriters == 0 {
		b.NumWriters = bench.conf.GetInt("numWriters", 10)
	}
	if b.NumReaders == 0 {
		b.NumReaders = bench.conf.GetInt("numReaders", 10)
	}
	if b.NumJobsPerTopic == 0 {
		b.NumJobsPerTopic = bench.conf.GetInt("numJobsPerTopic", 10000)
	}
	if b.EventSize == 0 {
		b.EventSize = bench.conf.GetInt("eventSize", 1536)
	}
	if b.EventsPickedUpPerQuery == 0 {
		b.EventsPickedUpPerQuery = bench.conf.GetInt("eventsPickedUpPerQuery", 2000)
	}
	bench.log.Infon(
		"base test config",
		logger.NewIntField("numWriters", int64(b.NumWriters)),
		logger.NewIntField("numReaders", int64(b.NumReaders)),
		logger.NewIntField("numJobsPerTopic", int64(b.NumJobsPerTopic)),
		logger.NewIntField("eventSize", int64(b.EventSize)),
		logger.NewIntField("EventsPickedUpPerQuery", int64(b.EventsPickedUpPerQuery)),
		logger.NewIntField("failure%", int64(b.FailurePercent)),
	)

	jd := jobsdb.NewForReadWrite(
		prefix,
		jobsdb.WithClearDB(b.ClearDB),
		jobsdb.WithDBHandle(db),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("jobsDB.skipMaintenanceError", false)),
	)
	defer jd.TearDown()
	if err := jd.Start(); err != nil {
		return fmt.Errorf("start jobsdb: %w", err)
	}
	totalJobs := int(float64(b.NumJobsPerTopic)/float64(b.NumWriters)) * b.NumReaders * b.NumWriters
	readProgressBar := progressbar.Default(int64(totalJobs))
	g, ctx := kitsync.NewEagerGroup(ctx, 2)
	writesDone := make(chan struct{})
	g.Go(func() error {
		defer close(writesDone)
		writeGroup, ctx := kitsync.NewEagerGroup(ctx, b.NumWriters)
		for i := 0; i < b.NumWriters; i++ {
			writeGroup.Go(func() error {
				start := time.Now()
				bench.log.Debugw("started writer", "num", i)
				defer func() {
					bench.log.Debugn("closed writer",
						logger.NewIntField("num", int64(i)),
						logger.NewDurationField("duration", time.Since(start)),
					)
				}()
				for j := 0; j < b.NumReaders; j++ {
					jobs := createJobs(
						"source_"+strconv.Itoa(j),
						int(float64(b.NumJobsPerTopic)/float64(b.NumWriters)),
						b.EventSize,
					)
					if err := jd.Store(ctx, jobs); err != nil {
						return fmt.Errorf("storing jobs: %w", err)
					}
					bench.log.Debugn("jobs stored",
						logger.NewIntField("num", int64(len(jobs))),
						logger.NewStringField("reader", "source_"+strconv.Itoa(j)),
					)

				}
				return nil
			})
		}
		return writeGroup.Wait()
	})
	g.Go(func() error {
		readGroup, ctx := kitsync.NewEagerGroup(ctx, b.NumReaders)
		for i := 0; i < b.NumReaders; i++ {
			readGroup.Go(func() error {
				start := time.Now()
				bench.log.Debugw("started reader", "num", i)
				defer func() {
					bench.log.Debugn(
						"closed reader",
						logger.NewIntField("num", int64(i)),
						logger.NewDurationField("readerDuration", time.Since(start)),
					)
				}()
				sourceID := "source_" + strconv.Itoa(i)
				timer := time.NewTicker(10 * time.Millisecond)
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
					}
					jobResult, err := jd.GetToProcess(
						ctx,
						jobsdb.GetQueryParams{
							CustomValFilters: []string{"GW"},
							ParameterFilters: []jobsdb.ParameterFilterT{
								{Name: "source_id", Value: sourceID},
							},
							JobsLimit:   b.EventsPickedUpPerQuery,
							EventsLimit: b.EventsPickedUpPerQuery,
						},
						nil,
					)
					if err != nil {
						return fmt.Errorf("getJobs - %s: %w", sourceID, err)
					}
					jobs := jobResult.Jobs
					if len(jobs) == 0 {
						select {
						case <-writesDone:
							return nil
						default:
							continue
						}
					}
					// mark jobs as executing
					if err := bench.markExecuting(ctx, jd, jobs); err != nil {
						return fmt.Errorf("mark executing - %s: %w", sourceID, err)
					}

					// perform some work
					// sleep somewhere between 10-100 ms
					randTime := rand.IntN(90) + 10
					time.Sleep(time.Duration(randTime) * time.Millisecond)

					toFail := int(float64(b.FailurePercent*len(jobs)) / 100)
					statusList := make([]*jobsdb.JobStatusT, 0, len(jobs))
					fail := 0
					for j := 0; j < len(jobs); j++ {
						jobState := jobsdb.Succeeded.State
						if j < toFail { // simply fail first `toFail` jobs
							// only if first attempt
							if jobs[j].LastJobStatus.AttemptNum == 0 {
								jobState = jobsdb.Failed.State
								fail++
							}
						}
						statusList = append(statusList, &jobsdb.JobStatusT{
							JobID:         jobs[j].JobID,
							AttemptNum:    jobs[j].LastJobStatus.AttemptNum + 1,
							JobState:      jobState,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "",
							ErrorResponse: []byte(`{}`),
							Parameters:    []byte(`{}`),
							JobParameters: jobs[j].Parameters,
							WorkspaceId:   jobs[j].WorkspaceId,
						})
					}
					if err := jd.UpdateJobStatus(ctx, statusList, []string{"GW"}, []jobsdb.ParameterFilterT{
						{Name: "source_id", Value: sourceID},
					}); err != nil {
						return fmt.Errorf("update status: %w", err)
					}
					readProgressBar.Add(len(jobs) - fail)
				}
			})
		}
		return readGroup.Wait()
	})

	if err := g.Wait(); err != nil {
		return err
	}
	bench.log.Infon(
		"done",
		logger.NewDurationField("duration", time.Since(start)),
	)
	return nil
}

type BaseTest struct {
	ClearDB                bool
	NumWriters             int
	NumReaders             int
	NumJobsPerTopic        int
	EventSize              int
	FailurePercent         int
	EventsPickedUpPerQuery int
}

func createJobs(sourceID string, numEvents, eventSize int) []*jobsdb.JobT {
	jobs := make([]*jobsdb.JobT, 0, numEvents)
	samplePayload := &bytes.Buffer{}
	samplePayload.WriteString(`{"field1": "`)
	samplePayload.WriteString(trand.String(eventSize / 2))
	samplePayload.WriteString(`", "field2": "`)
	samplePayload.WriteString(trand.String(eventSize / 2))
	samplePayload.WriteString(`"}`)
	payload := samplePayload.Bytes()
	for i := 0; i < numEvents; i++ {
		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       trand.String(10),
			CustomVal:    "GW",
			EventCount:   1,
			EventPayload: payload,
			Parameters:   json.RawMessage(parameters(sourceID)),
			WorkspaceId:  sourceID,
		})
	}
	return jobs
}

func parameters(sourceID string) string {
	return fmt.Sprintf(`{"source_id": "%s"}`, sourceID)
}

func (bencher *Bencher) markExecuting(ctx context.Context, jd *jobsdb.Handle, jobs []*jobsdb.JobT) error {
	start := time.Now()
	statusList := make([]*jobsdb.JobStatusT, len(jobs))
	for i, job := range jobs {
		statusList[i] = &jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			JobState:      jobsdb.Executing.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
	}
	if err := jd.UpdateJobStatus(ctx, statusList, []string{"GW"}, nil); err != nil {
		return err
	}
	bencher.log.Debugn(
		"marked executed",
		logger.NewIntField("num", int64(len(statusList))),
		logger.NewDurationField("duration", time.Since(start)),
	)
	return nil
}
