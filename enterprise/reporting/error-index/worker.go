package error_index

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type jobPayload struct {
	job     *jobsdb.JobT
	payload *payload
}

type worker struct {
	sourceID    string
	workspaceID string

	log          logger.Logger
	statsFactory stats.Stats

	jobsDB        jobsdb.JobsDB
	configFetcher configFetcher
	uploader      uploader

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	limiterGroup sync.WaitGroup
	limiter      struct {
		fetch  kitsync.Limiter
		upload kitsync.Limiter
		update kitsync.Limiter
	}

	now            func() time.Time
	lastUploadTime time.Time

	config struct {
		parquetParallelWriters, parquetRowGroupSize, parquetPageSize misc.ValueLoader[int64]
		bucketName, instanceID                                       string
		concurrency                                                  misc.ValueLoader[int]
		payloadLimit, eventsLimit                                    misc.ValueLoader[int64]
		minWorkerSleep, uploadFrequency, jobsDBCommandTimeout        time.Duration
		jobsDBMaxRetries                                             misc.ValueLoader[int]
	}
}

// newWorker creates a new worker for the given sourceID.
func newWorker(
	sourceID string,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	jobsDB jobsdb.JobsDB,
	configFetcher configFetcher,
	uploader uploader,
) *worker {
	workspaceID := configFetcher.WorkspaceIDFromSource(sourceID)

	w := &worker{
		sourceID:      sourceID,
		workspaceID:   workspaceID,
		log:           log.Child("worker").With("workspaceID", workspaceID).With("sourceID", sourceID),
		statsFactory:  statsFactory,
		jobsDB:        jobsDB,
		configFetcher: configFetcher,
		uploader:      uploader,
		now:           time.Now,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())

	w.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Reporting.errorIndexReporting.parquetParallelWriters")
	w.config.parquetRowGroupSize = conf.GetReloadableInt64Var(512*bytesize.MB, 1, "Reporting.errorIndexReporting.parquetRowGroupSize")
	w.config.parquetPageSize = conf.GetReloadableInt64Var(8*bytesize.KB, 1, "Reporting.errorIndexReporting.parquetPageSizeInKB")
	w.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	w.config.bucketName = conf.GetString("ErrorIndex.Storage.Bucket", "rudder-failed-messages")
	w.config.concurrency = conf.GetReloadableIntVar(10, 1, "Reporting.errorIndexReporting.concurrency")
	w.config.payloadLimit = conf.GetReloadableInt64Var(1*bytesize.GB, 1, "Reporting.errorIndexReporting.payloadLimit")
	w.config.eventsLimit = conf.GetReloadableInt64Var(100000, 1, "Reporting.errorIndexReporting.eventsLimit")
	w.config.minWorkerSleep = conf.GetDuration("Reporting.errorIndexReporting.minWorkerSleep", 1, time.Minute)
	w.config.uploadFrequency = conf.GetDuration("Reporting.errorIndexReporting.uploadFrequency", 5, time.Minute)
	w.config.jobsDBCommandTimeout = conf.GetDurationVar(10, time.Minute, "JobsDB.CommandRequestTimeout", "Reporting.errorIndexReporting.CommandRequestTimeout")
	w.config.jobsDBMaxRetries = conf.GetReloadableIntVar(3, 1, "JobsDB.MaxRetries", "Reporting.errorIndexReporting.MaxRetries")

	w.limiterGroup = sync.WaitGroup{}
	w.limiter.fetch = kitsync.NewLimiter(
		w.lifecycle.ctx, &w.limiterGroup, "erridx_fetch",
		w.config.concurrency.Load(),
		w.statsFactory,
	)
	w.limiter.upload = kitsync.NewLimiter(
		w.lifecycle.ctx, &w.limiterGroup, "erridx_upload",
		w.config.concurrency.Load(),
		w.statsFactory,
	)
	w.limiter.update = kitsync.NewLimiter(
		w.lifecycle.ctx, &w.limiterGroup, "erridx_update",
		w.config.concurrency.Load(),
		w.statsFactory,
	)
	return w
}

// Work does the following:
// 1. Fetches job results. If no jobs are fetched, returns.
// 2. Checks if job limits are not reached and upload frequency is not met; returns.
// 3. Upload jobs to object storage.
// 4. Updates job status in jobsDB.
func (w *worker) Work() (worked bool) {
	jobResult, err := w.fetchJobs()
	if err != nil && w.lifecycle.ctx.Err() != nil {
		return
	}
	if err != nil {
		panic(fmt.Errorf("failed to fetch jobs for error index: %s", err.Error()))
	}
	if len(jobResult.Jobs) == 0 {
		return
	}
	if !jobResult.LimitsReached && time.Since(w.lastUploadTime) < w.config.uploadFrequency {
		return
	}

	statusList, err := w.uploadJobs(w.lifecycle.ctx, jobResult.Jobs)
	if err != nil {
		w.log.Warnw("failed to upload jobs", "error", err)
		return
	}
	w.lastUploadTime = w.now()

	err = w.markJobsStatus(statusList)
	if err != nil && w.lifecycle.ctx.Err() != nil {
		return
	}
	if err != nil {
		panic(fmt.Errorf("failed to mark jobs: %s", err.Error()))
	}
	worked = true

	tags := stats.Tags{
		"workspaceId": w.workspaceID,
		"sourceId":    w.sourceID,
	}
	w.statsFactory.NewTaggedStat("erridx_uploaded_jobs", stats.CountType, tags).Count(len(jobResult.Jobs))
	return
}

func (w *worker) fetchJobs() (jobsdb.JobsResult, error) {
	defer w.limiter.fetch.Begin(w.sourceID)()

	return w.jobsDB.GetUnprocessed(w.lifecycle.ctx, jobsdb.GetQueryParams{
		ParameterFilters: []jobsdb.ParameterFilterT{
			{Name: "source_id", Value: w.sourceID},
		},
		PayloadSizeLimit: w.config.payloadLimit.Load(),
		EventsLimit:      int(w.config.eventsLimit.Load()),
		JobsLimit:        int(w.config.eventsLimit.Load()),
	})
}

// uploadJobs uploads aggregated job payloads to object storage.
// It aggregates payloads from a list of jobs, applies transformations if needed,
// uploads the payloads, and returns the concatenated locations of the uploaded files.
func (w *worker) uploadJobs(ctx context.Context, jobs []*jobsdb.JobT) ([]*jobsdb.JobStatusT, error) {
	defer w.limiter.upload.Begin(w.sourceID)()

	jobPayloadsMap, err := aggregatedJobPayloads(jobs)
	if err != nil {
		return nil, fmt.Errorf("aggregating job payloads: %w", err)
	}

	if w.configFetcher.IsPIIReportingDisabled(w.workspaceID) {
		for _, jobPayloads := range jobPayloadsMap {
			transformPayloadsForPII(jobPayloads)
		}
	}

	var statusList []*jobsdb.JobStatusT
	for _, jobPayloads := range jobPayloadsMap {
		jobStatusList, err := w.uploadAggregatedJobPayloads(ctx, jobPayloads)
		if err != nil {
			return nil, fmt.Errorf("uploading aggregated payloads: %w", err)
		}
		statusList = append(statusList, jobStatusList...)
	}
	return statusList, nil
}

func aggregatedJobPayloads(jobs []*jobsdb.JobT) (map[string][]jobPayload, error) {
	jobPayloadsMap := make(map[string][]jobPayload)
	for _, job := range jobs {
		var p payload
		if err := json.Unmarshal(job.EventPayload, &p); err != nil {
			return nil, fmt.Errorf("unmarshalling payload: %v", err)
		}

		aggregateKey := p.FailedAt.Format("2006-01-02/15")
		jobPayloadsMap[aggregateKey] = append(jobPayloadsMap[aggregateKey], jobPayload{
			job:     job,
			payload: &p,
		})
	}
	return jobPayloadsMap, nil
}

func transformPayloadsForPII(jobPayloads []jobPayload) {
	lo.ForEach(jobPayloads, func(jp jobPayload, index int) {
		jobPayloads[index].payload.EventName = ""
	})
}

func (w *worker) uploadAggregatedJobPayloads(ctx context.Context, jobPayloads []jobPayload) ([]*jobsdb.JobStatusT, error) {
	minFailedAt := lo.MinBy(jobPayloads, func(a, b jobPayload) bool { return a.payload.FailedAt.Before(b.payload.FailedAt) }).payload.FailedAt
	maxFailedAt := lo.MaxBy(jobPayloads, func(a, b jobPayload) bool { return a.payload.FailedAt.After(b.payload.FailedAt) }).payload.FailedAt

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	filePath := path.Join(tmpDirPath, w.sourceID, fmt.Sprintf("%d_%d_%s.parquet", minFailedAt.Unix(), maxFailedAt.Unix(), w.config.instanceID))

	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating directory: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()

	payloads := lo.Map(jobPayloads, func(item jobPayload, index int) payload {
		return *item.payload
	})
	if err = w.write(f, payloads); err != nil {
		return nil, fmt.Errorf("writing to file: %w", err)
	}
	if err = f.Close(); err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	f, err = os.Open(f.Name())
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	prefixes := []string{w.sourceID, minFailedAt.Format("2006-01-02"), strconv.Itoa(minFailedAt.Hour())}
	output, err := w.uploader.Upload(ctx, f, prefixes...)
	if err != nil {
		return nil, fmt.Errorf("uploading file to object storage: %w", err)
	}

	jobSatusList := lo.Map(jobPayloads, func(item jobPayload, index int) *jobsdb.JobStatusT {
		return &jobsdb.JobStatusT{
			JobID:         item.job.JobID,
			JobState:      jobsdb.Succeeded.State,
			ErrorResponse: []byte(fmt.Sprintf(`{"location": "%s"}`, output.Location)),
			Parameters:    []byte(`{}`),
			AttemptNum:    item.job.LastJobStatus.AttemptNum + 1,
			ExecTime:      w.now(),
			RetryTime:     w.now(),
		}
	})
	return jobSatusList, nil
}

// write writes the payloads to the parquet writer. Sorts the payloads to achieve better encoding.
func (w *worker) write(wr io.Writer, payloads []payload) error {
	pw, err := writer.NewParquetWriterFromWriter(wr, new(payloadParquet), w.config.parquetParallelWriters.Load())
	if err != nil {
		return fmt.Errorf("creating parquet writer: %v", err)
	}

	pw.RowGroupSize = w.config.parquetRowGroupSize.Load() * bytesize.MB
	pw.PageSize = w.config.parquetPageSize.Load() * bytesize.KB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	sortKey := func(p payload) string {
		keys := []string{
			p.DestinationID, p.TransformationID, p.TrackingPlanID,
			p.FailedStage, p.EventType, p.EventName,
		}
		return strings.Join(keys, "::")
	}

	sort.Slice(payloads, func(i, j int) bool {
		return sortKey(payloads[i]) < sortKey(payloads[j])
	})

	for _, p := range payloads {
		if err = pw.Write(p.toParquet()); err != nil {
			return fmt.Errorf("writing to parquet writer: %v", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("stopping parquet writer: %v", err)
	}
	return nil
}

// markJobsStatus marks the status of the jobs in the erridx jobsDB.
func (w *worker) markJobsStatus(statusList []*jobsdb.JobStatusT) error {
	defer w.limiter.update.Begin(w.sourceID)()

	err := misc.RetryWithNotify(
		w.lifecycle.ctx,
		w.config.jobsDBCommandTimeout,
		w.config.jobsDBMaxRetries.Load(),
		func(ctx context.Context) error {
			return w.jobsDB.UpdateJobStatus(ctx, statusList, nil, nil)
		},
		func(attempt int) {
			w.log.Warnw("failed to mark job's status", "attempt", attempt)
		},
	)
	if err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}

	tags := stats.Tags{
		"workspaceId": w.workspaceID,
		"sourceId":    w.sourceID,
		"state":       jobsdb.Succeeded.State,
	}
	w.statsFactory.NewTaggedStat("erridx_processed_jobs", stats.CountType, tags).Count(len(statusList))

	return nil
}

func (w *worker) SleepDurations() (time.Duration, time.Duration) {
	if w.lastUploadTime.IsZero() {
		return w.config.minWorkerSleep, w.config.uploadFrequency
	}
	return w.config.minWorkerSleep, time.Until(w.lastUploadTime.Add(w.config.uploadFrequency))
}

func (w *worker) Stop() {
	w.lifecycle.cancel()
	w.limiterGroup.Wait()
}
