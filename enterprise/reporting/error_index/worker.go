package error_index

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

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

type worker struct {
	sourceID    string
	workspaceID string

	log          logger.Logger
	statsFactory stats.Stats

	jobsDB           jobsdb.JobsDB
	configSubscriber configSubscriber
	uploader         uploader

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	limiter struct {
		fetch  kitsync.Limiter
		upload kitsync.Limiter
		update kitsync.Limiter
	}

	now            func() time.Time
	lastUploadTime time.Time

	config struct {
		parquetParallelWriters, parquetRowGroupSize, parquetPageSize config.ValueLoader[int64]
		bucketName, instanceID                                       string
		payloadLimit, eventsLimit                                    config.ValueLoader[int64]
		minWorkerSleep, uploadFrequency, jobsDBCommandTimeout        time.Duration
		jobsDBMaxRetries                                             config.ValueLoader[int]
	}
}

// newWorker creates a new worker for the given sourceID.
func newWorker(
	sourceID string,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	jobsDB jobsdb.JobsDB,
	configFetcher configSubscriber,
	uploader uploader,
	fetchLimiter, uploadLimiter, updateLimiter kitsync.Limiter,
) *worker {
	workspaceID := configFetcher.WorkspaceIDFromSource(sourceID)

	w := &worker{
		sourceID:         sourceID,
		workspaceID:      workspaceID,
		log:              log.Child("worker").With("workspaceID", workspaceID).With("sourceID", sourceID),
		statsFactory:     statsFactory,
		jobsDB:           jobsDB,
		configSubscriber: configFetcher,
		uploader:         uploader,
		now:              time.Now,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())

	w.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Reporting.errorIndexReporting.parquetParallelWriters")
	w.config.parquetRowGroupSize = conf.GetReloadableInt64Var(512*bytesize.MB, 1, "Reporting.errorIndexReporting.parquetRowGroupSize")
	w.config.parquetPageSize = conf.GetReloadableInt64Var(8*bytesize.KB, 1, "Reporting.errorIndexReporting.parquetPageSize")
	w.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	w.config.bucketName = conf.GetString("ErrorIndex.Storage.Bucket", "rudder-failed-messages")
	w.config.payloadLimit = conf.GetReloadableInt64Var(1*bytesize.GB, 1, "Reporting.errorIndexReporting.payloadLimit")
	w.config.eventsLimit = conf.GetReloadableInt64Var(100000, 1, "Reporting.errorIndexReporting.eventsLimit")
	w.config.minWorkerSleep = conf.GetDuration("Reporting.errorIndexReporting.minWorkerSleep", 1, time.Minute)
	w.config.uploadFrequency = conf.GetDuration("Reporting.errorIndexReporting.uploadFrequency", 5, time.Minute)
	w.config.jobsDBCommandTimeout = conf.GetDurationVar(10, time.Minute, "JobsDB.CommandRequestTimeout", "Reporting.errorIndexReporting.CommandRequestTimeout")
	w.config.jobsDBMaxRetries = conf.GetReloadableIntVar(3, 1, "JobsDB.MaxRetries", "Reporting.errorIndexReporting.MaxRetries")

	w.limiter.fetch = fetchLimiter
	w.limiter.upload = uploadLimiter
	w.limiter.update = updateLimiter
	return w
}

// Work fetches and processes job results:
// 1. Fetches job results.
// 2. If no jobs are fetched, returns.
// 3. If job limits are not reached and upload frequency is not met, returns.
// 4. Uploads jobs to object storage.
// 5. Updates job status in the jobsDB.
func (w *worker) Work() bool {
	for {
		jobResult, err := w.fetchJobs()
		if err != nil && w.lifecycle.ctx.Err() != nil {
			return false
		}
		if err != nil {
			panic(fmt.Errorf("failed to fetch jobs for error index: %s", err.Error()))
		}
		if len(jobResult.Jobs) == 0 {
			return false
		}
		if !jobResult.LimitsReached && time.Since(w.lastUploadTime) < w.config.uploadFrequency {
			return false
		}

		statusList, err := w.uploadJobs(w.lifecycle.ctx, jobResult.Jobs)
		if err != nil {
			w.log.Warnw("failed to upload jobs", "error", err)
			return false
		}
		w.lastUploadTime = w.now()

		err = w.markJobsStatus(statusList)
		if err != nil && w.lifecycle.ctx.Err() != nil {
			return false
		}
		if err != nil {
			panic(fmt.Errorf("failed to mark jobs: %s", err.Error()))
		}

		tags := stats.Tags{
			"workspaceId": w.workspaceID,
			"sourceId":    w.sourceID,
		}
		w.statsFactory.NewTaggedStat("erridx_uploaded_jobs", stats.CountType, tags).Count(len(jobResult.Jobs))

		if !jobResult.LimitsReached {
			return true
		}
	}
}

func (w *worker) fetchJobs() (jobsdb.JobsResult, error) {
	defer w.limiter.fetch.Begin("")()

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
	defer w.limiter.upload.Begin("")()

	jobWithPayloadsMap := make(map[string][]jobWithPayload)
	for _, job := range jobs {
		var p payload
		if err := json.Unmarshal(job.EventPayload, &p); err != nil {
			return nil, fmt.Errorf("unmarshalling payload: %w", err)
		}

		key := p.FailedAtTime().Format("2006-01-02/15")
		jobWithPayloadsMap[key] = append(jobWithPayloadsMap[key], jobWithPayload{JobT: job, payload: p})
	}

	statusList := make([]*jobsdb.JobStatusT, 0, len(jobs))
	for _, jobWithPayloads := range jobWithPayloadsMap {
		uploadFile, err := w.uploadPayloads(ctx, lo.Map(jobWithPayloads, func(item jobWithPayload, index int) payload {
			return item.payload
		}))
		if err != nil {
			return nil, fmt.Errorf("uploading aggregated payloads: %w", err)
		}
		w.log.Debugn("successfully uploaded aggregated payloads", logger.NewStringField("location", uploadFile.Location))

		statusList = append(statusList, lo.Map(jobWithPayloads, func(item jobWithPayload, index int) *jobsdb.JobStatusT {
			return &jobsdb.JobStatusT{
				JobID:         item.JobT.JobID,
				JobState:      jobsdb.Succeeded.State,
				ErrorResponse: []byte(fmt.Sprintf(`{"location": "%s"}`, uploadFile.Location)),
				Parameters:    []byte(`{}`),
				AttemptNum:    item.JobT.LastJobStatus.AttemptNum + 1,
				ExecTime:      w.now(),
				RetryTime:     w.now(),
			}
		})...)
	}
	return statusList, nil
}

func (w *worker) uploadPayloads(ctx context.Context, payloads []payload) (*filemanager.UploadedFile, error) {
	slices.SortFunc(payloads, func(i, j payload) int {
		return i.FailedAtTime().Compare(j.FailedAtTime())
	})

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	dir, err := os.MkdirTemp(tmpDirPath, "*")
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	minFailedAt := payloads[0].FailedAtTime()
	maxFailedAt := payloads[len(payloads)-1].FailedAtTime()

	filePath := path.Join(dir, fmt.Sprintf("%d_%d_%s_%s.parquet", minFailedAt.Unix(), maxFailedAt.Unix(), w.config.instanceID, uuid.NewString()))

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()

	if err = w.encodeToParquet(f, payloads); err != nil {
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
	uploadOutput, err := w.uploader.Upload(ctx, f, prefixes...)
	if err != nil {
		return nil, fmt.Errorf("uploading file to object storage: %w", err)
	}
	return &uploadOutput, nil
}

// encodeToParquet writes the payloads to the writer using parquet encoding. It sorts the payloads to achieve better encoding.
func (w *worker) encodeToParquet(wr io.Writer, payloads []payload) error {
	pw, err := writer.NewParquetWriterFromWriter(wr, new(payload), w.config.parquetParallelWriters.Load())
	if err != nil {
		return fmt.Errorf("creating parquet writer: %v", err)
	}

	pw.RowGroupSize = w.config.parquetRowGroupSize.Load()
	pw.PageSize = w.config.parquetPageSize.Load()
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	sort.Slice(payloads, func(i, j int) bool {
		return payloads[i].SortingKey() < payloads[j].SortingKey()
	})

	for _, payload := range payloads {
		if err = pw.Write(payload); err != nil {
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
	defer w.limiter.update.Begin("")()

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
}
