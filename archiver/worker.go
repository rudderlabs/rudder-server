package archiver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/payload"
)

type worker struct {
	log              logger.Logger
	partition        string
	archiveFrom      string
	jobsDB           jobsdb.JobsDB
	payloadLimitFunc payload.AdaptiveLimiterFunc
	storageProvider  fileuploader.Provider
	lifecycle        struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	jobFetchLimiter, uploadLimiter, statusUpdateLimiter kitsync.Limiter

	config struct {
		payloadLimit     func() int64
		maxRetryAttempts func() int
		instanceID       string
		eventsLimit      func() int
		minSleep         time.Duration
		maxSleep         time.Duration
	}
	lastUploadTime time.Time
	queryParams    jobsdb.GetQueryParamsT
}

func (w *worker) Work() bool {
start:
	var (
		location     string
		err          error
		jobs         []*jobsdb.JobT
		limitReached bool
	)
	jobs, limitReached, err = w.getJobs()
	if err != nil {
		w.log.Errorf("failed to fetch jobs for backup - partition: %s - %w", w.partition, err)
		return false
	}

	if len(jobs) == 0 {
		return false
	}

	if !(limitReached || time.Since(w.lastUploadTime) > w.config.maxSleep) {
		return false
	}

	var reason string

	storagePrefs, err := w.storageProvider.GetStoragePreferences(jobs[0].WorkspaceId)
	if err != nil {
		w.log.Errorf("failed to fetch storage preferences for workspaceID: %s - %w", jobs[0].WorkspaceId, err)
		reason = fmt.Sprintf(`{"location": "not uploaded because - %v"}`, err)
		if err := w.markStatus(
			jobs,
			func(*jobsdb.JobT) string { return jobsdb.Aborted.State },
			[]byte(reason),
		); err != nil {
			w.log.Errorf("failed to mark unconfigured archive jobs' status - %w", err)
			panic(err)
		}
		goto start
	}
	if !storagePrefs.Backup(w.archiveFrom) {
		reason = fmt.Sprintf(`{"location": "not uploaded because %s-archival disabled for %s"}`, w.archiveFrom, w.partition)
		if err := w.markStatus(
			jobs,
			func(*jobsdb.JobT) string { return jobsdb.Aborted.State },
			[]byte(reason),
		); err != nil {
			w.log.Errorf("failed to mark archive disabled jobs' status - %w", err)
			panic(err)
		}
		goto start
	}

	location, err = w.uploadJobs(w.lifecycle.ctx, jobs)
	w.lastUploadTime = time.Now()
	if err != nil {
		w.log.Errorf("failed to upload jobs - partition: %s - %w", w.partition, err)
		maxAttempts := w.config.maxRetryAttempts()
		if err := w.markStatus(
			jobs,
			func(job *jobsdb.JobT) string {
				if job.LastJobStatus.AttemptNum >= maxAttempts {
					return jobsdb.Aborted.State
				}
				return jobsdb.Failed.State
			},
			[]byte(fmt.Sprintf(`{"location": "not uploaded because - %v"}`, err)),
		); err != nil {
			w.log.Errorf("failed to mark failed jobs' status - %w", err)
			panic(err)
		}
		return false
	}

	if err := w.markStatus(
		jobs,
		func(*jobsdb.JobT) string { return jobsdb.Succeeded.State },
		[]byte(fmt.Sprintf(`{"location": "%v"}`, location)),
	); err != nil {
		w.log.Errorf("failed to mark successful upload status - %w", err)
		panic(err)
	}
	if limitReached {
		goto start
	}
	return true
}

func (w *worker) SleepDurations() (min, max time.Duration) {
	return w.config.minSleep, time.Until(w.lastUploadTime.Add(w.config.maxSleep))
}

func (w *worker) Stop() {
	w.lifecycle.cancel()
}

func (w *worker) uploadJobs(ctx context.Context, jobs []*jobsdb.JobT) (string, error) {
	defer w.uploadLimiter.BeginWithPriority(w.partition, kitsync.LimiterPriorityValue(1))()
	firstJobCreatedAt := jobs[0].CreatedAt
	lastJobCreatedAt := jobs[len(jobs)-1].CreatedAt
	workspaceID := jobs[0].WorkspaceId

	w.log.Debugf("[Archival: storeErrorsToObjectStorage]: Starting logging to object storage - %s", w.partition)

	gzWriter := fileuploader.NewGzMultiFileWriter()
	path := fmt.Sprintf(
		"%v%v.json.gz",
		lo.Must(misc.CreateTMPDIR())+"/rudder-backups/"+w.partition+"/",
		fmt.Sprintf("%v_%v_%v", firstJobCreatedAt.Unix(), lastJobCreatedAt.Unix(), workspaceID),
	)

	for _, job := range jobs {
		j, err := marshalJob(job)
		if err != nil {
			return "", fmt.Errorf("failed to marshal job - %w", err)
		}
		if _, err := gzWriter.Write(path, append(j, '\n')); err != nil {
			return "", fmt.Errorf("failed to write to file - %w", err)
		}
	}
	err := gzWriter.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close file - %w", err)
	}
	defer func() { _ = os.Remove(path) }()

	fileUploader, err := w.storageProvider.GetFileManager(workspaceID)
	if err != nil {
		w.log.Errorf("Skipping Storing errors for workspace: %s - partition: %s since no file manager is found",
			workspaceID, w.partition,
		)
		return "", fmt.Errorf("failed to get file manager - %w", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s - %w", path, err)
	}
	defer func() { _ = file.Close() }()
	prefixes := []string{
		w.partition,
		w.archiveFrom,
		firstJobCreatedAt.Format("2006-01-02"),
		fmt.Sprintf("%d", firstJobCreatedAt.Hour()),
		w.config.instanceID,
	}
	uploadOutput, err := fileUploader.Upload(ctx, file, prefixes...)
	if err != nil {
		w.log.Errorf("failed to upload file to object storage - %w", err)
		return "", fmt.Errorf("failed to upload file to object storage - %w", err)
	}

	return uploadOutput.Location, nil
}

func getStatuses(jobs []*jobsdb.JobT, stateFunc func(*jobsdb.JobT) string, response []byte) []*jobsdb.JobStatusT {
	return lo.Map(jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
		return &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      stateFunc(job),
			ErrorResponse: response,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
		}
	})
}

func (w *worker) getJobs() ([]*jobsdb.JobT, bool, error) {
	defer w.jobFetchLimiter.BeginWithPriority(
		w.partition, kitsync.LimiterPriorityValue(1),
	)()
	params := w.queryParams
	params.PayloadSizeLimit = w.payloadLimitFunc(w.config.payloadLimit())
	params.EventsLimit = w.config.eventsLimit()
	params.JobsLimit = w.config.eventsLimit()
	failed, err := w.jobsDB.GetToRetry(w.lifecycle.ctx, params)
	if err != nil {
		w.log.Errorf("failed to fetch jobs for backup - partition: %s - %w", w.partition, err)
		return nil, false, err
	}
	limitReached := failed.LimitsReached
	jobs := failed.Jobs
	if !limitReached {
		params.EventsLimit -= failed.EventsCount
		params.PayloadSizeLimit -= failed.PayloadSize
		unProcessed, err := w.jobsDB.GetUnprocessed(w.lifecycle.ctx, params)
		if err != nil {
			w.log.Errorf("failed to fetch unprocessed jobs for backup - partition: %s - %w", w.partition, err)
			return nil, false, err
		}
		jobs = append(jobs, unProcessed.Jobs...)
		limitReached = unProcessed.LimitsReached
	}
	return jobs, limitReached, nil
}

func marshalJob(job *jobsdb.JobT) ([]byte, error) {
	var J struct {
		UserID       string          `json:"UserID"`
		EventPayload json.RawMessage `json:"EventPayload"`
		CreatedAt    time.Time       `json:"CreatedAt"`
		MessageID    string          `json:"MessageID"`
	}
	J.UserID = job.UserID
	J.EventPayload = job.EventPayload
	J.CreatedAt = job.CreatedAt
	J.MessageID = gjson.GetBytes(job.EventPayload, "messageId").String()
	return json.Marshal(J)
}

func (w *worker) markStatus(
	jobs []*jobsdb.JobT, stateFunc func(*jobsdb.JobT) string, response []byte,
) error {
	defer w.statusUpdateLimiter.BeginWithPriority(w.partition, kitsync.LimiterPriorityValue(1))()
	return misc.RetryWithNotify(
		w.lifecycle.ctx,
		w.config.maxSleep,
		w.config.maxRetryAttempts(),
		func(ctx context.Context) error {
			return w.jobsDB.UpdateJobStatus(
				ctx,
				getStatuses(jobs, stateFunc, response),
				nil,
				nil,
			)
		},
		func(attempt int) {
			w.log.Warnf(
				"failed to mark %s-%s jobs' status - attempt: %v",
				w.archiveFrom, w.partition, attempt,
			)
		},
	)
}
