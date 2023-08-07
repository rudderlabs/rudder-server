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

var (
	sourceParam = func(sourceID string) []jobsdb.ParameterFilterT {
		return []jobsdb.ParameterFilterT{{
			Name:  "source_id",
			Value: sourceID,
		}}
	}
)

type worker struct {
	log             logger.Logger
	partition       string
	archiveFrom     string
	jobsDB          jobsdb.JobsDB
	payloadLimiter  payload.AdaptiveLimiterFunc
	storageProvider fileuploader.Provider
	lifecycle       struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	jobFetchLimit, uploadLimit, statusUpdateLimit kitsync.Limiter

	config struct {
		payloadLimit     func() int64
		maxRetryAttempts func() int
		instanceID       string
		eventsLimit      func() int
		minSleep         time.Duration
		maxSleep         time.Duration
	}
	lastUploadTime time.Time
}

func (w *worker) Work() bool {
	defer w.jobFetchLimit.BeginWithPriority(w.partition, kitsync.LimiterPriorityValue(1))()
	var (
		params = jobsdb.GetQueryParamsT{
			PayloadSizeLimit: w.payloadLimiter(w.config.payloadLimit()),
			ParameterFilters: sourceParam(w.partition),
			EventsLimit:      w.config.eventsLimit(),
			JobsLimit:        w.config.eventsLimit(),
		}
		toArchive    = true
		location     string
		err          error
		jobs         []*jobsdb.JobT
		limitReached bool
	)
start:
	jobs, limitReached, err = w.getJobs(params)
	if err != nil {
		w.log.Errorf("failed to fetch jobs for backup - partition: %s - %w", w.partition, err)
		return false
	}

	if len(jobs) == 0 {
		return false
	}

	storagePrefs, err := w.storageProvider.GetStoragePreferences(jobs[0].WorkspaceId)
	var reason string
	if err != nil {
		w.log.Errorf("failed to fetch storage preferences for workspaceID: %s - %w", jobs[0].WorkspaceId, err)
		reason = fmt.Sprintf(`{"location": "not uploaded because - %v"}`, err)
		toArchive = false
	}
	if !storagePrefs.Backup("gw") {
		reason = fmt.Sprintf(`{"location": "not uploaded because archival disabled for %s"}`, w.partition)
		toArchive = false
	}
	var statusList []*jobsdb.JobStatusT
	if !toArchive {
		statusList = getStatuses(
			jobs,
			func(*jobsdb.JobT) string { return jobsdb.Aborted.State },
			[]byte(reason),
		)
		goto markStatus
	}

	location, err = w.uploadJobs(w.lifecycle.ctx, jobs)
	if err != nil {
		w.log.Errorf("failed to upload jobs - partition: %s - %w", w.partition, err)
		maxAttempts := w.config.maxRetryAttempts()
		statusList = getStatuses(
			jobs,
			func(job *jobsdb.JobT) string {
				if job.LastJobStatus.AttemptNum >= maxAttempts {
					return jobsdb.Aborted.State
				}
				return jobsdb.Failed.State
			},
			[]byte(fmt.Sprintf(`{"location": "not uploaded because - %v"}`, err)),
		)
		goto markStatus
	}

	statusList = getStatuses(
		jobs,
		func(*jobsdb.JobT) string { return jobsdb.Succeeded.State },
		[]byte(fmt.Sprintf(`{"location": "%v"}`, location)),
	)

markStatus:
	if err := w.jobsDB.UpdateJobStatus(w.lifecycle.ctx, statusList, nil, nil); err != nil {
		w.log.Errorf("failed to mark jobs' status - %w", err)
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
	firstJobCreatedAt := jobs[0].CreatedAt
	lastJobCreatedAt := jobs[len(jobs)-1].CreatedAt
	workspaceID := jobs[0].WorkspaceId

	w.log.Infof("[Archival: storeErrorsToObjectStorage]: Starting logging to object storage - %s", w.partition)

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
	year, month, date := firstJobCreatedAt.Date()
	prefixes := []string{
		w.partition,
		w.archiveFrom,
		fmt.Sprintf("%d-%d-%d", year, month, date),
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

func (w *worker) getJobs(params jobsdb.GetQueryParamsT) ([]*jobsdb.JobT, bool, error) {
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
