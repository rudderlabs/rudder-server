package error_index

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"

	limiter "github.com/rudderlabs/rudder-server/utils/payload"
)

type uploader interface {
	Upload(context.Context, *os.File, ...string) (filemanager.UploadedFile, error)
}

type worker struct {
	log              logger.Logger
	sourceID         string
	workspaceID      string
	archiveFrom      string
	jobsDB           jobsdb.JobsDB
	payloadLimitFunc limiter.AdaptiveLimiterFunc
	storageProvider  fileuploader.Provider
	configFetcher    configFetcher
	stats            stats.Stats
	writer           Writer
	uploader         uploader
	lifecycle        struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	fetchLimiter, uploadLimiter, updateLimiter kitsync.Limiter

	config struct {
		payloadLimit     func() int64
		jobsdbMaxRetries func() int
		instanceID       string
		eventsLimit      func() int
		minSleep         time.Duration
		uploadFrequency  time.Duration
	}
	lastUploadTime time.Time
	queryParams    jobsdb.GetQueryParams
}

func (w *worker) Work() bool {
start:
	jobs, limitReached, err := w.getJobs()
	if err != nil {
		if w.lifecycle.ctx.Err() != nil {
			return false
		}
		w.log.Errorw("failed to fetch jobs for archiving", "error", err)
		panic(err)
	}

	if len(jobs) == 0 {
		return false
	}

	if !limitReached && time.Since(w.lastUploadTime) < w.config.uploadFrequency {
		return false // respect the upload frequency
	}

	log := w.log.With("workspaceID", w.workspaceID)

	location, err := w.uploadJobs(w.lifecycle.ctx, jobs)
	if err != nil {
		log.Errorw("failed to upload jobs", "error", err)
		return false
	}
	w.lastUploadTime = time.Now()

	if err := w.markStatus(
		jobs,
		jobsdb.Succeeded.State,
		locationJSON(location),
	); err != nil {
		if w.lifecycle.ctx.Err() != nil {
			return false
		}
		log.Errorw("failed to mark successful upload status", "error", err)
		panic(err)
	}
	w.stats.NewTaggedStat("arc_uploaded_jobs", stats.CountType, map[string]string{"workspaceId": workspaceID, "sourceId": w.sourceID}).Count(len(jobs))
	if !limitReached {
		return true
	}
	goto start
}

func (w *worker) SleepDurations() (min, max time.Duration) {
	if w.lastUploadTime.IsZero() {
		return w.config.minSleep, w.config.uploadFrequency
	}
	return w.config.minSleep, time.Until(w.lastUploadTime.Add(w.config.uploadFrequency))
}

func (w *worker) Stop() {
	w.lifecycle.cancel()
}

func (w *worker) uploadJobs(ctx context.Context, jobs []*jobsdb.JobT) (string, error) {
	defer w.uploadLimiter.Begin(w.sourceID)()

	aggregatedJobs, err := aggregateJobs(jobs)
	if err != nil {
		return "", fmt.Errorf("aggregate jobs: %w", err)
	}

	var files []*os.File
	defer func() {
		for _, file := range files {
			misc.RemoveFilePaths(file.Name())
		}
	}()
	for _, aggregatedJob := range aggregatedJobs {
		if len(aggregatedJob) == 0 {
			continue
		}

		if w.configFetcher.IsPIIReportingDisabled(w.workspaceID) {
			transformPayloadForPII(aggregatedJob)
		}

		f, err := w.createFile(ctx, aggregatedJob)
		if err != nil {
			return "", fmt.Errorf("creating file: %w", err)
		}

		firstFailedAt := aggregatedJob[0].FailedAt.UTC()
		prefixes := []string{
			w.sourceID,
			firstFailedAt.Format("2006-01-02"),
			strconv.Itoa(firstFailedAt.Hour()),
		}
		if _, err = w.uploader.Upload(ctx, f, prefixes...); err != nil {
			return "", fmt.Errorf("uploading file to object storage: %w", err)
		}

		files = append(files, f)
	}

	return uploadOutput.Location, nil
}

func aggregateJobs(jobs []*jobsdb.JobT) (map[string][]payload, error) {
	aggregatedJobs := make(map[string][]payload)
	for _, job := range jobs {
		var p payload
		if err := json.Unmarshal(job.EventPayload, &p); err != nil {
			return nil, fmt.Errorf("unmarshalling payload: %v", err)
		}

		key := aggregateKey(p)
		aggregatedJobs[key] = append(aggregatedJobs[key], p)
	}
	return aggregatedJobs, nil
}

func aggregateKey(payload payload) string {
	return payload.FailedAt.Format("2006-01-02/15")
}

func transformPayloadForPII(payloads []payload) {
	for _, payload := range payloads {
		payload.EventName = ""
	}
}

func (w *worker) createFile(ctx context.Context, payloads []payload) (*os.File, error) {
	firstFailedAt := payloads[0].FailedAt.UTC()
	lastFailedAt := payloads[len(payloads)-1].FailedAt.UTC()

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	filePath := path.Join(
		tmpDirPath,
		"rudder-failed-messages",
		w.sourceID,
		fmt.Sprintf("%d_%d_%s%s", firstFailedAt.Unix(), lastFailedAt.Unix(), w.config.instanceID, w.writer.Extension()),
	)

	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return nil, fmt.Errorf("mkdir tmp directory full path: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err = w.writer.Write(f, payloads); err != nil {
		return nil, fmt.Errorf("writing to file: %w", err)
	}

	prefixes := []string{
		w.sourceID,
		firstFailedAt.Format("2006-01-02"),
		strconv.Itoa(firstFailedAt.Hour()),
	}
	if _, err = w.uploader.Upload(ctx, f, prefixes...); err != nil {
		return nil, fmt.Errorf("uploading file to object storage: %w", err)
	}

	return f, nil
}

func (w *worker) getJobs() ([]*jobsdb.JobT, bool, error) {
	defer w.fetchLimiter.Begin(w.sourceID)()

	params := w.queryParams
	params.PayloadSizeLimit = w.payloadLimitFunc(w.config.payloadLimit())
	params.EventsLimit = w.config.eventsLimit()
	params.JobsLimit = w.config.eventsLimit()

	unProcessed, err := w.jobsDB.GetUnprocessed(w.lifecycle.ctx, params)
	if err != nil {
		return nil, false, fmt.Errorf("fetching unprocessed jobs: %w", err)
	}

	return unProcessed.Jobs, unProcessed.LimitsReached, nil
}

func (w *worker) markStatus(
	jobs []*jobsdb.JobT, state string, response []byte,
) error {
	defer w.updateLimiter.Begin(w.sourceID)()

	workspaceID := jobs[0].WorkspaceId
	if err := misc.RetryWithNotify(
		w.lifecycle.ctx,
		w.config.uploadFrequency,
		w.config.jobsdbMaxRetries(),
		func(ctx context.Context) error {
			return w.jobsDB.UpdateJobStatus(
				ctx,
				lo.Map(jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
					return &jobsdb.JobStatusT{
						JobID:         job.JobID,
						JobState:      state,
						ErrorResponse: response,
						Parameters:    []byte(`{}`),
						AttemptNum:    job.LastJobStatus.AttemptNum + 1,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
					}
				}),
				nil,
				nil,
			)
		},
		func(attempt int) {
			w.log.Warnw("failed to mark jobs' status", "attempt", attempt)
		},
	); err != nil {
		return err
	}
	w.stats.NewTaggedStat("arc_processed_jobs", stats.CountType, map[string]string{"workspaceId": workspaceID, "sourceId": w.sourceID, "state": state}).Count(len(jobs))
	return nil
}

func errJSON(err error) []byte {
	m := struct {
		Error string `json:"error"`
	}{
		Error: err.Error(),
	}
	b, _ := json.Marshal(m)
	return b
}

func locationJSON(location string) []byte {
	m := map[string]string{"location": location}
	b, _ := json.Marshal(m)
	return b
}
