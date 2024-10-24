package archiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/payload"
)

type worker struct {
	log              logger.Logger
	sourceID         string
	archiveFrom      string
	jobsDB           jobsdb.JobsDB
	payloadLimitFunc payload.AdaptiveLimiterFunc
	storageProvider  fileuploader.Provider
	stats            stats.Stats
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

	workspaceID := jobs[0].WorkspaceId
	log := w.log.With("workspaceID", workspaceID)
	storagePrefs, err := w.storageProvider.GetStoragePreferences(w.lifecycle.ctx, workspaceID)
	if err != nil {
		if errors.Is(err, fileuploader.ErrNotSubscribed) {
			log.Debug("not subscribed to backend config")
			return false
		}
		log.Errorw("failed to fetch storage preferences", "error", err)
		if err := w.markStatus(
			jobs,
			jobsdb.Aborted.State,
			errJSON(err),
		); err != nil {
			if w.lifecycle.ctx.Err() != nil {
				return false
			}
			log.Errorw("failed to mark unconfigured archive jobs' status", "error", err)
			panic(err)

		}
		if !limitReached {
			return true
		}
		goto start
	}
	if !storagePrefs.Backup(w.archiveFrom) {
		if err := w.markStatus(
			jobs,
			jobsdb.Aborted.State,
			errJSON(fmt.Errorf("%s archival disabled for workspace %s", w.archiveFrom, workspaceID)),
		); err != nil {
			if w.lifecycle.ctx.Err() != nil {
				return false
			}
			log.Errorw("failed to mark archive disabled jobs' status", "error", err)
			panic(err)
		}
		if !limitReached {
			return true
		}
		goto start
	}

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
	defer w.uploadLimiter.Begin("")()
	firstJobCreatedAt := jobs[0].CreatedAt.UTC()
	lastJobCreatedAt := jobs[len(jobs)-1].CreatedAt.UTC()
	workspaceID := jobs[0].WorkspaceId

	filePath := path.Join(
		lo.Must(misc.CreateTMPDIR()),
		"rudder-backups",
		w.sourceID,
		fmt.Sprintf("%d_%d_%s_%s.json.gz", firstJobCreatedAt.Unix(), lastJobCreatedAt.Unix(), workspaceID, uuid.NewString()),
	)
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("creating gz file %q: mkdir error: %w", filePath, err)
	}
	gzWriter, err := misc.CreateGZ(filePath)
	if err != nil {
		return "", fmt.Errorf("create gz writer: %w", err)
	}
	defer func() { _ = os.Remove(filePath) }()

	for _, job := range jobs {
		j, err := marshalJob(job)
		if err != nil {
			_ = gzWriter.Close()
			return "", fmt.Errorf("marshal job: %w", err)
		}
		if _, err := gzWriter.Write(append(j, '\n')); err != nil {
			_ = gzWriter.Close()
			return "", fmt.Errorf("write to file: %w", err)
		}
	}
	if err := gzWriter.Close(); err != nil {
		return "", fmt.Errorf("close writer: %w", err)
	}

	fileUploader, err := w.storageProvider.GetFileManager(w.lifecycle.ctx, workspaceID)
	if err != nil {
		return "", fmt.Errorf("no file manager found: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file %s: %w", filePath, err)
	}
	defer func() { _ = file.Close() }()
	prefixes := []string{
		w.sourceID,
		w.archiveFrom,
		firstJobCreatedAt.Format("2006-01-02"),
		fmt.Sprintf("%d", firstJobCreatedAt.Hour()),
		w.config.instanceID,
	}
	uploadOutput, err := fileUploader.Upload(ctx, file, prefixes...)
	if err != nil {
		return "", fmt.Errorf("upload file to object storage - %w", err)
	}

	return uploadOutput.Location, nil
}

func (w *worker) getJobs() ([]*jobsdb.JobT, bool, error) {
	defer w.fetchLimiter.Begin("")()
	params := w.queryParams
	params.PayloadSizeLimit = w.payloadLimitFunc(w.config.payloadLimit())
	params.EventsLimit = w.config.eventsLimit()
	params.JobsLimit = w.config.eventsLimit()
	unProcessed, err := w.jobsDB.GetUnprocessed(w.lifecycle.ctx, params)
	if err != nil {
		w.log.Errorw("failed to fetch unprocessed jobs for backup", "error", err)
		return nil, false, err
	}
	return unProcessed.Jobs, unProcessed.LimitsReached, nil
}

func marshalJob(job *jobsdb.JobT) ([]byte, error) {
	var J struct {
		UserID       string          `json:"userId"`
		EventPayload json.RawMessage `json:"payload"`
		CreatedAt    time.Time       `json:"createdAt"`
		MessageID    string          `json:"messageId"`
	}
	J.UserID = job.UserID
	J.EventPayload = job.EventPayload
	J.CreatedAt = job.CreatedAt
	J.MessageID = gjson.GetBytes(job.EventPayload, "messageId").String()
	return json.Marshal(J)
}

func (w *worker) markStatus(
	jobs []*jobsdb.JobT, state string, response []byte,
) error {
	defer w.updateLimiter.Begin("")()
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
