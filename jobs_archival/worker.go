package jobs_archival

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type worker struct {
	log       logger.Logger
	partition string
	*archiver
	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
}

func (w *worker) Work() bool {
	var (
		params = jobsdb.GetQueryParamsT{
			IgnoreCustomValFiltersInQuery: true,
			PayloadSizeLimit:              w.limiter(payloadLimit()),
			ParameterFilters:              sourceParam(w.partition),
			EventsLimit:                   eventsLimit(),
			JobsLimit:                     eventsLimit(),
		}
		toArchive           = true
		res                 uploadResult
		err                 error
		failed, unProcessed jobsdb.JobsResult
		jobs                []*jobsdb.JobT
		limitReached        bool
	)
start:
	failed, err = w.jobsDB.GetToRetry(w.lifecycle.ctx, params)
	if err != nil {
		w.log.Errorf("failed to fetch jobs for backup - partition: %s - %w", w.partition, err)
		if w.lifecycle.ctx.Err() != nil {
			panic(err)
		}
		return false
	}
	limitReached = failed.LimitsReached
	jobs = failed.Jobs
	if !limitReached {
		params.EventsLimit -= failed.EventsCount
		params.PayloadSizeLimit -= failed.PayloadSize
		unProcessed, err = w.jobsDB.GetUnprocessed(w.lifecycle.ctx, params)
		if err != nil {
			w.log.Errorf("failed to fetch unprocessed jobs for backup - partition: %s - %w", w.partition, err)
			if w.lifecycle.ctx.Err() != nil {
				panic(err)
			}
			return false
		}
		jobs = append(jobs, unProcessed.Jobs...)
		limitReached = unProcessed.LimitsReached
	}

	if len(jobs) == 0 {
		return false
	}

	storagePrefs, err := w.storageProvider.GetStoragePreferences(jobs[0].WorkspaceId)
	if err != nil {
		w.log.Errorf("failed to fetch storage preferences for workspaceID: %s - %w", jobs[0].WorkspaceId, err)
		toArchive = false
	}
	if !storagePrefs.Backup(w.jobsDB.Identifier()) {
		toArchive = false
	}
	var statusList []*jobsdb.JobStatusT
	if !toArchive {
		statusList = getStatuses(
			jobs,
			func(*jobsdb.JobT) string { return jobsdb.Aborted.State },
			[]byte(`{"location": "not uploaded because storage not enabled/configured"}`),
		)
		goto markStatus
	}

	res = w.uploadJobs(w.lifecycle.ctx, jobs)
	if res.err != nil {
		w.log.Errorf("failed to upload jobs - partition: %s - %w", w.partition, res.err)
		if w.lifecycle.ctx.Err() != nil {
			panic(err)
		}
		statusList = getStatuses(
			jobs,
			func(job *jobsdb.JobT) string {
				if job.LastJobStatus.AttemptNum >= maxRetryAttempts() {
					return jobsdb.Aborted.State
				}
				return jobsdb.Failed.State
			},
			[]byte(fmt.Sprintf(`{"location": "not uploaded because - %v"}`, res.err)),
		)
		goto markStatus
	}

	statusList = getStatuses(
		jobs,
		func(*jobsdb.JobT) string { return jobsdb.Succeeded.State },
		[]byte(fmt.Sprintf(`{"location": "%v"}`, res.location)),
	)

markStatus:
	if err := w.jobsDB.UpdateJobStatus(w.lifecycle.ctx, statusList, nil, nil); err != nil {
		w.log.Errorf("failed to mark jobs' status - %w", err)
		if w.lifecycle.ctx.Err() != nil {
			panic(err)
		}
	}
	if limitReached {
		goto start
	}
	return true
}

func (w *worker) SleepDurations() (min, max time.Duration) {
	return 0, 0
}

func (w *worker) Stop() {
	w.lifecycle.cancel()
}

type uploadResult struct {
	location string
	err      error
}

func (w *worker) uploadJobs(ctx context.Context, jobs []*jobsdb.JobT) uploadResult {
	firstJobCreatedAt := jobs[0].CreatedAt
	workspaceID := jobs[0].WorkspaceId

	w.log.Debugf("[Archival: storeErrorsToObjectStorage]: Starting logging to object storage - %s", w.partition)

	gzWriter := fileuploader.NewGzMultiFileWriter()
	path := fmt.Sprintf(
		"%v%v.json.gz",
		lo.Must(misc.CreateTMPDIR())+"/rudder-backups/",
		fmt.Sprintf("%v", firstJobCreatedAt.Unix()),
	)

	for _, job := range jobs {
		rawJob, err := json.Marshal(job)
		if err != nil {
			panic(err)
		}
		if _, err := gzWriter.Write(path, append(rawJob, '\n')); err != nil {
			panic(err)
		}
	}
	err := gzWriter.Close()
	if err != nil {
		panic(err)
	}
	defer os.Remove(path)

	fileUploader, err := w.storageProvider.GetFileManager(workspaceID)
	if err != nil {
		w.log.Errorf("Skipping Storing errors for workspace: %s - partition: %s since no file manager is found",
			workspaceID, w.partition,
		)
		return uploadResult{
			err:      err,
			location: "",
		}
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	prefixes := []string{
		w.partition,
		firstJobCreatedAt.Format("2005-01-31"),
		strconv.Itoa(firstJobCreatedAt.Hour()),
		instanceID,
		w.jobsDB.Identifier(),
	}
	uploadOutput, err := fileUploader.Upload(ctx, file, prefixes...)
	if err != nil {
		w.log.Errorf("failed to upload file to object storage - %w", err)
		return uploadResult{
			err:      err,
			location: "",
		}
	}

	return uploadResult{
		err:      nil,
		location: uploadOutput.Location,
	}
}

func getStatuses(jobs []*jobsdb.JobT, stateFunc func(*jobsdb.JobT) string, response []byte) []*jobsdb.JobStatusT {
	return lo.Map(jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
		return &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      stateFunc(job),
			ErrorResponse: response,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
		}
	})
}
