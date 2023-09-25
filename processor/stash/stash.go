package stash

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type StoreErrorOutputT struct {
	Location string
	Error    error
}

type ErrorJob struct {
	jobs        []*jobsdb.JobT
	errorOutput StoreErrorOutputT
}

type HandleT struct {
	errorDB         jobsdb.JobsDB
	errProcessQ     chan []*jobsdb.JobT
	statErrDBR      stats.Measurement
	logger          logger.Logger
	transientSource transientsource.Service
	fileuploader    fileuploader.Provider

	adaptiveLimit func(int64) int64
	config        struct {
		jobsDBCommandTimeout      misc.ValueLoader[time.Duration]
		jobdDBQueryRequestTimeout misc.ValueLoader[time.Duration]
		jobdDBMaxRetries          misc.ValueLoader[int]
		errorStashEnabled         misc.ValueLoader[bool]
		errDBReadBatchSize        misc.ValueLoader[int]
		noOfErrStashWorkers       misc.ValueLoader[int]
		maxFailedCountForErrJob   misc.ValueLoader[int]
		pkgLogger                 logger.Logger
		payloadLimit              misc.ValueLoader[int64]
	}
}

func New() *HandleT {
	return &HandleT{}
}

func (st *HandleT) Setup(
	errorDB jobsdb.JobsDB,
	transientSource transientsource.Service,
	fileuploader fileuploader.Provider,
	adaptiveLimitFunc func(int64) int64,
) {
	st.config.errorStashEnabled = config.GetReloadableBoolVar(true, "Processor.errorStashEnabled")
	st.config.errDBReadBatchSize = config.GetReloadableIntVar(1000, 1, "Processor.errDBReadBatchSize")
	st.config.noOfErrStashWorkers = config.GetReloadableIntVar(2, 1, "Processor.noOfErrStashWorkers")
	st.config.maxFailedCountForErrJob = config.GetReloadableIntVar(3, 1, "Processor.maxFailedCountForErrJob")
	st.config.payloadLimit = config.GetReloadableInt64Var(100*bytesize.MB, 1, "Processor.stashP	ayloadLimit")
	st.config.jobdDBMaxRetries = config.GetReloadableIntVar(2, 1, "JobsDB.Processor.MaxRetries", "JobsDB.MaxRetries")
	st.config.jobdDBQueryRequestTimeout = config.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.QueryRequestTimeout", "JobsDB.QueryRequestTimeout")
	st.config.jobsDBCommandTimeout = config.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.CommandRequestTimeout", "JobsDB.CommandRequestTimeout")

	st.logger = logger.NewLogger().Child("processor").Child("stash")
	st.errorDB = errorDB
	st.statErrDBR = stats.Default.NewStat("processor.err_db_read_time", stats.TimerType)
	st.transientSource = transientSource
	st.fileuploader = fileuploader
	st.adaptiveLimit = adaptiveLimitFunc
	st.crashRecover()
}

func (st *HandleT) crashRecover() {
	st.errorDB.FailExecuting()
}

func (st *HandleT) Start(ctx context.Context) {
	st.errProcessQ = make(chan []*jobsdb.JobT)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		st.runErrWorkers(ctx)
		return nil
	})

	g.Go(func() error {
		st.readErrJobsLoop(ctx)
		return nil
	})

	_ = g.Wait()
}

func (st *HandleT) sendRetryUpdateStats(attempt int) {
	st.logger.Warnf("Timeout during update job status in stash module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "stash"}).Count(1)
}

func (st *HandleT) sendQueryRetryStats(attempt int) {
	st.logger.Warnf("Timeout during query jobs in stash module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "stash"}).Count(1)
}

func (st *HandleT) backupEnabled(jd jobsdb.JobsDB) bool {
	return st.config.errorStashEnabled.Load() && jd.IsMasterBackupEnabled()
}

func (st *HandleT) runErrWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for i := 0; i < st.config.noOfErrStashWorkers.Load(); i++ {
		g.Go(misc.WithBugsnag(func() error {
			for jobs := range st.errProcessQ {
				uploadStart := time.Now()
				uploadStat := stats.Default.NewStat("Processor.err_upload_time", stats.TimerType)
				errorJobs := st.storeErrorsToObjectStorage(jobs)
				for _, errorJob := range errorJobs {
					st.setErrJobStatus(errorJob.jobs, errorJob.errorOutput)
				}
				uploadStat.Since(uploadStart)
			}

			return nil
		}))
	}

	_ = g.Wait()
}

func (st *HandleT) storeErrorsToObjectStorage(jobs []*jobsdb.JobT) (errorJob []ErrorJob) {
	localTmpDirName := "/rudder-processor-errors/"

	uuid := uuid.New().String()
	st.logger.Debug("[Processor: storeErrorsToObjectStorage]: Starting logging to object storage")

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}

	jobsPerWorkspace := lo.GroupBy(jobs, func(job *jobsdb.JobT) string {
		return job.WorkspaceId
	})
	gzWriter := fileuploader.NewGzMultiFileWriter()
	dumps := make(map[string]string)

	errorJobs := make([]ErrorJob, 0)

	for workspaceID, jobsForWorkspace := range jobsPerWorkspace {
		preferences, err := st.fileuploader.GetStoragePreferences(workspaceID)
		if err != nil {
			st.logger.Errorf("Skipping Storing errors for workspace: %s since no storage preferences are found", workspaceID)
			errorJobs = append(errorJobs, ErrorJob{
				jobs: jobsForWorkspace,
				errorOutput: StoreErrorOutputT{
					Location: "",
					Error:    err,
				},
			})
			continue
		}
		if !preferences.ProcErrors {
			st.logger.Infof("Skipping Storing errors for workspace: %s since ProcErrors is set to false", workspaceID)
			errorJobs = append(errorJobs, ErrorJob{
				jobs: jobsForWorkspace,
				errorOutput: StoreErrorOutputT{
					Location: "",
					Error:    nil,
				},
			})
			continue
		}
		path := fmt.Sprintf("%v%v.json.gz", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v.%v.%v.%v", time.Now().Unix(), config.GetString("INSTANCE_ID", "1"), fmt.Sprintf("%v-%v", jobs[0].JobID, jobs[len(jobs)-1].JobID), uuid, workspaceID))
		dumps[workspaceID] = path
		newline := []byte("\n")
		lo.ForEach(jobsForWorkspace, func(job *jobsdb.JobT, _ int) {
			rawJob, err := json.Marshal(job)
			if err != nil {
				panic(err)
			}
			if _, err := gzWriter.Write(path, append(rawJob, newline...)); err != nil {
				panic(err)
			}
		})
	}

	err = gzWriter.Close()
	if err != nil {
		panic(err)
	}
	defer func() {
		for _, path := range dumps {
			os.Remove(path)
		}
	}()

	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(config.GetInt("Processor.errorBackupWorkers", 100))
	var mu sync.Mutex
	for workspaceID, filePath := range dumps {
		wrkId := workspaceID
		path := filePath
		errFileUploader, err := st.fileuploader.GetFileManager(wrkId)
		if err != nil {
			st.logger.Errorf("Skipping Storing errors for workspace: %s since no file manager is found", workspaceID)
			mu.Lock()
			errorJobs = append(errorJobs, ErrorJob{
				jobs: jobsPerWorkspace[workspaceID],
				errorOutput: StoreErrorOutputT{
					Location: "",
					Error:    err,
				},
			})
			mu.Unlock()
			continue
		}
		g.Go(misc.WithBugsnag(func() error {
			outputFile, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			prefixes := []string{"rudder-proc-err-logs", time.Now().Format("01-02-2006")}
			uploadOutput, err := errFileUploader.Upload(context.TODO(), outputFile, prefixes...)
			st.logger.Infof("Uploaded error logs to %s for workspaceId %s", uploadOutput.Location, wrkId)
			mu.Lock()
			errorJobs = append(errorJobs, ErrorJob{
				jobs: jobsPerWorkspace[wrkId],
				errorOutput: StoreErrorOutputT{
					Location: uploadOutput.Location,
					Error:    err,
				},
			})
			mu.Unlock()
			return nil
		}))
	}

	_ = g.Wait()

	return errorJobs
}

func (st *HandleT) setErrJobStatus(jobs []*jobsdb.JobT, output StoreErrorOutputT) {
	var statusList []*jobsdb.JobStatusT
	for _, job := range jobs {
		state := jobsdb.Succeeded.State
		errorResp := []byte(`{"success":"OK"}`)
		if output.Error != nil {
			var err error
			errorResp, err = json.Marshal(struct{ Error string }{output.Error.Error()})
			if err != nil {
				panic(err)
			}
			if job.LastJobStatus.AttemptNum >= st.config.maxFailedCountForErrJob.Load() {
				state = jobsdb.Aborted.State
			} else {
				state = jobsdb.Failed.State
			}
		}
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			JobState:      state,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: errorResp,
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
		statusList = append(statusList, &status)
	}
	err := misc.RetryWithNotify(context.Background(), st.config.jobsDBCommandTimeout.Load(), st.config.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return st.errorDB.UpdateJobStatus(ctx, statusList, nil, nil)
	}, st.sendRetryUpdateStats)
	if err != nil {
		st.logger.Errorf("Error occurred while updating proc error jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
}

func (st *HandleT) readErrJobsLoop(ctx context.Context) {
	st.logger.Info("Processor errors stash loop started")
	var sleepTime time.Duration
	for {
		select {
		case <-ctx.Done():
			close(st.errProcessQ)
			return
		case <-time.After(sleepTime):
			start := time.Now()
			var combinedList []*jobsdb.JobT
			var limitReached bool
			// NOTE: sending custom val filters array of size 1 to take advantage of cache in jobsdb.
			queryParams := jobsdb.GetQueryParams{
				CustomValFilters:              []string{""},
				IgnoreCustomValFiltersInQuery: true,
				JobsLimit:                     st.config.errDBReadBatchSize.Load(),
				PayloadSizeLimit:              st.adaptiveLimit(st.config.payloadLimit.Load()),
			}

			toProcess, err := misc.QueryWithRetriesAndNotify(ctx, st.config.jobdDBQueryRequestTimeout.Load(), st.config.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
				return st.errorDB.GetJobs(ctx, []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, queryParams)
			}, st.sendQueryRetryStats)
			if err != nil {
				if ctx.Err() != nil { // we are shutting down
					close(st.errProcessQ)
					return //nolint:nilerr
				}
				st.logger.Errorf("Error occurred while reading proc error jobs. Err: %v", err)
				panic(err)
			}

			combinedList = toProcess.Jobs
			limitReached = toProcess.LimitsReached

			st.statErrDBR.Since(start)

			if len(combinedList) == 0 {
				st.logger.Debug("[Processor: readErrJobsLoop]: DB Read Complete. No proc_err Jobs to process")
				sleepTime = st.calculateSleepTime(limitReached)
				continue
			}

			canUpload := st.backupEnabled(st.errorDB)

			jobState := jobsdb.Executing.State

			var filteredJobList []*jobsdb.JobT

			// abort jobs if file uploader not configured to store them to object storage
			// or backup is not enabled
			if !canUpload {
				jobState = jobsdb.Aborted.State
				filteredJobList = combinedList
			}
			var statusList []*jobsdb.JobStatusT

			for _, job := range combinedList {

				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
					JobState:      jobState,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`),
					Parameters:    []byte(`{}`),
					JobParameters: job.Parameters,
					WorkspaceId:   job.WorkspaceId,
				}

				if canUpload {
					if st.transientSource.ApplyJob(job) {
						// if it is a transient source, we don't process the job and mark it as aborted
						status.JobState = jobsdb.Aborted.State
					} else {
						filteredJobList = append(filteredJobList, job)
					}
				}
				statusList = append(statusList, &status)
			}
			if err := misc.RetryWithNotify(context.Background(), st.config.jobsDBCommandTimeout.Load(), st.config.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return st.errorDB.UpdateJobStatus(ctx, statusList, nil, nil)
			}, st.sendRetryUpdateStats); err != nil {
				if ctx.Err() != nil { // we are shutting down
					return //nolint:nilerr
				}
				st.logger.Errorf("Error occurred while marking proc error jobs statuses as %v. Panicking. Err: %v", jobState, err)
				panic(err)
			}

			if canUpload && len(filteredJobList) > 0 {
				st.errProcessQ <- filteredJobList
			}
			sleepTime = st.calculateSleepTime(limitReached)
		}
	}
}

func (*HandleT) calculateSleepTime(limitReached bool) time.Duration {
	if limitReached {
		return time.Duration(0)
	}
	return config.GetDuration("Processor.errReadLoopSleep", 30, time.Second)
}
