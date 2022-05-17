package stash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	errorStashEnabled       bool
	errReadLoopSleep        time.Duration
	errDBReadBatchSize      int
	noOfErrStashWorkers     int
	maxFailedCountForErrJob int
	pkgLogger               logger.LoggerI
	payloadLimit            int64
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor").Child("stash")
}

func loadConfig() {
	config.RegisterBoolConfigVariable(true, &errorStashEnabled, true, "Processor.errorStashEnabled")
	config.RegisterDurationConfigVariable(30, &errReadLoopSleep, true, time.Second, []string{"Processor.errReadLoopSleep", "errReadLoopSleepInS"}...)
	config.RegisterIntConfigVariable(1000, &errDBReadBatchSize, true, 1, "Processor.errDBReadBatchSize")
	config.RegisterIntConfigVariable(2, &noOfErrStashWorkers, true, 1, "Processor.noOfErrStashWorkers")
	config.RegisterIntConfigVariable(3, &maxFailedCountForErrJob, true, 1, "Processor.maxFailedCountForErrJob")
	config.RegisterInt64ConfigVariable(100*bytesize.MB, &payloadLimit, true, 1, "Processor.payloadLimit")
}

type StoreErrorOutputT struct {
	Location string
	Error    error
}

type HandleT struct {
	errorDB         jobsdb.JobsDB
	errProcessQ     chan []*jobsdb.JobT
	errFileUploader filemanager.FileManager
	stats           stats.Stats
	statErrDBR      stats.RudderStats
	statErrDBW      stats.RudderStats
	logger          logger.LoggerI
	transientSource transientsource.Service
}

func New() *HandleT {
	return &HandleT{}
}

func (st *HandleT) Setup(errorDB jobsdb.JobsDB, transientSource transientsource.Service) {
	st.logger = pkgLogger
	st.errorDB = errorDB
	st.stats = stats.DefaultStats
	st.statErrDBR = st.stats.NewStat("processor.err_db_read_time", stats.TimerType)
	st.statErrDBW = st.stats.NewStat("processor.err_db_write_time", stats.TimerType)
	st.transientSource = transientSource
	st.crashRecover()
}

func (st *HandleT) crashRecover() {
	st.errorDB.DeleteExecuting()
}

func (st *HandleT) Start(ctx context.Context) {
	st.setupFileUploader()
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

func (st *HandleT) setupFileUploader() {
	if errorStashEnabled && jobsdb.IsMasterBackupEnabled() {
		provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "")
		bucket := config.GetEnv("JOBS_BACKUP_BUCKET", "")
		if provider != "" && bucket != "" {
			var err error
			st.errFileUploader, err = filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
				Provider: provider,
				Config:   filemanager.GetProviderConfigFromEnv(),
			})
			if err != nil {
				panic(err)
			}
		}
	}
}

func (st *HandleT) runErrWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for i := 0; i < noOfErrStashWorkers; i++ {
		g.Go(misc.WithBugsnag(func() error {
			for jobs := range st.errProcessQ {
				uploadStat := stats.NewStat("Processor.err_upload_time", stats.TimerType)
				uploadStat.Start()
				output := st.storeErrorsToObjectStorage(jobs)
				st.setErrJobStatus(jobs, output)
				uploadStat.End()
			}

			return nil
		}))
	}

	_ = g.Wait()
}

func (st *HandleT) storeErrorsToObjectStorage(jobs []*jobsdb.JobT) StoreErrorOutputT {
	localTmpDirName := "/rudder-processor-errors/"

	uuid := uuid.Must(uuid.NewV4())
	st.logger.Debug("[Processor: storeErrorsToObjectStorage]: Starting logging to object storage")

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v.%v.%v", time.Now().Unix(), config.GetEnv("INSTANCE_ID", "1"), fmt.Sprintf("%v-%v", jobs[0].JobID, jobs[len(jobs)-1].JobID), uuid))

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		panic(err)
	}
	defer os.Remove(gzipFilePath)

	var contentSlice [][]byte
	for _, job := range jobs {
		rawJob, err := json.Marshal(job)
		if err != nil {
			panic(err)
		}
		contentSlice = append(contentSlice, rawJob)
	}
	content := bytes.Join(contentSlice[:], []byte("\n"))
	if _, err := gzWriter.Write(content); err != nil {
		panic(err)
	}
	if err := gzWriter.CloseGZ(); err != nil {
		panic(err)
	}

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}
	prefixes := []string{"rudder-proc-err-logs", time.Now().Format("01-02-2006")}
	uploadOutput, err := st.errFileUploader.Upload(context.TODO(), outputFile, prefixes...)

	return StoreErrorOutputT{
		Location: uploadOutput.Location,
		Error:    err,
	}
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
			if job.LastJobStatus.AttemptNum >= maxFailedCountForErrJob {
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
			WorkspaceId:   job.WorkspaceId,
		}
		statusList = append(statusList, &status)
	}
	err := st.errorDB.UpdateJobStatus(statusList, nil, nil)
	if err != nil {
		pkgLogger.Errorf("Error occurred while updating proc error jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
}

func (st *HandleT) readErrJobsLoop(ctx context.Context) {
	st.logger.Info("Processor errors stash loop started")

	for {
		select {
		case <-ctx.Done():
			close(st.errProcessQ)
			return
		case <-time.After(errReadLoopSleep):
			st.statErrDBR.Start()

			if !(errorStashEnabled && jobsdb.IsMasterBackupEnabled()) {
				continue
			}
			//NOTE: sending custom val filters array of size 1 to take advantage of cache in jobsdb.
			toQuery := errDBReadBatchSize
			retryList := st.errorDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{""}, IgnoreCustomValFiltersInQuery: true, JobsLimit: toQuery, PayloadSizeLimit: payloadLimit})
			toQuery -= len(retryList)
			unprocessedList := st.errorDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{""}, IgnoreCustomValFiltersInQuery: true, JobsLimit: toQuery, PayloadSizeLimit: payloadLimit})

			st.statErrDBR.End()

			combinedList := append(retryList, unprocessedList...)

			if len(combinedList) == 0 {
				st.logger.Debug("[Processor: readErrJobsLoop]: DB Read Complete. No proc_err Jobs to process")
				continue
			}

			hasFileUploader := st.errFileUploader != nil

			jobState := jobsdb.Executing.State

			var filteredJobList []*jobsdb.JobT

			// abort jobs if file uploader not configured to store them to object storage
			if !hasFileUploader {
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
					WorkspaceId:   job.WorkspaceId,
				}

				if hasFileUploader {
					if st.transientSource.ApplyJob(job) {
						// if it is a transient source, we don't process the job and mark it as aborted
						status.JobState = jobsdb.Aborted.State
					} else {
						filteredJobList = append(filteredJobList, job)
					}
				}
				statusList = append(statusList, &status)
			}

			err := st.errorDB.UpdateJobStatus(statusList, nil, nil)
			if err != nil {
				pkgLogger.Errorf("Error occurred while marking proc error jobs statuses as %v. Panicking. Err: %v", jobState, err)
				panic(err)
			}

			if hasFileUploader && len(filteredJobList) > 0 {
				st.errProcessQ <- filteredJobList
			}
		}
	}
}
