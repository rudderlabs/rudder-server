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

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	errorStashEnabled       bool
	errReadLoopSleep        time.Duration
	errDBReadBatchSize      int
	noOfErrStashWorkers     int
	maxFailedCountForErrJob int
	pkgLogger               logger.LoggerI
)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor").Child("stash")
}

func loadConfig() {
	config.RegisterBoolConfigVariable(true, &errorStashEnabled, true, "Processor.errorStashEnabled")
	config.RegisterDurationConfigVariable(time.Duration(30), &errReadLoopSleep, true, time.Second, []string{"Processor.errReadLoopSleep", "errReadLoopSleepInS"}...)
	config.RegisterIntConfigVariable(1000, &errDBReadBatchSize, true, 1, "Processor.errDBReadBatchSize")
	config.RegisterIntConfigVariable(2, &noOfErrStashWorkers, true, 1, "Processor.noOfErrStashWorkers")
	config.RegisterIntConfigVariable(3, &maxFailedCountForErrJob, true, 1, "Processor.maxFailedCountForErrJob")
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
}

func New() *HandleT {
	return &HandleT{}
}

func (st *HandleT) Setup(errorDB jobsdb.JobsDB) {
	st.logger = pkgLogger
	st.errorDB = errorDB
	st.stats = stats.DefaultStats
	st.statErrDBR = st.stats.NewStat("processor.err_db_read_time", stats.TimerType)
	st.statErrDBW = st.stats.NewStat("processor.err_db_write_time", stats.TimerType)
	st.crashRecover()
}

func (st *HandleT) crashRecover() {
	st.errorDB.DeleteExecuting(jobsdb.GetQueryParamsT{Count: -1})
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

	g.Wait()
}

func (st *HandleT) setupFileUploader() {
	if errorStashEnabled {
		provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "")
		bucket := config.GetEnv("JOBS_BACKUP_BUCKET", "")
		if provider != "" && bucket != "" {
			var err error
			st.errFileUploader, err = filemanager.New(&filemanager.SettingsT{
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
		g.Go(func() error {
			for {
				jobs, ok := <-st.errProcessQ
				if !ok {
					return nil
				}

				uploadStat := stats.NewStat("Processor.err_upload_time", stats.TimerType)
				uploadStat.Start()
				output := st.storeErrorsToObjectStorage(jobs)
				st.setErrJobStatus(jobs, output)
				uploadStat.End()

			}
		})
	}

	g.Wait()
}

func (st *HandleT) storeErrorsToObjectStorage(jobs []*jobsdb.JobT) StoreErrorOutputT {
	localTmpDirName := "/rudder-processor-errors/"

	uuid := uuid.NewV4()
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
	gzWriter.Write(content)
	gzWriter.CloseGZ()

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}
	prefixes := []string{"rudder-proc-err-logs", time.Now().Format("01-02-2006")}
	uploadOutput, err := st.errFileUploader.Upload(outputFile, prefixes...)

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

			//NOTE: sending custom val filters array of size 1 to take advantage of cache in jobsdb.
			toQuery := errDBReadBatchSize
			retryList := st.errorDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{""}, Count: toQuery, IgnoreCustomValFiltersInQuery: true})
			toQuery -= len(retryList)
			unprocessedList := st.errorDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{""}, Count: toQuery, IgnoreCustomValFiltersInQuery: true})

			st.statErrDBR.End()

			combinedList := append(retryList, unprocessedList...)

			if len(combinedList) == 0 {
				st.logger.Debug("[Processor: readErrJobsLoop]: DB Read Complete. No proc_err Jobs to process")
				continue
			}

			hasFileUploader := st.errFileUploader != nil

			jobState := jobsdb.Executing.State
			// abort jobs if file uploader not configured to store them to object storage
			if !hasFileUploader {
				jobState = jobsdb.Aborted.State
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
				}
				statusList = append(statusList, &status)
			}

			err := st.errorDB.UpdateJobStatus(statusList, nil, nil)
			if err != nil {
				pkgLogger.Errorf("Error occurred while marking proc error jobs statuses as %v. Panicking. Err: %v", jobState, err)
				panic(err)
			}

			if hasFileUploader {
				st.errProcessQ <- combinedList
			}
		}
	}
}
