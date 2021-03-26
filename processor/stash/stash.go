package stash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
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
	errorStashEnabled = config.GetBool("Processor.errorStashEnabled", true)
	errReadLoopSleep = config.GetDuration("Processor.errReadLoopSleepInS", time.Duration(30)) * time.Second
	errDBReadBatchSize = config.GetInt("Processor.errDBReadBatchSize", 1000)
	noOfErrStashWorkers = config.GetInt("Processor.noOfErrStashWorkers", 2)
	maxFailedCountForErrJob = config.GetInt("Processor.maxFailedCountForErrJob", 3)
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
	for {
		execList := st.errorDB.GetExecuting(jobsdb.GetQueryParamsT{Count: errDBReadBatchSize})

		if len(execList) == 0 {
			break
		}
		st.logger.Debug("Process Error Stash crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.Failed.State,
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		err := st.errorDB.UpdateJobStatus(statusList, nil, nil)
		if err != nil {
			pkgLogger.Errorf("Error occurred while marking proc error jobs statuses as failed. Panicking. Err: %v", err)
			panic(err)
		}
	}
}

func (st *HandleT) Start() {
	st.setupFileUploader()
	st.errProcessQ = make(chan []*jobsdb.JobT)
	st.initErrWorkers()
	st.readErrJobsLoop()
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

func (st *HandleT) initErrWorkers() {
	for i := 0; i < noOfErrStashWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					select {
					case jobs := <-st.errProcessQ:
						uploadStat := stats.NewStat("Processor.err_upload_time", stats.TimerType)
						uploadStat.Start()
						output := st.storeErrorsToObjectStorage(jobs)
						st.setErrJobStatus(jobs, output)
						uploadStat.End()
					}
				}
			}()
		})
	}
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
		}
		statusList = append(statusList, &status)
	}
	err := st.errorDB.UpdateJobStatus(statusList, nil, nil)
	if err != nil {
		pkgLogger.Errorf("Error occurred while updating proc error jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
}

func (st *HandleT) readErrJobsLoop() {
	st.logger.Info("Processor errors stash loop started")

	for {
		time.Sleep(errReadLoopSleep)
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
