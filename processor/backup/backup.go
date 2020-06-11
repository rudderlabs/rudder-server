package backup

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
	errReadLoopSleep        time.Duration
	errDBReadBatchSize      int
	noOfErrBackupWorkers    int
	maxFailedCountForErrJob int
)

func init() {
	loadConfig()
}

func loadConfig() {
	errReadLoopSleep = config.GetDuration("Processor.errReadLoopSleepInS", time.Duration(30)) * time.Second
	errDBReadBatchSize = config.GetInt("Processor.errDBReadBatchSize", 10000)
	noOfErrBackupWorkers = config.GetInt("Processor.noOfErrBackupWorkers", 2)
	maxFailedCountForErrJob = config.GetInt("BatchRouter.maxFailedCountForErrJob", 3)
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
}

func (bk *HandleT) storeErorrsToObjectStorage(jobs []*jobsdb.JobT) StoreErrorOutputT {
	localTmpDirName := "/rudder-processor-errors/"

	uuid := uuid.NewV4()
	logger.Debug("[Processor: storeErorrsToObjectStorage]: Starting logging to object storage")

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
	uploadOutput, err := bk.errFileUploader.Upload(outputFile, prefixes...)

	return StoreErrorOutputT{
		Location: uploadOutput.Location,
		Error:    err,
	}
}

func (bk *HandleT) setErrJobStatus(jobs []*jobsdb.JobT, output StoreErrorOutputT) {
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
	bk.errorDB.UpdateJobStatus(statusList, nil, nil)
}

func (bk *HandleT) initErrWorkers() {
	for i := 0; i < noOfErrBackupWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					select {
					case jobs := <-bk.errProcessQ:
						uploadStat := stats.NewStat("Processor.err_upload_time", stats.TimerType)
						uploadStat.Start()
						output := bk.storeErorrsToObjectStorage(jobs)
						bk.setErrJobStatus(jobs, output)
						uploadStat.End()
					}
				}
			}()
		})
	}
}

func (bk *HandleT) readErrJobsLoop() {
	logger.Info("Processor errors backup loop started")

	for {
		time.Sleep(errReadLoopSleep)
		bk.statErrDBR.Start()

		toQuery := errDBReadBatchSize
		retryList := bk.errorDB.GetToRetry(nil, toQuery, nil)
		toQuery -= len(retryList)
		unprocessedList := bk.errorDB.GetUnprocessed(nil, toQuery, nil)

		bk.statErrDBR.End()

		combinedList := append(retryList, unprocessedList...)

		if len(combinedList) == 0 {
			logger.Debug("[Processor: readErrJobsLoop]: DB Read Complete. No proc_err Jobs to process")
			continue
		}

		hasFileUploader := bk.errFileUploader != nil

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

		bk.errorDB.UpdateJobStatus(statusList, nil, nil)
		if hasFileUploader {
			bk.errProcessQ <- combinedList
		}
	}
}

func (bk *HandleT) crashRecover() {
	for {
		execList := bk.errorDB.GetExecuting(nil, errDBReadBatchSize, nil)

		if len(execList) == 0 {
			break
		}
		logger.Debug("Process Error Backup crash recovering", len(execList))

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
		bk.errorDB.UpdateJobStatus(statusList, nil, nil)
	}
}

func New() *HandleT {
	return &HandleT{}
}

func (bk *HandleT) Setup(errorDB jobsdb.JobsDB) {
	bk.errorDB = errorDB
	bk.stats = stats.DefaultStats
	bk.statErrDBR = bk.stats.NewStat("processor.err_db_read_time", stats.TimerType)
	bk.statErrDBW = bk.stats.NewStat("processor.err_db_write_time", stats.TimerType)

	bk.crashRecover()
}

func (bk *HandleT) Start() {
	provider := config.GetEnv("PROC_ERROR_BACKUP_STORAGE_PROVIDER", "")
	bucket := config.GetEnv("PROC_ERROR_BACKUP_BUCKET", "")
	if provider != "" && bucket != "" {
		var err error
		bk.errFileUploader, err = filemanager.New(&filemanager.SettingsT{
			Provider: provider,
			Config:   filemanager.GetProviderConfigFromEnv("PROC_ERROR_BACKUP"),
		})
		if err != nil {
			panic(err)
		}
	}
	bk.errProcessQ = make(chan []*jobsdb.JobT)
	bk.initErrWorkers()
	bk.readErrJobsLoop()
}
