package replay

import (
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

type HandleT struct {
	bucket      string
	db          *jobsdb.HandleT
	toDB        *jobsdb.HandleT
	noOfWorkers int
	workers     []*SourceWorkerT
	dumpsLoader *DumpsLoaderHandleT
	dbReadSize  int
	tablePrefix string
}

func (handle *HandleT) generatorLoop() {
	pkgLogger.Infof("generator reading from replay_jobs_* started")
	var breakLoop bool
	for {
		queryParams := jobsdb.GetQueryParamsT{
			CustomValFilters: []string{"replay"},
			JobsLimit:        handle.dbReadSize,
		}
		toRetry := handle.db.GetToRetry(queryParams)
		combinedList := toRetry.Jobs

		if !toRetry.LimitsReached {
			queryParams.JobsLimit -= len(combinedList)
			unprocessed := handle.db.GetUnprocessed(queryParams)
			combinedList = append(combinedList, unprocessed.Jobs...)
		}
		pkgLogger.Debugf("length of combinedList : %d", len(combinedList))

		if len(combinedList) == 0 {
			if breakLoop {
				executingList := handle.db.GetExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{"replay"}, JobsLimit: handle.dbReadSize})
				pkgLogger.Infof("breakLoop is set. Pending executing: %d", len(executingList.Jobs))
				if len(executingList.Jobs) == 0 {
					break
				}
			}

			if handle.dumpsLoader.done {
				breakLoop = true
			}

			pkgLogger.Debugf("DB Read Complete. No Jobs to process")
			time.Sleep(5 * time.Second)
			continue
		}

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		// List of jobs wich can be processed mapped per channel
		type workerJobT struct {
			worker *SourceWorkerT
			job    *jobsdb.JobT
		}

		var statusList []*jobsdb.JobStatusT
		var toProcess []workerJobT

		for _, job := range combinedList {
			w := handle.workers[rand.Intn(handle.noOfWorkers)]
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
				Parameters:    []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
			toProcess = append(toProcess, workerJobT{worker: w, job: job})
		}

		// Mark the jobs as executing
		handle.db.UpdateJobStatus(statusList, []string{"replay"}, nil)

		// Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.channel <- wrkJob.job
		}
	}

	// Since generator read is done, closing worker channels
	for _, worker := range handle.workers {
		close(worker.channel)
	}
}

func (handle *HandleT) initSourceWorkers() {
	handle.workers = make([]*SourceWorkerT, handle.noOfWorkers)
	for i := 0; i < handle.noOfWorkers; i++ {
		worker := &SourceWorkerT{
			channel:     make(chan *jobsdb.JobT, handle.dbReadSize),
			workerID:    i,
			replayer:    handle,
			tablePrefix: handle.tablePrefix,
		}
		handle.workers[i] = worker
		worker.transformer = transformer.NewTransformer()
		worker.transformer.Setup()
		go worker.workerProcess()
	}
}

func (handle *HandleT) Setup(dumpsLoader *DumpsLoaderHandleT, db, toDB *jobsdb.HandleT, tablePrefix string) {
	handle.db = db
	handle.toDB = toDB
	handle.bucket = strings.TrimSpace(config.GetEnv("S3_DUMPS_BUCKET", ""))
	if handle.bucket == "" {
		panic("Bucket is not configured.")
	}
	handle.noOfWorkers = config.GetEnvAsInt("WORKERS_PER_SOURCE", 4)
	handle.dumpsLoader = dumpsLoader
	handle.dbReadSize = config.GetEnvAsInt("DB_READ_SIZE", 10)
	handle.tablePrefix = tablePrefix

	go handle.initSourceWorkers()
	// sleep till workers are setup
	time.Sleep(5 * time.Second)
	go handle.generatorLoop()
}
