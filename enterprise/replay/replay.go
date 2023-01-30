package replay

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type Handler struct {
	log                      logger.Logger
	bucket                   string
	db                       *jobsdb.HandleT
	toDB                     *jobsdb.HandleT
	noOfWorkers              int
	workers                  []*SourceWorkerT
	dumpsLoader              *dumpsLoaderHandleT
	dbReadSize               int
	tablePrefix              string
	uploader                 filemanager.FileManager
	initSourceWorkersChannel chan bool
}

func (handle *Handler) generatorLoop(ctx context.Context) {
	handle.log.Infof("generator reading from replay_jobs_* started")
	var breakLoop bool
	select {
	case <-ctx.Done():
		handle.log.Infof("generator reading from replay_jobs_* stopped:Context cancelled")
		return
	case <-handle.initSourceWorkersChannel:
	}
	for {
		queryParams := jobsdb.GetQueryParamsT{
			CustomValFilters: []string{"replay"},
			JobsLimit:        handle.dbReadSize,
		}
		toRetry, err := handle.db.GetToRetry(context.TODO(), queryParams)
		if err != nil {
			handle.log.Errorf("Error getting to retry jobs: %v", err)
			panic(err)
		}
		combinedList := toRetry.Jobs

		if !toRetry.LimitsReached {
			queryParams.JobsLimit -= len(combinedList)
			unprocessed, err := handle.db.GetUnprocessed(context.TODO(), queryParams)
			if err != nil {
				handle.log.Errorf("Error getting unprocessed jobs: %v", err)
				panic(err)
			}
			combinedList = append(combinedList, unprocessed.Jobs...)
		}
		handle.log.Infof("length of combinedList : %d", len(combinedList))

		if len(combinedList) == 0 {
			if breakLoop {
				executingList, err := handle.db.GetExecuting(
					context.TODO(),
					jobsdb.GetQueryParamsT{
						CustomValFilters: []string{"replay"},
						JobsLimit:        handle.dbReadSize,
					},
				)
				if err != nil {
					handle.log.Errorf("Error getting executing jobs: %v", err)
					panic(err)
				}
				handle.log.Infof("breakLoop is set. Pending executing: %d", len(executingList.Jobs))
				if len(executingList.Jobs) == 0 {
					break
				}
			}

			if handle.dumpsLoader.done {
				breakLoop = true
			}

			handle.log.Debugf("DB Read Complete. No Jobs to process")
			time.Sleep(5 * time.Second)
			continue
		}

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		// List of jobs which can be processed mapped per channel
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
		err = handle.db.UpdateJobStatus(ctx, statusList, []string{"replay"}, nil)
		if err != nil {
			panic(err)
		}

		// Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.channel <- wrkJob.job
		}
	}

	// Since generator read is done, closing worker channels
	for _, worker := range handle.workers {
		handle.log.Infof("Closing worker channels")
		close(worker.channel)
	}
}

func (handle *Handler) initSourceWorkers(ctx context.Context) {
	handle.workers = make([]*SourceWorkerT, handle.noOfWorkers)
	for i := 0; i < handle.noOfWorkers; i++ {
		worker := &SourceWorkerT{
			log:           handle.log,
			channel:       make(chan *jobsdb.JobT, handle.dbReadSize),
			workerID:      i,
			replayHandler: handle,
			tablePrefix:   handle.tablePrefix,
			uploader:      handle.uploader,
		}
		handle.workers[i] = worker
		worker.transformer = transformer.NewTransformer()
		worker.transformer.Setup()
		go worker.workerProcess(ctx)
	}
	handle.initSourceWorkersChannel <- true
}

func (handle *Handler) Setup(ctx context.Context, dumpsLoader *dumpsLoaderHandleT, db, toDB *jobsdb.HandleT, tablePrefix string, uploader filemanager.FileManager, bucket string, log logger.Logger) {
	handle.log = log
	handle.db = db
	handle.toDB = toDB
	handle.bucket = bucket
	handle.uploader = uploader
	handle.noOfWorkers = config.GetInt("WORKERS_PER_SOURCE", 4)
	handle.dumpsLoader = dumpsLoader
	handle.dbReadSize = config.GetInt("DB_READ_SIZE", 10)
	handle.tablePrefix = tablePrefix
	handle.initSourceWorkersChannel = make(chan bool)

	go handle.initSourceWorkers(ctx)
	go handle.generatorLoop(ctx)
}
