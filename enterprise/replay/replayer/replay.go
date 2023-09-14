package replayer

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/replay/dumpsloader"
	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

type Replay interface {
	Start()
	Stop() error
}

type Replayer struct {
	ctx                      context.Context
	cancel                   context.CancelFunc
	g                        errgroup.Group
	log                      logger.Logger
	db                       *jobsdb.Handle
	toDB                     *jobsdb.Handle
	dumpsLoader              dumpsloader.DumpsLoader
	uploader                 filemanager.FileManager
	config                   replayConfig
	workers                  []*SourceWorkerT
	initSourceWorkersChannel chan bool
}

type replayConfig struct {
	bucket      string
	startTime   time.Time
	endTime     time.Time
	noOfWorkers int
	dbReadSize  int
	tablePrefix string
}

func (handle *Replayer) generatorLoop(ctx context.Context) error {
	handle.log.Infof("generator reading from replay_jobs_* started")
	var breakLoop bool
	for {
		select {
		case <-ctx.Done():
			handle.log.Infof("generator reading from replay_jobs_* stopped:Context cancelled")
			return nil
		case <-handle.initSourceWorkersChannel:
		}

		queryParams := jobsdb.GetQueryParams{
			CustomValFilters: []string{"replay"},
			JobsLimit:        handle.config.dbReadSize,
		}
		jobsResult, err := handle.db.GetJobs(context.TODO(), []string{jobsdb.Unprocessed.State, jobsdb.Failed.State}, queryParams)
		if err != nil {
			handle.log.Errorf("Error getting to retry jobs: %v", err)
			return err
		}
		combinedList := jobsResult.Jobs

		handle.log.Infof("length of combinedList : %d", len(combinedList))

		if len(combinedList) == 0 {
			if breakLoop {
				executingList, err := handle.db.GetJobs(
					context.TODO(),
					[]string{jobsdb.Executing.State},
					jobsdb.GetQueryParams{
						CustomValFilters: []string{"replay"},
						JobsLimit:        handle.config.dbReadSize,
					},
				)
				if err != nil {
					handle.log.Errorf("Error getting executing jobs: %v", err)
					return err
				}
				handle.log.Infof("breakLoop is set. Pending executing: %d", len(executingList.Jobs))
				if len(executingList.Jobs) == 0 {
					break
				}
			}

			if handle.dumpsLoader.IsDone() {
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
			w := handle.workers[rand.Intn(handle.config.noOfWorkers)]
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
				Parameters:    []byte(`{}`), // check
				JobParameters: job.Parameters,
			}
			statusList = append(statusList, &status)
			toProcess = append(toProcess, workerJobT{worker: w, job: job})
		}

		// Mark the jobs as executing
		err = handle.db.UpdateJobStatus(ctx, statusList, []string{"replay"}, nil)
		if err != nil {
			return err
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
	return nil
}

func (handle *Replayer) initSourceWorkers(ctx context.Context) {
	handle.workers = make([]*SourceWorkerT, handle.config.noOfWorkers)
	for i := 0; i < handle.config.noOfWorkers; i++ {
		worker := &SourceWorkerT{
			log:           handle.log,
			channel:       make(chan *jobsdb.JobT, handle.config.dbReadSize),
			workerID:      i,
			replayHandler: handle,
			tablePrefix:   handle.config.tablePrefix,
			uploader:      handle.uploader,
		}
		handle.workers[i] = worker
		worker.transformer = transformer.NewTransformer(config.Default, handle.log, stats.Default)
		handle.g.Go(func() error {
			err := worker.workerProcess(ctx)
			if err != nil {
				return err
			}
			return nil
		})
	}
	close(handle.initSourceWorkersChannel)
}

func Setup(ctx context.Context, config *config.Config, dumpsLoader dumpsloader.DumpsLoader, db, toDB *jobsdb.Handle, tablePrefix string, uploader filemanager.FileManager, bucket string, log logger.Logger) (Replay, error) {
	ctx, cancel := context.WithCancel(ctx)
	handle := &Replayer{
		ctx:         ctx,
		cancel:      cancel,
		g:           errgroup.Group{},
		log:         log,
		db:          db,
		toDB:        toDB,
		dumpsLoader: dumpsLoader,
		uploader:    uploader,
		config: replayConfig{
			bucket:      bucket,
			tablePrefix: tablePrefix,
			noOfWorkers: config.GetInt("WORKERS_PER_SOURCE", 4),
			dbReadSize:  config.GetInt("DB_READ_SIZE", 10),
		},
	}
	handle.initSourceWorkersChannel = make(chan bool)
	startTime, endTime, err := utils.GetStartAndEndTime(config)
	if err != nil {
		return nil, err
	}
	handle.config.startTime = startTime
	handle.config.endTime = endTime
	return handle, nil
}

func (handle *Replayer) Start() {
	handle.g.Go(func() error {
		handle.initSourceWorkers(handle.ctx)
		return nil
	})
	handle.g.Go(func() error {
		return handle.generatorLoop(handle.ctx)
	})
}

func (handle *Replayer) Stop() error {
	handle.cancel()
	return handle.g.Wait()
}
