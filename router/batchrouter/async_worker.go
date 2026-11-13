package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// asyncWorker serves a single async destination. It does all the work of its destination itself, one step at a time:
//
//	start ──► find imports in progress (jobsdb) ──┐
//	                                              ▼
//	        ┌──────────────► import in progress? ──yes──► poll when due ──► import finished? ──yes──► write statuses ─┐
//	        │                     │ no                                          │ no                                │
//	        │                     ▼                                             └──────────► wait for the next poll   │
//	        │        batch full or old enough? ──yes──► upload ──► import started? ──no──► write statuses ───────────┤
//	        │                     │ no                                 │ yes                                          │
//	        │                     ▼                                    └──► the batch's jobs become the import ───────┤
//	        │        fetch the jobs that the batch can still take                                                    │
//	        └────────────────────────────────────────────────────────────────────────────────────────────────────────┘
//
// So a destination has at most one import in progress, its batches go out in job ID order, and its manager gets one
// call at a time. The number of workers of a destination type that upload, or poll, at the same time is limited.
// jobsdb is the source of truth: after a restart, the worker finds its imports in jobsdb and fetches its batch again.
type asyncWorker struct {
	ctx           context.Context
	destinationID string
	logger        logger.Logger
	brt           *Handle
	cb            circuitbreaker.CircuitBreaker

	discovered       bool           // the imports in progress have been loaded from jobsdb
	rediscover       bool           // more importing jobs may exist than the last discovery loaded
	imports          []*asyncImport // imports in progress, oldest first
	batch            *asyncBatch    // nil while the worker holds no jobs to upload
	moreJobs         bool           // the last fetch hit the limits of a jobsdb query, so more jobs are waiting
	nextFetch        time.Time      // while the batch fills slowly, the worker doesn't fetch before this time
	fetchBackoff     misc.ExponentialNumber[time.Duration]
	partFileNumber   int    // number of the last part uploaded for sourceJobRunID (SFTP)
	sourceJobRunID   string // source job run of the last batch
	uploadCauseStats map[string]stats.Measurement
}

// asyncBatch holds the jobs that a worker has taken but not uploaded yet. Its jobs are executing, and each one is a
// line in the upload file.
type asyncBatch struct {
	jobs      []*jobsdb.JobT
	fileName  string
	size      int
	full      bool
	createdAt time.Time
	uploadURL string
}

// asyncImport is an import in progress at the destination
type asyncImport struct {
	parameters stdjson.RawMessage // status parameters of the import's first job: the import ID and count
	jobs       []*jobsdb.JobT     // the importing jobs of the import, in job ID order
	nextPoll   time.Time
	polls      int
	startedAt  time.Time
	tracked    func() // stops tracking the import's age
}

func newAsyncWorker(ctx context.Context, destinationID string, log logger.Logger, brt *Handle) *asyncWorker {
	log = log.Withn(obskit.DestinationID(destinationID))
	return &asyncWorker{
		ctx:           ctx,
		destinationID: destinationID,
		logger:        log,
		brt:           brt,
		cb:            brt.newCircuitBreaker(destinationID, log),
		uploadCauseStats: map[string]stats.Measurement{
			"full": brt.asyncWorkerStat("async_worker_uploads", stats.CountType, destinationID, stats.Tags{"cause": "full"}),
			"age":  brt.asyncWorkerStat("async_worker_uploads", stats.CountType, destinationID, stats.Tags{"cause": "age"}),
		},
	}
}

// Work does the first step that applies. It reports work while the worker holds a batch or an import, so that the
// worker pool keeps the worker, and its state, until the batch and the import are done.
func (w *asyncWorker) Work() bool {
	if w.ctx.Err() != nil {
		return false
	}
	if !w.discovered {
		w.discoverImports()
	}
	if len(w.imports) > 0 {
		w.pollImport()
		return true
	}
	if w.rediscover {
		w.discovered = false
		return true
	}
	if w.cb.IsOpen() {
		return w.batch != nil
	}
	if w.batch != nil && w.batchReady() {
		w.uploadBatch()
		return true
	}
	if w.batch != nil && time.Now().Before(w.nextFetch) {
		return true
	}
	fetched := w.fetch()
	if w.batch != nil && !fetched {
		w.nextFetch = time.Now().Add(w.fetchBackoff.Next(w.SleepDurations()))
	} else {
		w.fetchBackoff.Reset()
	}
	return fetched || w.batch != nil || w.moreJobs
}

// SleepDurations returns the min and max sleep durations for the worker when idle, i.e when [Work] returns false.
func (w *asyncWorker) SleepDurations() (min, max time.Duration) {
	return w.brt.minIdleSleep.Load(), w.brt.uploadFreq.Load() / 2
}

// Stop drops the worker's batch. Its jobs stay executing, and the next start resets them.
func (w *asyncWorker) Stop() {
	if w.batch != nil {
		misc.RemoveFilePaths(w.batch.fileName)
	}
	for _, imp := range w.imports {
		imp.tracked()
	}
}

// discoverImports loads the importing jobs of the destination and groups them by import, oldest import first.
func (w *asyncWorker) discoverImports() {
	brt := w.brt
	limit := 2 * brt.maxEventsInABatch
	res, err := brt.getImportingJobs(w.ctx, func(gqp *jobsdb.GetQueryParams) {
		gqp.ParameterFilters = []jobsdb.ParameterFilterT{{Name: "destination_id", Value: w.destinationID}}
	}, limit)
	if err != nil {
		if w.ctx.Err() != nil {
			return
		}
		panic(fmt.Errorf("finding the imports in progress of %s destination %s: %w", brt.destType, w.destinationID, err))
	}
	groups := lo.GroupBy(res.Jobs, func(job *jobsdb.JobT) string {
		return gjson.GetBytes(job.LastJobStatus.Parameters, "importId").Raw
	})
	for _, jobs := range groups {
		slices.SortFunc(jobs, func(a, b *jobsdb.JobT) int { return int(a.JobID - b.JobID) })
		startedAt := lo.MinBy(jobs, func(a, b *jobsdb.JobT) bool { return a.LastJobStatus.ExecTime.Before(b.LastJobStatus.ExecTime) }).LastJobStatus.ExecTime
		w.imports = append(w.imports, &asyncImport{
			parameters: jobs[0].LastJobStatus.Parameters,
			jobs:       jobs,
			startedAt:  startedAt,
			tracked:    brt.asyncCalls.start(asyncCallImport, startedAt),
		})
	}
	slices.SortFunc(w.imports, func(a, b *asyncImport) int { return int(a.jobs[0].JobID - b.jobs[0].JobID) })
	w.discovered = true
	// a truncated discovery may have split an import: its remaining jobs are loaded after the loaded imports finish
	w.rediscover = len(res.Jobs) >= limit
	if len(w.imports) > 0 {
		w.logger.Infon("Found imports in progress",
			logger.NewIntField("imports", int64(len(w.imports))),
			logger.NewIntField("jobs", int64(len(res.Jobs))),
		)
	}
}

// pollImport polls the oldest import in progress if its poll is due, and writes the statuses of its jobs if it finished.
func (w *asyncWorker) pollImport() {
	brt := w.brt
	imp := w.imports[0]
	if time.Now().Before(imp.nextPoll) {
		return
	}
	m, ok := w.manager()
	if !ok {
		return
	}
	defer brt.limiter.asyncPoll.Begin(w.destinationID)()
	if w.ctx.Err() != nil {
		return
	}
	pollInput := asynccommon.AsyncPoll{
		ImportId:    gjson.GetBytes(imp.parameters, "importId").String(),
		ImportCount: int(gjson.GetBytes(imp.parameters, "importCount").Int()),
	}
	start := time.Now()
	done := brt.asyncCalls.start(asyncCallPoll, start)
	pollResp := m.manager.Poll(w.ctx, pollInput)
	done()
	brt.asyncPollTimeStat.Since(start)
	imp.polls++
	if w.ctx.Err() != nil {
		// a poll is safe to repeat: the next start polls the import again
		return
	}
	if pollResp.InProgress {
		imp.nextPoll = time.Now().Add(brt.asyncPollInterval.Load())
		return
	}
	sourceID := gjson.GetBytes(imp.jobs[0].Parameters, "source_id").String()
	statusList, err := brt.updateImportStatuses(w.ctx, updateImportStatusesParams{
		manager:          m.manager,
		destinationID:    w.destinationID,
		sourceID:         sourceID,
		importParameters: imp.parameters,
		importingList:    imp.jobs,
		pollResp:         pollResp,
		failMissing:      true,
	})
	if err != nil {
		w.logger.Errorn("Writing the statuses of a finished import, polling it again", obskit.Error(err))
		imp.nextPoll = time.Now().Add(brt.asyncPollInterval.Load())
		return
	}
	brt.recordAsyncDestinationDeliveryStatus(sourceID, w.destinationID, statusList)
	w.recordOutcome(statusList, len(imp.jobs))
	brt.asyncWorkerStat("async_worker_import_duration", stats.TimerType, w.destinationID, nil).Since(imp.startedAt)
	brt.asyncWorkerStat("async_worker_polls_per_import", stats.HistogramType, w.destinationID, nil).Observe(float64(imp.polls))
	imp.tracked()
	w.imports = w.imports[1:]
}

// batchReady returns true if the batch is full or older than the destination's upload interval
func (w *asyncWorker) batchReady() bool {
	if w.batch.full {
		w.uploadCauseStats["full"].Increment()
		return true
	}
	w.brt.configSubscriberMu.RLock()
	interval, ok := w.brt.uploadIntervalMap[w.destinationID]
	w.brt.configSubscriberMu.RUnlock()
	if !ok {
		interval = w.brt.asyncUploadTimeout.Load()
	}
	if time.Since(w.batch.createdAt) >= interval {
		w.uploadCauseStats["age"].Increment()
		return true
	}
	return false
}

// uploadBatch uploads the batch and writes the statuses of its jobs: importing, or final.
func (w *asyncWorker) uploadBatch() {
	brt := w.brt
	m, ok := w.manager()
	if !ok {
		return
	}
	defer brt.limiter.asyncUpload.Begin(w.destinationID)()
	if w.ctx.Err() != nil {
		return
	}
	batch := w.batch
	metadata := newAsyncJobMetadata(batch.jobs)
	jobIDs := lo.Map(batch.jobs, func(job *jobsdb.JobT, _ int) int64 { return job.JobID })
	w.partFileNumber++
	start := time.Now()
	brt.asyncWorkerStat("async_worker_batch_jobs", stats.HistogramType, w.destinationID, nil).Observe(float64(len(batch.jobs)))
	done := brt.asyncCalls.start(asyncCallUpload, start)
	// an upload isn't safe to repeat, so it continues during shutdown until its statuses are written
	out := m.manager.Upload(context.WithoutCancel(w.ctx), &asynccommon.AsyncDestinationStruct{
		ImportingJobIDs:      jobIDs,
		FailedJobIDs:         []int64{},
		Exists:               true,
		Size:                 batch.size,
		CreatedAt:            batch.createdAt,
		FileName:             batch.fileName,
		Count:                len(batch.jobs),
		CanUpload:            true,
		DestinationUploadURL: batch.uploadURL,
		Destination:          m.destination,
		Manager:              m.manager,
		PartFileNumber:       w.partFileNumber,
		SourceJobRunID:       w.sourceJobRunID,
		AttemptNums:          metadata.AttemptNums,
		FirstAttemptedAts:    metadata.FirstAttemptedAts,
		PartitionIDs:         metadata.PartitionIDs,
		JobParameters:        metadata.JobParameters,
	})
	done()
	brt.asyncWorkerStat("async_worker_upload_duration", stats.TimerType, w.destinationID, nil).Since(start)
	out.DestinationID = w.destinationID
	statusList := brt.setMultipleJobStatus(setMultipleJobStatusParams{
		asyncJobMetadata: metadata,
		AsyncOutput:      out,
		Attempted:        true,
	})

	reported := lo.Keyify(slices.Concat(out.ImportingJobIDs, out.SucceededJobIDs, out.FailedJobIDs, out.AbortJobIDs))
	if missing := lo.Filter(jobIDs, func(id int64, _ int) bool { _, ok := reported[id]; return !ok }); len(missing) > 0 {
		w.logger.Errorn("Async Destination Bug: jobs missing from the outcome of the upload, marking them as failed",
			logger.NewIntField("missingJobs", int64(len(missing))),
			logger.NewIntField("minMissingJobID", lo.Min(missing)),
			logger.NewIntField("maxMissingJobID", lo.Max(missing)),
		)
		statusList = append(statusList, brt.setMultipleJobStatus(setMultipleJobStatusParams{
			asyncJobMetadata: metadata,
			AsyncOutput: asynccommon.AsyncUploadOutput{
				DestinationID: w.destinationID,
				FailedJobIDs:  missing,
				FailedReason:  "job missing from the outcome of the upload",
			},
			Attempted: true,
		})...)
	}

	misc.RemoveFilePaths(batch.fileName)
	w.batch = nil
	if out.ImportingParameters != nil && len(out.ImportingJobIDs) > 0 {
		w.startImport(batch.jobs, statusList, start)
		return
	}
	w.recordOutcome(statusList, len(batch.jobs))
}

// startImport keeps the importing jobs of an upload in memory, with the statuses that were written for them, as jobsdb
// would return them to a poll.
func (w *asyncWorker) startImport(jobs []*jobsdb.JobT, statusList []*jobsdb.JobStatusT, startedAt time.Time) {
	importing := lo.SliceToMap(lo.Filter(statusList, func(s *jobsdb.JobStatusT, _ int) bool {
		return s.JobState == jobsdb.Importing.State
	}), func(s *jobsdb.JobStatusT) (int64, *jobsdb.JobStatusT) { return s.JobID, s })
	var importJobs []*jobsdb.JobT
	for _, job := range jobs {
		status, ok := importing[job.JobID]
		if !ok {
			continue
		}
		job.LastJobStatus = jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      status.JobState,
			AttemptNum:    status.AttemptNum,
			ExecTime:      status.ExecTime,
			RetryTime:     status.RetryTime,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
			Parameters:    status.Parameters,
			JobParameters: status.JobParameters,
			WorkspaceId:   status.WorkspaceId,
			PartitionID:   status.PartitionID,
			CustomVal:     status.CustomVal,
		}
		importJobs = append(importJobs, job)
	}
	if len(importJobs) == 0 {
		return
	}
	w.imports = append(w.imports, &asyncImport{
		parameters: importJobs[0].LastJobStatus.Parameters,
		jobs:       importJobs,
		nextPoll:   time.Now().Add(w.brt.asyncPollInterval.Load()),
		startedAt:  startedAt,
		tracked:    w.brt.asyncCalls.start(asyncCallImport, startedAt),
	})
}

// recordOutcome counts an upload that left no job importing, or a finished import, in the circuit breaker: a failure
// if all of its jobs failed and will be retried, a success otherwise.
func (w *asyncWorker) recordOutcome(statusList []*jobsdb.JobStatusT, jobs int) {
	failed := lo.CountBy(statusList, func(s *jobsdb.JobStatusT) bool { return s.JobState == jobsdb.Failed.State })
	if jobs > 0 && failed == jobs {
		w.cb.Failure()
		return
	}
	w.cb.Success()
}

// fetch fetches the jobs that the batch can still take, adds them to the batch in job ID order and marks them
// executing. It returns true if it fetched any job.
func (w *asyncWorker) fetch() bool {
	brt := w.brt
	m, ok := w.manager()
	if !ok {
		return false
	}
	brt.configSubscriberMu.RLock()
	destWithSources, ok := brt.destinationsMap[w.destinationID]
	brt.configSubscriberMu.RUnlock()
	if !ok {
		return false
	}

	var count, size int
	if w.batch != nil {
		count, size = len(w.batch.jobs), w.batch.size
	}
	jobsLimit := brt.jobQueryBatchSize.Load()
	if !asynccommon.IsSFTPDestination(brt.destType) {
		jobsLimit = min(jobsLimit, brt.maxEventsInABatch-count)
	}
	payloadLimit := min(brt.adaptiveLimit(brt.payloadLimit.Load()), int64(brt.maxPayloadSizeInBytes-size))
	if jobsLimit <= 0 || payloadLimit <= 0 {
		w.batch.full = true
		return false
	}
	jobs, limitsReached := w.getJobs(jobsLimit, payloadLimit)
	w.moreJobs = limitsReached
	if len(jobs) == 0 {
		return false
	}

	if brt.disableEgress {
		brt.setMultipleJobStatus(setMultipleJobStatusParams{
			asyncJobMetadata: newAsyncJobMetadata(jobs),
			AsyncOutput: asynccommon.AsyncUploadOutput{
				DestinationID:   w.destinationID,
				SucceededJobIDs: lo.Map(jobs, func(job *jobsdb.JobT, _ int) int64 { return job.JobID }),
				SuccessResponse: fmt.Sprintf(`{"error":%q}`, rterror.ErrDisabledEgress.Error()),
			},
			JobsList: jobs,
		})
		return true
	}

	jobs = brt.abortDrainedJobs(destWithSources, jobs)
	if invalidManager, ok := m.manager.(*asynccommon.InvalidManager); ok {
		w.abortJobs(jobs, invalidManager.Error.Error())
		w.cb.Failure()
		return true
	}
	w.addToBatch(m.manager, jobs)
	return true
}

// getJobs fetches failed and unprocessed jobs of the destination in job ID order, and returns whether the query hit
// its limits.
func (w *asyncWorker) getJobs(jobsLimit int, payloadLimit int64) ([]*jobsdb.JobT, bool) {
	brt := w.brt
	defer brt.limiter.read.Begin("")()
	queryParams := jobsdb.GetQueryParams{
		CustomValFilters: []string{brt.destType},
		ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: w.destinationID}},
		JobsLimit:        jobsLimit,
		PayloadSizeLimit: payloadLimit,
	}
	queryStart := time.Now()
	var res jobsdb.JobsResult
	for query := true; query; { // keep trying while no jobs are returned because ds limits are being reached
		var err error
		res, err = misc.QueryWithRetriesAndNotify(w.ctx, brt.jobdDBQueryRequestTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
			return brt.jobsDB.GetJobs(ctx, []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, queryParams)
		}, brt.sendQueryRetryStats)
		if err != nil {
			if w.ctx.Err() != nil {
				return nil, false
			}
			panic(fmt.Errorf("fetching jobs of %s destination %s: %w", brt.destType, w.destinationID, err))
		}
		query = len(res.Jobs) == 0 && res.DSLimitsReached
	}
	stats.Default.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, stats.Tags{"function": "getJobs", "destType": brt.destType, "partition": w.destinationID}).Since(queryStart)
	slices.SortFunc(res.Jobs, func(a, b *jobsdb.JobT) int { return int(a.JobID - b.JobID) })
	return res.Jobs, res.LimitsReached
}

// addToBatch adds jobs to the batch until one doesn't fit, and marks the added jobs executing. The jobs after it keep
// their status, and a later fetch reads them again. A batch always takes its first job, even if the job is larger
// than the batch limit, so that one large job never stops a destination.
func (w *asyncWorker) addToBatch(manager asynccommon.AsyncDestinationManager, jobs []*jobsdb.JobT) {
	brt := w.brt
	var added []*jobsdb.JobT
	var file *os.File
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
	for _, job := range jobs {
		line, err := manager.Transform(job)
		if err != nil {
			w.abortJobs([]*jobsdb.JobT{job}, err.Error())
			continue
		}
		line += "\n"
		if w.batch != nil && len(w.batch.jobs) > 0 && !w.fits(len(line)) {
			w.batch.full = true
			break
		}
		if w.batch == nil {
			w.batch = w.newBatch(job)
		}
		if file == nil {
			if file, err = os.OpenFile(w.batch.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil {
				panic(fmt.Errorf("opening the upload file of %s destination %s: %w", brt.destType, w.destinationID, err))
			}
		}
		if _, err := file.WriteString(line); err != nil {
			panic(fmt.Errorf("writing the upload file of %s destination %s: %w", brt.destType, w.destinationID, err))
		}
		w.batch.jobs = append(w.batch.jobs, job)
		w.batch.size += len(line)
		w.batch.uploadURL = gjson.GetBytes(job.EventPayload, "endpoint").String()
		added = append(added, job)
	}
	if w.batch != nil && !w.fits(0) {
		w.batch.full = true
	}
	if len(added) == 0 {
		return
	}
	statusList := lo.Map(added, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT { return executingStatus(job) })
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.UpdateJobStatus(ctx, statusList)
	}, brt.sendRetryUpdateStats)
	if err != nil {
		panic(fmt.Errorf("updating %s job statuses: %w", brt.destType, err))
	}
}

// fits returns true if the batch can take a line of the given size: SFTP batches are limited only by bytes
func (w *asyncWorker) fits(lineSize int) bool {
	if w.batch.size+lineSize > w.brt.maxPayloadSizeInBytes {
		return false
	}
	return asynccommon.IsSFTPDestination(w.brt.destType) || len(w.batch.jobs) < w.brt.maxEventsInABatch
}

func (w *asyncWorker) newBatch(firstJob *jobsdb.JobT) *asyncBatch {
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		panic(err)
	}
	sourceID := gjson.GetBytes(firstJob.Parameters, "source_id").String()
	fileName := filepath.Join(tmpDirPath, misc.RudderAsyncDestinationLogs, fmt.Sprintf("%s.%s.txt", sourceID, uuid.New().String()))
	if err := os.MkdirAll(filepath.Dir(fileName), os.ModePerm); err != nil {
		panic(err)
	}
	if jobRunID := gjson.GetBytes(firstJob.Parameters, "source_job_run_id").String(); jobRunID != w.sourceJobRunID {
		w.sourceJobRunID = jobRunID
		w.partFileNumber = 0
	}
	return &asyncBatch{fileName: fileName, createdAt: w.brt.now()}
}

// abortJobs aborts jobs that the worker must not deliver
func (w *asyncWorker) abortJobs(jobs []*jobsdb.JobT, reason string) {
	if len(jobs) == 0 {
		return
	}
	w.brt.setMultipleJobStatus(setMultipleJobStatusParams{
		asyncJobMetadata: newAsyncJobMetadata(jobs),
		AsyncOutput: asynccommon.AsyncUploadOutput{
			DestinationID: w.destinationID,
			AbortJobIDs:   lo.Map(jobs, func(job *jobsdb.JobT, _ int) int64 { return job.JobID }),
			AbortReason:   reason,
		},
		JobsList: jobs,
	})
}

// manager returns the manager of the destination. Workers only read the managers, which the config subscriber
// replaces as a whole.
func (w *asyncWorker) manager() (asyncManager, bool) {
	w.brt.configSubscriberMu.RLock()
	defer w.brt.configSubscriberMu.RUnlock()
	m, ok := w.brt.asyncManagers[w.destinationID]
	return m, ok
}

func (brt *Handle) asyncWorkerStat(name, statType, destinationID string, tags stats.Tags) stats.Measurement {
	allTags := stats.Tags{"module": "batch_router", "destType": brt.destType, "destId": destinationID}
	maps.Copy(allTags, tags)
	return stats.Default.NewTaggedStat(name, statType, allTags)
}
