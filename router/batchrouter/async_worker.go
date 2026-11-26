package batchrouter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// asyncWorker serves a single async destination. It does all the work of its destination itself, one step at a time:
//
//	start ──► find imports in progress (jobsdb) ──┐
//	                                              ▼
//	        ┌──────────────► import in progress? ──yes──► poll when due ──► import finished? ──yes──► write statuses ─┐
//	        │                     │ no                                          │ no                                  │
//	        │                     ▼                                             └──────────► wait for the next poll   │
//	        │        fetch the jobs that the batch can still take, unless it is full                                  │
//	        │                     │                                                                                   │
//	        │                     ▼                                                                                   │
//	        │        batch full or old enough? ──yes──► upload ──► import started? ──no──► write statuses ────────────┤
//	        │                     │ no                                 │ yes                                          │
//	        │                     ▼                                    └──► the batch's jobs become the import ───────┤
//	        │        wait for the next ping                                                                           │
//	        └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
//
// So a worker uploads only while no import is in progress, its batches go out in job ID order, and its manager gets one
// call at a time. The number of workers of a destination type that upload, or poll, at the same time is limited.
// jobsdb is the source of truth: after a restart, the worker finds its imports in jobsdb and fetches its batch again.
type asyncWorker struct {
	ctx           context.Context
	destinationID string
	logger        logger.Logger
	brt           *Handle
	cb            circuitbreaker.CircuitBreaker

	importsLoaded  bool           // whether imports has been loaded from jobsdb
	imports        []*asyncImport // imports in progress, oldest first
	batch          *asyncBatch    // the batch that is filling
	backlog        bool           // whether the last fetch left jobs behind
	partFileNumber int            // number of the last part uploaded for sourceJobRunID (SFTP)
	sourceJobRunID string         // source job run of the last batch

	stats struct {
		uploadsFull    stats.Counter
		uploadsAge     stats.Counter
		batchJobs      stats.Histogram
		pollsPerImport stats.Histogram
	}
}

// asyncWorkerSupplier returns a supplier that gives each destination the same worker every time. The worker pool stops
// workers that are idle, but a destination's batch, imports and part numbers must survive that.
func (brt *Handle) asyncWorkerSupplier(ctx context.Context) workerpool.WorkerSupplier {
	workers := map[string]*asyncWorker{}
	return func(destinationID string) workerpool.Worker {
		if _, ok := workers[destinationID]; !ok {
			workers[destinationID] = newAsyncWorker(ctx, destinationID, brt)
		}
		return workers[destinationID]
	}
}

func newAsyncWorker(ctx context.Context, destinationID string, brt *Handle) *asyncWorker {
	log := brt.logger.Withn(obskit.DestinationID(destinationID))
	w := &asyncWorker{
		ctx:           ctx,
		destinationID: destinationID,
		logger:        log,
		brt:           brt,
		cb:            brt.newCircuitBreaker(destinationID, log),
	}
	w.batch = w.newBatch()
	tags := brt.asyncDestinationTags(destinationID)
	w.stats.uploadsFull = stats.Default.NewTaggedStat("async_worker_uploads", stats.CountType, lo.Assign(tags, stats.Tags{"cause": "full"}))
	w.stats.uploadsAge = stats.Default.NewTaggedStat("async_worker_uploads", stats.CountType, lo.Assign(tags, stats.Tags{"cause": "age"}))
	w.stats.batchJobs = stats.Default.NewTaggedStat("async_worker_batch_jobs", stats.HistogramType, tags)
	w.stats.pollsPerImport = stats.Default.NewTaggedStat("async_worker_polls_per_import", stats.HistogramType, tags)
	return w
}

// asyncStep is the result of a worker's step
type asyncStep int

const (
	stepIdle asyncStep = iota // there was nothing to do yet
	stepDone                  // the next step can wait for a ping
	stepMore                  // the next step is due at once
)

// Work does steps until the next one can wait, and returns false if there was nothing to do yet.
func (w *asyncWorker) Work() bool {
	step := w.step()
	worked := step != stepIdle
	for step == stepMore {
		step = w.step()
	}
	return worked
}

// step polls an import in progress if there is one. Otherwise it fetches what the batch can still take, so that a batch
// that is old enough also takes the jobs that arrived while the worker slept, and uploads the batch if it is ready.
func (w *asyncWorker) step() asyncStep {
	if w.ctx.Err() != nil {
		return stepIdle
	}
	dest, ok := w.brt.asyncDestination(w.destinationID)
	if !ok {
		return stepIdle
	}
	if !w.importsLoaded {
		if err := w.loadImports(); err != nil {
			return stepIdle
		}
	}
	if len(w.imports) > 0 {
		return w.pollImport(w.imports[0])
	}
	if w.cb.IsOpen() {
		return stepIdle
	}
	step := stepIdle
	if !w.batch.full { // a fetch without room would have no limits
		step = w.fetch()
	}
	if w.batch.ready(dest.uploadInterval) {
		return w.uploadBatch()
	}
	return step
}

// SleepDurations returns the min and max sleep durations for the worker when [Work] returns false. A worker doesn't
// sleep past its next poll or upload.
func (w *asyncWorker) SleepDurations() (minSleep, maxSleep time.Duration) {
	minSleep, maxSleep = w.brt.minIdleSleep.Load(), w.brt.uploadFreq.Load()/2
	if due := w.nextDue(); !due.IsZero() {
		maxSleep = min(maxSleep, max(0, time.Until(due)))
	}
	return min(minSleep, maxSleep), maxSleep
}

// nextDue returns the time of the next poll or upload, or zero if none is planned
func (w *asyncWorker) nextDue() time.Time {
	if len(w.imports) > 0 {
		return w.imports[0].nextPoll
	}
	if dest, ok := w.brt.asyncDestination(w.destinationID); ok && !w.batch.empty() && !w.cb.IsOpen() {
		return w.batch.createdAt.Add(dest.uploadInterval)
	}
	return time.Time{}
}

// Stop is a no-op: a worker that the pool stops while idle keeps its state. At shutdown, the jobs of its batch stay
// executing, and the next start resets them and removes the upload files.
func (w *asyncWorker) Stop() {}

// loadImports loads all importing jobs of the destination and groups them by import, oldest import first. It returns
// an error only if the worker is stopping.
func (w *asyncWorker) loadImports() error {
	brt := w.brt
	defer brt.asyncDestinationTimer("async_find_import_time", w.destinationID).RecordDuration()()
	res, err := brt.getImportingJobs(w.ctx, func(gqp *jobsdb.GetQueryParams) {
		gqp.ParameterFilters = []jobsdb.ParameterFilterT{{Name: "destination_id", Value: w.destinationID}}
	}, math.MaxInt)
	if err != nil {
		if w.ctx.Err() != nil {
			return err
		}
		panic(fmt.Errorf("finding the imports in progress of %s destination %s: %w", brt.destType, w.destinationID, err))
	}
	for _, imp := range asyncImportsOf(res.Jobs) {
		w.addImport(imp)
	}
	w.importsLoaded = true
	if len(w.imports) > 0 {
		w.logger.Infon("Found imports in progress",
			logger.NewIntField("imports", int64(len(w.imports))),
			logger.NewIntField("jobs", int64(len(res.Jobs))),
		)
	}
	return nil
}

func (w *asyncWorker) addImport(imp *asyncImport) {
	imp.tracked = w.brt.asyncCalls.start(w.destinationID, asyncCallImport, imp.startedAt)
	w.imports = append(w.imports, imp)
}

// pollImport polls an import in progress, and writes the statuses of its jobs if it finished.
func (w *asyncWorker) pollImport(imp *asyncImport) asyncStep {
	brt := w.brt
	if time.Now().Before(imp.nextPoll) {
		return stepIdle
	}
	defer brt.limiter.asyncPoll.Begin(w.destinationID)()
	if w.ctx.Err() != nil {
		return stepIdle
	}
	dest, ok := brt.asyncDestination(w.destinationID) // after the wait for a slot, during which the config can change
	if !ok {
		return stepIdle
	}
	defer func() { imp.nextPoll = time.Now().Add(brt.pollStatusLoopSleep.Load()) }()
	pollResp, err := w.poll(dest, imp)
	if w.ctx.Err() != nil {
		// a poll is safe to repeat: the next start polls the import again
		return stepIdle
	}
	if err != nil {
		w.logger.Errorn("Resolving the import ID of an import in progress, trying again later", obskit.Error(err))
		return stepDone
	}
	if pollResp.InProgress {
		return stepDone
	}
	sourceID := gjson.GetBytes(imp.jobs[0].Parameters, "source_id").String()
	statusList, err := brt.updateImportStatuses(w.ctx, updateImportStatusesParams{
		manager:          dest.manager,
		destinationID:    w.destinationID,
		sourceID:         sourceID,
		importParameters: imp.parameters,
		importingList:    imp.jobs,
		pollResp:         pollResp,
	})
	if err != nil {
		w.logger.Errorn("Writing the statuses of a finished import, polling it again", obskit.Error(err))
		return stepDone
	}
	brt.recordAsyncDestinationDeliveryStatus(sourceID, w.destinationID, statusList)
	statusList = append(statusList, w.failLeftOut(imp.jobs, statusList, false)...)
	brt.asyncDestinationTimer("async_import_duration", w.destinationID).Since(imp.startedAt)
	w.stats.pollsPerImport.Observe(float64(imp.polls))
	imp.tracked()
	w.imports = slices.Delete(w.imports, 0, 1)
	return w.recordOutcome(statusList)
}

// poll returns the manager's response for an import, or an error if the import ID can't be looked up right now.
func (w *asyncWorker) poll(dest *asyncDestination, imp *asyncImport) (asynccommon.PollStatusResponse, error) {
	brt := w.brt
	parameters, err := brt.resolveImportParameters(w.ctx, w.destinationID, imp.parameters)
	if errors.Is(err, errImportIDNotFound) {
		// The import cannot be polled without its import ID: failing its jobs lets them be uploaded again.
		return asynccommon.PollStatusResponse{StatusCode: http.StatusInternalServerError, Error: err.Error()}, nil
	}
	if err != nil {
		brt.asyncDestinationCounter("async_import_id_lookup_errors", w.destinationID).Increment()
		return asynccommon.PollStatusResponse{}, err
	}
	imp.parameters = parameters
	imp.polls++
	defer brt.asyncDestinationTimer("async_poll_time", w.destinationID).RecordDuration()()
	defer brt.asyncCalls.start(w.destinationID, asyncCallPoll, time.Now())()
	return dest.manager.Poll(w.ctx, getPollInput(imp.parameters)), nil
}

// uploadBatch uploads the batch and writes the statuses of its jobs: importing, or final.
func (w *asyncWorker) uploadBatch() asyncStep {
	brt := w.brt
	defer brt.limiter.asyncUpload.Begin(w.destinationID)()
	if w.ctx.Err() != nil {
		return stepIdle
	}
	dest, ok := brt.asyncDestination(w.destinationID) // after the wait for a slot, during which the config can change
	if !ok {
		return stepIdle
	}
	batch := w.batch
	if batch.full {
		w.stats.uploadsFull.Increment()
	} else {
		w.stats.uploadsAge.Increment()
	}
	w.stats.batchJobs.Observe(float64(len(batch.jobs)))
	brt.asyncDestinationTimer("async_upload_wait_time", w.destinationID).Since(batch.createdAt)

	out := w.upload(dest, batch)
	statusUpdateStart := time.Now()
	statusList := brt.setMultipleJobStatus(setMultipleJobStatusParams{
		asyncJobMetadata: newAsyncJobMetadata(batch.jobs),
		AsyncOutput:      out,
		Attempted:        true,
	})
	statusList = append(statusList, w.failLeftOut(batch.jobs, statusList, true)...)
	brt.asyncDestinationTimer("async_upload_status_update_time", w.destinationID).Since(statusUpdateStart)

	batch.removeFile()
	w.batch = w.newBatch()
	jobs := importingJobs(batch.jobs, statusList)
	if len(jobs) == 0 {
		return w.recordOutcome(statusList)
	}
	// the manager's own parameters: the statuses may only hold a reference to the import ID
	imp := newAsyncImport(out.ImportingParameters, jobs)
	imp.nextPoll = time.Now().Add(brt.pollStatusLoopSleep.Load())
	w.addImport(imp)
	return stepDone
}

func (w *asyncWorker) upload(dest *asyncDestination, batch *asyncBatch) asynccommon.AsyncUploadOutput {
	brt := w.brt
	if jobRunID := gjson.GetBytes(batch.jobs[0].Parameters, "source_job_run_id").String(); jobRunID != w.sourceJobRunID {
		w.sourceJobRunID = jobRunID
		w.partFileNumber = 0
	}
	w.partFileNumber++
	defer brt.asyncDestinationTimer("async_manager_upload_time", w.destinationID).RecordDuration()()
	defer brt.asyncCalls.start(w.destinationID, asyncCallUpload, time.Now())()
	// an upload isn't safe to repeat, so it continues during shutdown until its statuses are written
	out := dest.manager.Upload(context.WithoutCancel(w.ctx), &asynccommon.AsyncDestinationStruct{
		ImportingJobIDs: jobIDs(batch.jobs),
		Size:            batch.size,
		CreatedAt:       batch.createdAt,
		FileName:        batch.fileName,
		Count:           len(batch.jobs),
		Destination:     &dest.Destination,
		PartFileNumber:  w.partFileNumber,
		SourceJobRunID:  w.sourceJobRunID,
	})
	out.DestinationID = w.destinationID
	return out
}

// failLeftOut marks as failed the jobs that a manager left out of the outcome of an upload or an import, and returns
// their statuses.
func (w *asyncWorker) failLeftOut(jobs []*jobsdb.JobT, statusList []*jobsdb.JobStatusT, attempted bool) []*jobsdb.JobStatusT {
	reported := lo.SliceToMap(statusList, func(s *jobsdb.JobStatusT) (int64, struct{}) { return s.JobID, struct{}{} })
	leftOut := lo.Filter(jobs, func(job *jobsdb.JobT, _ int) bool { return !lo.HasKey(reported, job.JobID) })
	if len(leftOut) == 0 {
		return nil
	}
	w.logger.Errorn("Async Destination Bug: jobs left out of the outcome of an upload or an import, marking them as failed",
		logger.NewIntField("jobs", int64(len(leftOut))),
		logger.NewIntField("firstJobID", leftOut[0].JobID),
		logger.NewIntField("lastJobID", leftOut[len(leftOut)-1].JobID),
	)
	return w.brt.setMultipleJobStatus(setMultipleJobStatusParams{
		asyncJobMetadata: newAsyncJobMetadata(leftOut),
		AsyncOutput: asynccommon.AsyncUploadOutput{
			DestinationID: w.destinationID,
			FailedJobIDs:  jobIDs(leftOut),
			FailedReason:  "job left out of the destination's outcome",
		},
		Attempted: attempted,
	})
}

// recordOutcome counts an upload that left no job importing, or a finished import, in the circuit breaker: a failure
// if all of its jobs failed and will be retried, a success otherwise. It returns the result of the step. After a
// failure, the next fetch waits for a ping, so that the failed jobs aren't retried at once.
func (w *asyncWorker) recordOutcome(statusList []*jobsdb.JobStatusT) asyncStep {
	if lo.EveryBy(statusList, func(s *jobsdb.JobStatusT) bool { return s.JobState == jobsdb.Failed.State }) {
		w.cb.Failure()
		return stepDone
	}
	w.cb.Success()
	if w.backlog {
		return stepMore
	}
	return stepDone
}

// fetch fetches the jobs that the batch can still take, adds them to the batch in job ID order and marks them
// executing.
func (w *asyncWorker) fetch() asyncStep {
	brt := w.brt
	jobs := w.getJobs()
	if len(jobs) == 0 {
		return stepIdle
	}
	dest, ok := brt.asyncDestination(w.destinationID) // after the query, during which the config can change
	if !ok {
		return stepIdle
	}
	jobs = brt.abortDrainedJobs(&dest.DestinationWithSources, jobs)
	if brt.disableEgress {
		brt.setMultipleJobStatus(setMultipleJobStatusParams{
			asyncJobMetadata: newAsyncJobMetadata(jobs),
			AsyncOutput: asynccommon.AsyncUploadOutput{
				DestinationID:   w.destinationID,
				SucceededJobIDs: jobIDs(jobs),
				SuccessResponse: fmt.Sprintf(`{"error":%q}`, rterror.ErrDisabledEgress.Error()),
			},
			JobsList: jobs,
		})
		return stepDone
	}
	if invalidManager, ok := dest.manager.(*asynccommon.InvalidManager); ok {
		w.abortJobs(dest, jobs, invalidManager.Error)
		w.cb.Failure()
		return stepDone
	}
	w.addToBatch(dest, jobs)
	if w.backlog || w.batch.full {
		return stepMore
	}
	return stepDone
}

// getJobs fetches the failed and unprocessed jobs that the batch can still take, in job ID order.
func (w *asyncWorker) getJobs() []*jobsdb.JobT {
	brt := w.brt
	defer brt.limiter.read.Begin("")()
	jobsLimit := brt.jobQueryBatchSize.Load()
	if w.batch.limits.jobs > 0 {
		jobsLimit = min(jobsLimit, w.batch.limits.jobs-len(w.batch.jobs))
	}
	queryParams := jobsdb.GetQueryParams{
		CustomValFilters: []string{brt.destType},
		ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: w.destinationID}},
		JobsLimit:        jobsLimit,
		PayloadSizeLimit: min(brt.adaptiveLimit(brt.payloadLimit.Load()), int64(w.batch.limits.bytes-w.batch.size)),
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
				return nil
			}
			panic(fmt.Errorf("fetching jobs of %s destination %s: %w", brt.destType, w.destinationID, err))
		}
		query = len(res.Jobs) == 0 && res.DSLimitsReached
	}
	stats.Default.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, stats.Tags{"function": "getJobs", "destType": brt.destType, "partition": w.destinationID}).Since(queryStart)
	w.backlog = res.LimitsReached || res.DSLimitsReached
	brt.pipelineDelayStats(w.destinationID, lo.FirstOrEmpty(res.Jobs), lo.LastOrEmpty(res.Jobs))
	return res.Jobs
}

// addToBatch adds jobs to the batch until one doesn't fit, and marks the added jobs executing. The jobs after it keep
// their status, and a later fetch reads them again.
func (w *asyncWorker) addToBatch(dest *asyncDestination, jobs []*jobsdb.JobT) {
	brt := w.brt
	prepareStart := time.Now()
	defer w.batch.closeFile()
	var added []*jobsdb.JobT
	for _, job := range jobs {
		line, err := dest.manager.Transform(job)
		if err != nil {
			w.abortJobs(dest, []*jobsdb.JobT{job}, err)
			continue
		}
		ok, err := w.batch.add(job, line+"\n")
		if err != nil {
			panic(fmt.Errorf("adding a job to the batch of %s destination %s: %w", brt.destType, w.destinationID, err))
		}
		if !ok {
			w.backlog = true
			break
		}
		added = append(added, job)
	}
	brt.asyncDestinationTimer("async_prepare_time", w.destinationID).Since(prepareStart)
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

// newBatch returns an empty batch. SFTP batches are limited only by bytes.
func (w *asyncWorker) newBatch() *asyncBatch {
	limits := asyncBatchLimits{jobs: w.brt.maxEventsInABatch, bytes: w.brt.maxPayloadSizeInBytes}
	if asynccommon.IsSFTPDestination(w.brt.destType) {
		limits.jobs = 0
	}
	return &asyncBatch{destinationID: w.destinationID, limits: limits}
}

// abortJobs aborts jobs that the worker must not deliver
func (w *asyncWorker) abortJobs(dest *asyncDestination, jobs []*jobsdb.JobT, reason error) {
	bySource := lo.GroupBy(jobs, func(job *jobsdb.JobT) string { return gjson.GetBytes(job.Parameters, "source_id").String() })
	for sourceID, jobs := range bySource {
		source, _ := lo.Find(dest.Sources, func(source backendconfig.SourceT) bool { return source.ID == sourceID })
		w.brt.updateJobStatus(&BatchedJobs{
			Jobs:       jobs,
			Connection: &Connection{Destination: dest.Destination, Source: source},
			JobState:   jobsdb.Aborted.State,
		}, false, reason, false)
	}
}

func jobIDs(jobs []*jobsdb.JobT) []int64 {
	return lo.Map(jobs, func(job *jobsdb.JobT, _ int) int64 { return job.JobID })
}
