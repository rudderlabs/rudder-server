package operationmanager

import (
	"encoding/json"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
)

type ClearOperationHandlerT struct {
	gatewayDB     jobsdb.JobsDB
	routerDB      jobsdb.JobsDB
	batchRouterDB jobsdb.JobsDB
}

var (
	clearOperationHandler *ClearOperationHandlerT
	jobQueryBatchSize     int
)

type ClearQueueRequestPayload struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	JobRunID      string `json:"job_run_id"`
}

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterIntConfigVariable(100000, &jobQueryBatchSize, true, 1, "ClearQueueManager.jobQueryBatchSize")
}

func GetClearOperationHandlerInstance(gatewayDB, routerDB, batchRouterDB jobsdb.JobsDB) *ClearOperationHandlerT {
	if clearOperationHandler == nil {
		clearOperationHandler = new(ClearOperationHandlerT)
		clearOperationHandler.gatewayDB = gatewayDB
		clearOperationHandler.routerDB = routerDB
		clearOperationHandler.batchRouterDB = batchRouterDB
	}

	return clearOperationHandler
}

func (handler *ClearOperationHandlerT) Exec(payload []byte) error {
	var reqPayload ClearQueueRequestPayload
	err := json.Unmarshal(payload, &reqPayload)
	if err != nil {
		return err
	}

	var parameterFilters []jobsdb.ParameterFilterT
	if reqPayload.SourceID != "" {
		parameterFilters = append(parameterFilters, jobsdb.ParameterFilterT{
			Name:     "source_id",
			Value:    reqPayload.SourceID,
			Optional: false,
		},
		)
	}
	if reqPayload.DestinationID != "" {
		parameterFilters = append(parameterFilters, jobsdb.ParameterFilterT{
			Name:     "destination_id",
			Value:    reqPayload.DestinationID,
			Optional: false,
		},
		)
	}
	if reqPayload.JobRunID != "" {
		parameterFilters = append(parameterFilters, jobsdb.ParameterFilterT{
			Name:     "source_job_run_id",
			Value:    reqPayload.JobRunID,
			Optional: false,
		},
		)
	}

	var pm *processor.HandleT
	for {
		pm, err = processor.GetProcessorManager()
		if err == nil && pm != nil {
			break
		}
		pkgLogger.Infof("ProcessorManager is nil. Retrying after a second")
		time.Sleep(time.Second)
	}
	pm.Shutdown()

	var rm router.RoutersManagerI
	for {
		rm, err = router.GetRoutersManager()
		if err == nil && rm != nil && rm.AreRoutersReady() {
			break
		}
		pkgLogger.Infof("RoutersManager is nil or Routers are not ready. Retrying after a second")
		time.Sleep(time.Second)
	}
	rm.PauseAll()

	var brm batchrouter.BatchRoutersManagerI
	for {
		brm, err = batchrouter.GetBatchRoutersManager()
		if err == nil && brm != nil && brm.AreBatchRoutersReady() {
			break
		}
		pkgLogger.Infof("BatchRoutersManager is nil or BatchRouters are not ready. Retrying after a second")
		time.Sleep(time.Second)
	}
	brm.PauseAll()

	//Clear From GatewayDB
	handler.clearFromJobsdb(clearOperationHandler.gatewayDB, parameterFilters, false, false)
	//Clear From RouterDB
	handler.clearFromJobsdb(clearOperationHandler.routerDB, parameterFilters, true, true)
	//Clear From BatchRouterDB
	handler.clearFromJobsdb(clearOperationHandler.batchRouterDB, parameterFilters, false, true)

	// Need to find a way to restart the processor
	p, _ := apphandlers.GetProcessorParams()
	p.RudderCoreErrGrp.Go(func() error {
		apphandlers.StartProcessor(*p.RudderCoreCtx, &p.AppOptions.ClearDB, *p.EnableProcessor, p.GatewayDB,
			p.RouterDB, p.BatchRouterDB, p.ProcErrorDB, p.ReportingI, *p.MultitenantStats)
		return nil
	})
	pm.Resume()
	rm.ResumeAll()
	brm.ResumeAll()

	return nil
}

func (handler *ClearOperationHandlerT) clearFromJobsdb(db jobsdb.JobsDB, parameterFilters []jobsdb.ParameterFilterT, throttled, waiting bool) {
	startTime := time.Now().UTC()
	for {
		var pendingList, unprocessedList []*jobsdb.JobT
		toQuery := jobQueryBatchSize
		getParams := jobsdb.GetQueryParamsT{JobCount: toQuery, ParameterFilters: parameterFilters}
		getParams.StateFilters = []string{jobsdb.Failed.State, jobsdb.Waiting.State, jobsdb.Importing.State}
		pendingList = db.GetProcessed(getParams)
		toQuery -= len(pendingList)
		getParams = jobsdb.GetQueryParamsT{JobCount: toQuery, ParameterFilters: parameterFilters}
		getParams.UseTimeFilter = true
		getParams.Before = startTime
		unprocessedList = db.GetUnprocessed(getParams)

		combinedList := append(unprocessedList, pendingList...)

		if len(combinedList) == 0 {
			pkgLogger.Infof("ClearQueueManager: clearFromJobsdb Complete. No more Jobs to clear from %s db for params: %#v", db.GetIdentifier(), parameterFilters)
			break
		}

		var statusList []*jobsdb.JobStatusT
		for _, job := range combinedList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{"reason": "aborted as per request"}`), // check
				Parameters:    []byte(`{}`),
			}
			statusList = append(statusList, &status)
		}
		//Mark the jobs statuses
		err := db.UpdateJobStatus(statusList, []string{}, parameterFilters)
		if err != nil {
			pkgLogger.Errorf("ClearQueueManager: Error occurred while marking jobs statuses as aborted. Panicking. ParameterFilters:%#v, Err: %v", parameterFilters, err)
			panic(err)
		}

		pkgLogger.Infof("cleared %d jobs from %s db", len(statusList), db.GetIdentifier())
	}
}
