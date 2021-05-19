package queuemanager

import (
	"encoding/json"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
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
}

func init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterIntConfigVariable(100000, &jobQueryBatchSize, true, 1, "QueueManager.jobQueryBatchSize")
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

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:     "source_id",
			Value:    reqPayload.SourceID,
			Optional: false,
		},
	}

	//Clear From GatewayDB
	handler.clearFromJobsdb(clearOperationHandler.gatewayDB, parameterFilters, false, false)
	//Clear From RouterDB
	handler.clearFromJobsdb(clearOperationHandler.routerDB, parameterFilters, true, true)
	//Clear From BatchRouterDB
	handler.clearFromJobsdb(clearOperationHandler.batchRouterDB, parameterFilters, false, true)

	return nil
}

func (handler *ClearOperationHandlerT) clearFromJobsdb(db jobsdb.JobsDB, parameterFilters []jobsdb.ParameterFilterT, throttled, waiting bool) {

	for {
		var retryList, throttledList, waitList, unprocessedList []*jobsdb.JobT
		toQuery := jobQueryBatchSize
		retryList = db.GetToRetry(jobsdb.GetQueryParamsT{Count: toQuery, ParameterFilters: parameterFilters})
		toQuery -= len(retryList)
		if throttled {
			throttledList = db.GetThrottled(jobsdb.GetQueryParamsT{Count: toQuery, ParameterFilters: parameterFilters})
			toQuery -= len(throttledList)
		}
		if waiting {
			waitList = db.GetWaiting(jobsdb.GetQueryParamsT{Count: toQuery, ParameterFilters: parameterFilters})
			toQuery -= len(waitList)
		}
		unprocessedList = db.GetUnprocessed(jobsdb.GetQueryParamsT{Count: toQuery, ParameterFilters: parameterFilters})

		combinedList := append(waitList, append(unprocessedList, append(throttledList, retryList...)...)...)

		if len(combinedList) == 0 {
			pkgLogger.Infof("ClearQueueManager: clearFromJobsdb Complete. No more Jobs to clear from %s db for params: %#v", db.GetTablePrefix(), parameterFilters)
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
			}
			statusList = append(statusList, &status)
		}
		//Mark the jobs statuses
		err := db.UpdateJobStatus(statusList, []string{}, parameterFilters)
		if err != nil {
			pkgLogger.Errorf("ClearQueueManager: Error occurred while marking jobs statuses as aborted. Panicking. ParameterFilters:%#v, Err: %v", parameterFilters, err)
			panic(err)
		}

		pkgLogger.Infof("cleared %d jobs from %s db", len(statusList), db.GetTablePrefix)
	}
}
