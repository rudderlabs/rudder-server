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

func (handler *ClearOperationHandlerT) Exec(payload []byte) (bool, error) {
	//TODO fill clear code
	time.Sleep(time.Second)

	var reqPayload ClearQueueRequestPayload
	err := json.Unmarshal(payload, &reqPayload)
	if err != nil {
		return false, err
	}

	//clearFromJobsdb(clearOperationHandler.gatewayDB)

	return false, nil
}
