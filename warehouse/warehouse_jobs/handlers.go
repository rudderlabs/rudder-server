/*
	Warehouse jobs package provides the capability to running arbitrary jobs on the warehouses using the query parameters provided.
	Some of the jobs that can be run are
	1) delete by task run id,
	2) delete by job run id,
	3) delete by update_at
	4) any other update / clean up operations

	The following handlers file is the entry point for the handlers.
*/

package warehouse_jobs

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger = logger.NewLogger().Child("warehouse-asyncjob")
)

//The following handler gets called
func AddWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.Info("Got Async Job Request")
	pkgLogger.LogRequest(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var startJobPayload StartJobReqPayload
	err = json.Unmarshal(body, &startJobPayload)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}
	if !AsyncJobWH.enabled {
		pkgLogger.Errorf("[WH]: Error Warehouse Jobs API not initialized %v", err)
		http.Error(w, "warehouse jobs api not initialized", http.StatusBadRequest)
		return
	}
	tableNames, err := AsyncJobWH.getTableNamesByJobRunID(startJobPayload.JobRunID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error extracting tableNames for the job run id: %v", err)
		http.Error(w, "Error extracting tableNames", http.StatusBadRequest)
		return
	}

	//Add to wh_async_job queue each of the tables
	for _, th := range tableNames {
		if th != "RUDDER_DISCARDS" {
			destType := (*AsyncJobWH.connectionsMap)[startJobPayload.DestinationID][startJobPayload.SourceID].Destination.DestinationDefinition.Name
			payload := AsyncJobPayloadT{
				SourceID:      startJobPayload.SourceID,
				DestinationID: startJobPayload.DestinationID,
				TableName:     th,
				DestType:      destType,
				JobType:       "deleteByJobRunID",
				JobRunID:      startJobPayload.JobRunID,
				TaskRunID:     startJobPayload.TaskRunID,
				StartTime:     startJobPayload.StartTime,
			}
			AsyncJobWH.addJobstoDB(&payload)
		}
	}

}

func StatusWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {

}

func StopWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {

}

func GetWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {

}
