/*
	Warehouse jobs package provides the capability to running arbitrary jobs on the warehouses using the query parameters provided.
	Some of the jobs that can be run are
	1) delete by task run id,
	2) delete by job run id,
	3) delete by update_at
	4) any other update / clean up operations

	The following handlers file is the entry point for the handlers.
*/

package jobs

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger = logger.NewLogger().Child("warehouse-asyncjob")
)

//The following handler gets called for adding async
func AddWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.Info("[WH-Jobs] Got Async Job Add Request")
	pkgLogger.LogRequest(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH-Jobs]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var startJobPayload StartJobReqPayload
	err = json.Unmarshal(body, &startJobPayload)
	if err != nil {
		pkgLogger.Errorf("[WH-Jobs]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}
	if startJobPayload.SourceID == "" || startJobPayload.JobRunID == "" || startJobPayload.TaskRunID == "" || startJobPayload.DestinationID == "" {
		pkgLogger.Errorf("[WH-Jobs]: Invalid Payload %v", err)
		http.Error(w, "invalid Payload", http.StatusBadRequest)
		return
	}
	if !AsyncJobWH.enabled {
		pkgLogger.Errorf("[WH-Jobs]: Error Warehouse Jobs API not initialized %v", err)
		http.Error(w, "warehouse jobs api not initialized", http.StatusBadRequest)
		return
	}
	tableNames, err := AsyncJobWH.getTableNamesBy(startJobPayload.SourceID, startJobPayload.DestinationID, startJobPayload.JobRunID, startJobPayload.TaskRunID)

	if err != nil {
		pkgLogger.Errorf("[WH-Jobs]: Error extracting tableNames for the job run id: %v", err)
		http.Error(w, "Error extracting tableNames", http.StatusBadRequest)
		return
	}

	//Add to wh_async_job queue each of the tables
	for _, th := range tableNames {
		if th != "RUDDER_DISCARDS" && th != "rudder_discards" {
			destType := (*AsyncJobWH.connectionsMap)[startJobPayload.DestinationID][startJobPayload.SourceID].Destination.DestinationDefinition.Name
			payload := AsyncJobPayloadT{
				SourceID:      startJobPayload.SourceID,
				DestinationID: startJobPayload.DestinationID,
				TableName:     th,
				DestType:      destType,
				JobType:       "async_job",
				JobRunID:      startJobPayload.JobRunID,
				TaskRunID:     startJobPayload.TaskRunID,
				StartTime:     startJobPayload.StartTime,
				AsyncJobType:  startJobPayload.AsyncJobType,
			}
			AsyncJobWH.addJobstoDB(&payload)
		}
	}
	_, _ = w.Write([]byte(`{ "error":"" }`))

}

func StatusWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.Info("Got Async Job Status Request")
	pkgLogger.LogRequest(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH-Jobs]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var startJobPayload StartJobReqPayload
	err = json.Unmarshal(body, &startJobPayload)
	pkgLogger.Infof("Got Payload jobrunid %s, taskrunid %s \n", startJobPayload.JobRunID, startJobPayload.TaskRunID)
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

	status, err := AsyncJobWH.getStatusAsyncJob(&startJobPayload)

	var statusresponse WhStatusResponse
	if err != nil {
		statusresponse = WhStatusResponse{
			Status: status,
			Err:    err.Error(),
		}
	} else {
		statusresponse = WhStatusResponse{
			Status: status,
			Err:    "",
		}
	}

	writeResponse, err := json.Marshal(statusresponse)

	if err != nil {
		w.Write(writeResponse)
	}
	w.Write(writeResponse)

}

func StopWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {

}

func GetWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {

}
