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

func StartWarehouseJobHandler(w http.ResponseWriter, r *http.Request) {
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
			payload := AsyncJobPayloadT{
				SourceID:      startJobPayload.SourceID,
				DestinationID: startJobPayload.DestinationID,
				TableName:     th,
				DestType:      "abc",
				JobType:       "deleteByJobRunID",
				JobRunID:      startJobPayload.JobRunID,
				TaskRunID:     "adbabdakjwdkl",
				StartTime:     startJobPayload.StartTime,
			}
			AsyncJobWH.addJobstoDB(&payload)
		}
	}

}
