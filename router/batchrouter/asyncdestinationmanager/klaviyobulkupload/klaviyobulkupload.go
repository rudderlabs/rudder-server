package klaviyobulkupload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type KlaviyoBulkUploader struct {
	destName          string
	destinationConfig map[string]interface{}
	logger            logger.Logger
	Client            *http.Client
}

func NewManager(destination *backendconfig.DestinationT) (*KlaviyoBulkUploader, error) {
	return &KlaviyoBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Klaviyo").Child("KlaviyoBulkUploader"),
	}, nil
}

func (kbu *KlaviyoBulkUploader) Poll(_ common.AsyncPoll) common.PollStatusResponse {
	// pollUrl := "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
	return common.PollStatusResponse{}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(_ common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

// func (kbu *KlaviyoBulkUploader) generateErrorOutput(errorString string, err error, importingJobIds []int64) common.AsyncUploadOutput {
// 	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
// 		"module":   "batch_router",
// 		"destType": "klaviyobulkupload",
// 	})
// 	eventsAbortedStat.Count(len(importingJobIds))
// 	return common.AsyncUploadOutput{
// 		AbortCount:    len(importingJobIds),
// 		DestinationID: kbu.destinationId,
// 		AbortJobIDs:   importingJobIds,
// 		AbortReason:   fmt.Sprintf("%s %v", errorString, err.Error()),
// 	}
// }

func (kbu *KlaviyoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	// startTime := time.Now()
	var input []common.AsyncJob
	destination := asyncDestStruct.Destination
	destType := destination.DestinationDefinition.Name
	destinationID := destination.ID
	destConfig := destination.Config
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	uploadURL := "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
	kbupayload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destination.Config,
		DestType: strings.ToLower(destType),
	})
	// print kbupayload in json format
	fmt.Println(string(kbupayload))
	if err != nil {
		return common.AsyncUploadOutput{}
	}

	payloadReader := bytes.NewReader(kbupayload)
	req, err := http.NewRequest(http.MethodPost, uploadURL, payloadReader)
	if err != nil {
		return common.AsyncUploadOutput{}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization Klaviyo-API-Key`", destConfig["privateApiKey"].(string))
	resp, err := kbu.Client.Do(req)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  failedJobIDs,
			AbortReason:   err.Error(),
			DestinationID: destinationID,
		}
	}
	defer resp.Body.Close()
	return common.AsyncUploadOutput{
		ImportingJobIDs: importingJobIDs,
		SucceededJobIDs: importingJobIDs,
		SuccessResponse: string(kbupayload),
		DestinationID:   destination.ID,
	}
}

func (kbu *KlaviyoBulkUploader) GetErrorStats() map[string]interface{} {
	return nil
}
