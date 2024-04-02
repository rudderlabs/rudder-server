package yandexmetricaofflineevents

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type YandexMetricaBulkUploader struct {
	destName          string
	destinationConfig map[string]interface{}
	transformUrl      string
	pollUrl           string
	logger            logger.Logger
	timeout           time.Duration
}

func NewManager(destination *backendconfig.DestinationT) (*YandexMetricaBulkUploader, error) {
	YandexMetricaBulkUpload := &YandexMetricaBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		pollUrl:           "",
		transformUrl:      config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("YandexMetrica").Child("YandexMetricaBulkUploader"),
		timeout:           config.GetDuration("HttpClient.yandexMetricaBulkUpload.timeout", 30, time.Second),
	}
	return YandexMetricaBulkUpload, nil
}

// return a success response for the poll request every time by default
func (ym *YandexMetricaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	return common.PollStatusResponse{
		Complete:       true,
		InProgress:     false,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobURLs:  "",
		WarningJobURLs: "",
		Error:          "",
	}
}

type YandexMetricaOfflineEvents struct {
	// Add your configuration fields here
}

func NewYandexMetricaOfflineEvents() *YandexMetricaOfflineEvents {
	// Initialize and return a new instance of YandexMetricaOfflineEvents
	return &YandexMetricaOfflineEvents{}
}

func (ym *YandexMetricaBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

func (ym *YandexMetricaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	destConfig := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	destinationUploadUrl := asyncDestStruct.DestinationUploadURL
	uploadURL, err := url.JoinPath(ym.transformUrl, destinationUploadUrl)

	if err != nil {
		return common.AsyncUploadOutput{
			FailedReason:  "BRT: Failed to prepare upload url " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	file, err := os.Open(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedReason:  "Error while opening file",
			DestinationID: destinationID,
		}
	}
	defer file.Close()
	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Error in Unmarshalling Job for Yandex Metrica destination: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		input = append(input, tempJob)
	}
	ympayload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destConfig,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: JSON Marshal Failed" + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.Observe(float64(len(ympayload)))
	ym.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v", destType)
	ymresponseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(uploadURL, ympayload, config.GetDuration("HttpClient.yandexmetricaofflineevents.timeout", 10, time.Minute))
	ym.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var statusCode string
	if statusCodeHTTP != 200 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf(`HTTP Call to Transformer Returned Non 200. StatusCode: %d`, statusCodeHTTP),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	bodyBytes = ymresponseBody

	// uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
	// 	"module":   "batch_router",
	// 	"destType": ym.destName,
	// })

	return common.AsyncUploadOutput{}
}
