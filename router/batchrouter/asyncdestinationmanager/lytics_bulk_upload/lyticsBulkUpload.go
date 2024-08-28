package lyticsBulkUpload

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type LyticsServiceImpl struct {
	BulkApi string
}

func (e *LyticsServiceImpl) getBulkApi(destConfig DestinationConfig) *LyticsServiceImpl {
	return &LyticsServiceImpl{
		BulkApi: fmt.Sprintf("https://bulk.lytics.io/collect/bulk/%s?timestamp_field=%s", destConfig.LyticsStreamName, destConfig.TimestampField),
	}
}

func (*LyticsBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (e *LyticsServiceImpl) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
	req, err := http.NewRequest(data.Method, data.Endpoint, data.Body)
	if err != nil {
		return nil, 500, err
	}
	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("content-type", data.ContentType)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, 500, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, res.StatusCode, err
}

func (e *LyticsServiceImpl) UploadBulkFile(data *HttpRequestData, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	data.Method = http.MethodPost
	data.ContentType = "application/csv"
	data.Body = file
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 200 {
		return fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	return nil
}

func (b *LyticsBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	filePath := asyncDestStruct.FileName
	destConfig, err := jsoniter.Marshal(destination.Config)
	if err != nil {
		eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsAbortedStat.Count(len(asyncDestStruct.ImportingJobIDs))
		return common.AsyncUploadOutput{
			AbortCount:    len(asyncDestStruct.ImportingJobIDs),
			DestinationID: asyncDestStruct.Destination.ID,
			AbortJobIDs:   asyncDestStruct.ImportingJobIDs,
			AbortReason:   fmt.Sprintf("%s %v", "Error while marshalling destination config", err.Error()),
		}
	}
	var failedJobs []int64
	var successJobs []int64

	destConfigJson := string(destConfig)
	streamTraitsMapping := convertGjsonToStreamTraitMapping(gjson.Get(destConfigJson, "streamTraitsMapping"))
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("failed to convert streamTraitsMapping: %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	actionFiles, err := b.createCSVFile(filePath, streamTraitsMapping)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})

	uploadRetryableStat.Count(len(actionFiles.FailedJobIDs))

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})

	uploadDataData := HttpRequestData{
		Endpoint:      b.baseEndpoint,
		Authorization: b.authorization,
	}

	startTime := time.Now()
	errorDuringUpload := b.service.UploadBulkFile(&uploadDataData, actionFiles.CSVFilePath)
	uploadTimeStat.Since(startTime)

	if errorDuringUpload != nil {
		b.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
		failedJobs = append(append(failedJobs, actionFiles.SuccessfulJobIDs...), actionFiles.FailedJobIDs...)
		// remove the file that could not be uploaded
		err = os.Remove(actionFiles.CSVFilePath)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
				FailedReason:  fmt.Sprintf("Error in removing zip file: %v", err.Error()),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
		}
	}

	failedJobs = append(failedJobs, actionFiles.FailedJobIDs...)
	successJobs = append(successJobs, actionFiles.SuccessfulJobIDs...)

	err = os.Remove(actionFiles.CSVFilePath)
	if err != nil {
		b.logger.Error("Error in removing zip file: %v", err)
	}

	return common.AsyncUploadOutput{
		ImportingJobIDs: successJobs,
		FailedJobIDs:    append(asyncDestStruct.FailedJobIDs, failedJobs...),
		ImportingCount:  len(successJobs),
		FailedCount:     len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:   destination.ID,
	}
}
