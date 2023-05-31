package marketobulkupload

import (
	"bufio"
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	time "time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoBulkUploader struct {
	destName string
	timeout  time.Duration
}

func NewManager(destination *backendconfig.DestinationT, HTTPTimeout time.Duration) *MarketoBulkUploader {
	marketobulkupload := &MarketoBulkUploader{destName: "MARKETO_BULK_UPLOAD", timeout: HTTPTimeout}
	return marketobulkupload
}

var (
	pkgLogger logger.Logger
)

func init() {
	pkgLogger = logger.NewLogger().Child("asyncdestinationmanager").Child("marketobulkupload")
}

func (b *MarketoBulkUploader) Poll(pollStruct common.AsyncPoll) ([]byte, int) {
	payload, err := json.Marshal(pollStruct)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	pollUrl := "/pollStatus"
	bodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(transformUrl+pollUrl, payload, b.timeout)
	return bodyBytes, statusCode
}

func (b *MarketoBulkUploader) FetchFailedEvents(destStruct *utils.DestinationWithSources, importingList []*jobsdb.JobT, importingJob *jobsdb.JobT, asyncResponse common.AsyncStatusResponse) ([]byte, int) {
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	failedJobUrl := asyncResponse.FailedJobsURL
	parameters := importingJob.LastJobStatus.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	csvHeaders := gjson.GetBytes(parameters, "metadata.csvHeader").String()
	payload := common.GenerateFailedPayload(destStruct.Destination.Config, importingList, importId, b.destName, csvHeaders)
	//payload eg: "{\"config\":{\"clientId\":\"01a70f1f-ff37-46fc-bdff-42e92a3f2bb3\",\"clientSecret\":\"rziQBHtZ34Vl1CE3x3OiA3n8Wr45lwar\",\"columnFieldsMapping\":[{\"from\":\"anonymousId\",\"to\":\"anonymousId\"},{\"from\":\"address\",\"to\":\"address\"},{\"from\":\"email\",\"to\":\"email\"}],\"deDuplicationField\":\"\",\"munchkinId\":\"585-AXP-425\",\"oneTrustCookieCategories\":[],\"uploadInterval\":\"10\"},\"input\":[{\"message\":{\"address\":23,\"anonymousId\":\"Test Kinesis\",\"email\":\"test@kinesis.com\"},\"metadata\":{\"job_id\":5}}],\"destType\":\"marketo_bulk_upload\",\"importId\":\"3095\""
	failedBodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(transformUrl+failedJobUrl, payload, b.timeout)
	// failedBodyBytes eg: "{\"statusCode\":400,\"error\":\"404\"}"
	return failedBodyBytes, statusCode
}

func (b *MarketoBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	fmt.Println("**********IN UPLOAD FUNCTION***********")
	resolveURL := func(base, relative string) string {
		fmt.Println("**********IN RESOLVE URL***********")
		// var logger logger.Logger
		baseURL, _ := url.Parse(base)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		relURL, _ := url.Parse(relative)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		destURL := baseURL.ResolveReference(relURL).String()
		return destURL
	}
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	destinationID := destination.ID
	destinationUploadUrl := asyncDestStruct.URL
	url := resolveURL(transformUrl, destinationUploadUrl)
	filePath := asyncDestStruct.FileName
	config := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	file, err := os.Open(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	defer file.Close()
	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			panic("Unmarshalling a Single Line Failed")
		}
		input = append(input, tempJob)
	}
	var uploadT common.AsyncUploadT
	uploadT.Input = input
	uploadT.Config = config
	uploadT.DestType = strings.ToLower(destType)
	payload, err := json.Marshal(uploadT)
	if err != nil {
		panic("BRT: JSON Marshal Failed " + err.Error())
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.SendTiming(time.Millisecond * time.Duration(len(payload)))
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Started for Dest Type %v", destType)
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(url, payload, b.timeout)
	// resp body eg: {statusCode: 200, importId: '3099', pollURL: '/pollStatus', metadata: {…}}
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var httpFailed bool
	var statusCode string
	if statusCodeHTTP != 200 {
		bodyBytes = []byte(`"error" : "HTTP Call to Transformer Returned Non 200"`)
		httpFailed = true
	} else {
		bodyBytes = responseBody
		statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()
	}

	var uploadResponse common.AsyncUploadOutput
	if httpFailed {
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	} else if statusCode == "200" {
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		var parameters common.Parameters
		parameters.ImportId = responseStruct.ImportId
		parameters.PollUrl = responseStruct.PollUrl
		metaDataString, ok := responseStruct.Metadata["csvHeader"].(string)
		if !ok {
			parameters.MetaData = common.MetaDataT{CSVHeaders: ""}
		} else {
			parameters.MetaData = common.MetaDataT{CSVHeaders: metaDataString}
		}
		importParameters, err := json.Marshal(parameters)
		if err != nil {
			panic("Errored in Marshalling" + err.Error())
		}
		successfulJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)

		uploadResponse = common.AsyncUploadOutput{
			ImportingJobIDs:     successfulJobIDs,
			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
			ImportingParameters: stdjson.RawMessage(importParameters),
			ImportingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID:       destinationID,
		}
	} else if statusCode == "400" {
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		eventsAbortedStat := stats.Default.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": destType,
		})
		abortedJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)
		eventsAbortedStat.Count(len(abortedJobIDs))
		uploadResponse = common.AsyncUploadOutput{
			AbortJobIDs:   abortedJobIDs,
			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID: destinationID,
		}
	} else {
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	return uploadResponse

}
