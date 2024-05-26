package klaviyobulkupload

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Uploader interface {
	Upload(*common.AsyncDestinationStruct) common.AsyncUploadOutput
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Poller interface {
	Poll(input common.AsyncPoll) common.PollStatusResponse
}

type UploadStats interface {
	GetUploadStats(common.GetUploadStatsInput) common.GetUploadStatsResponse
}

type KlaviyoBulkUploader struct {
	destName             string
	destinationConfig    map[string]interface{}
	logger               logger.Logger
	Client               *http.Client
	jobIdToIdentifierMap map[string]int64
}

type UploadResp struct {
	Data struct {
		Id string `json:"id"`
	} `json:"data"`
}

type PollResp struct {
	Data struct {
		Id         string `json:"id"`
		Attributes struct {
			Total_count     int    `json:"total_count"`
			Completed_count int    `json:"completed_count"`
			Failed_count    int    `json:"failed_count"`
			Status          string `json:"status"`
		} `json:"attributes"`
	} `json:"data"`
}

type UploadStatusResp struct {
	Data []struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Attributes struct {
			Code   string `json:"code"`
			Title  string `json:"title"`
			Detail string `json:"detail"`
			Source struct {
				Pointer string `json:"pointer"`
			} `json:"source"`
			OriginalPayload struct {
				Id          string `json:"id"`
				AnonymousId string `json:"anonymous_id"`
			} `json:"original_payload"`
		} `json:"attributes"`
		Links struct {
			Self string `json:"self"`
		} `json:"links"`
	} `json:"data"`
	Links struct {
		Self  string `json:"self"`
		First string `json:"first"`
		Last  string `json:"last"`
		Prev  string `json:"prev"`
		Next  string `json:"next"`
	} `json:"links"`
}

func NewManager(destination *backendconfig.DestinationT) (*KlaviyoBulkUploader, error) {
	return &KlaviyoBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("KlaviyoBulkUpload").Child("KlaviyoBulkUploader"),
	}, nil
}

func (kbu *KlaviyoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	client := &http.Client{}
	destConfig := kbu.destinationConfig
	privateApiKey, _ := destConfig["privateApiKey"].(string)
	importId := pollInput.ImportId
	// pp.Println("ImportId: ", importId)
	// pp.Println("PrivateApiKey: ", privateApiKey)
	pollUrl := "https://a.klaviyo.com/api/profile-bulk-import-jobs/" + importId
	req, err := http.NewRequest("GET", pollUrl, nil)
	if err != nil {
		// pp.Println("inside error")
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			HasFailed:  true,
			Error:      err.Error(),
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization",
		"Klaviyo-API-Key "+privateApiKey)
	req.Header.Set("revision", "2024-05-15")
	resp, err := client.Do(req)
	if err != nil {
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 0,
			HasFailed:  true,
			Error:      err.Error(),
			HasWarning: false,
		}
	}

	var pollBodyBytes []byte
	var pollresp PollResp
	pollBodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()

	pollRespErr := json.Unmarshal(pollBodyBytes, &pollresp)
	if pollRespErr != nil {
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 0,
			HasFailed:  true,
			Error:      pollRespErr.Error(),
			HasWarning: false,
		}
	}
	// totalCount := pollresp.Data.Attributes.Total_count
	// completedCount := pollresp.Data.Attributes.Completed_count
	failCount := pollresp.Data.Attributes.Failed_count
	status := pollresp.Data.Attributes.Status
	// fmt.Println("Status: ", status)
	// fmt.Println("TotalCount: ", totalCount)
	// fmt.Println("CompletedCount: ", completedCount)
	// fmt.Println("FailCount: ", failCount)

	defer func() { _ = resp.Body.Close() }()

	switch status {
	case "queued", "processing":
		// pp.Println("inside processing")
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: true,
		}
	case "complete":
		// pp.Println("inside complete")
		if failCount > 0 {
			// pp.Println("inside fail")
			// pp.Println(importId)
			return common.PollStatusResponse{
				Complete:      true,
				InProgress:    false,
				StatusCode:    200,
				HasFailed:     true,
				HasWarning:    false,
				FailedJobURLs: importId,
			}
		} else {
			// pp.Println("inside success")
			return common.PollStatusResponse{
				Complete:   true,
				InProgress: false,
				StatusCode: 200,
				HasFailed:  false,
				HasWarning: false,
			}
		}
	default:
		// pp.Print("inside default")
		return common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			InProgress: true,
		}
	}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	client := &http.Client{}
	destConfig := kbu.destinationConfig
	privateApiKey, _ := destConfig["privateApiKey"].(string)
	pollResultImportId := UploadStatsInput.FailedJobURLs

	// make a map of jobId to error reason
	jobIdToErrorMap := make(map[int64]string)

	importingList := UploadStatsInput.ImportingList
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}
	// pp.Println("JobIDs: ", jobIDs)

	// pp.Println(privateApiKey)
	// pp.Println(pollResult)
	// pp.Println(importingList)
	ErrorMap := kbu.jobIdToIdentifierMap
	// pp.Println("ErrorMap: ", ErrorMap)

	importErrorUrl := "https://a.klaviyo.com/api/profile-bulk-import-jobs/" + pollResultImportId + "/import-errors"
	req, err := http.NewRequest("GET", importErrorUrl, nil)
	if err != nil {
		return common.GetUploadStatsResponse{}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization",
		"Klaviyo-API-Key "+privateApiKey)
	req.Header.Set("revision", "2024-05-15")
	resp, err := client.Do(req)
	// pp.Print("Response: ", string(resp.Body))
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 400,
			Error:      err.Error(),
		}
	}

	var uploadStatsBodyBytes []byte
	var failedJobIds []int64
	var uploadStatsResp UploadStatusResp
	uploadStatsBodyBytes, _ = io.ReadAll(resp.Body)
	// pp.Println("Response Body: ", string(uploadStatsBodyBytes))

	// uploadStatsBodyBytes -> original_payload>id
	// map -> jobId
	defer func() { _ = resp.Body.Close() }()

	uploadStatsBodyBytesErr := json.Unmarshal(uploadStatsBodyBytes, &uploadStatsResp)
	if uploadStatsBodyBytesErr != nil {
		// fmt.Println("Reached here!!!")
		return common.GetUploadStatsResponse{
			StatusCode: 400,
			Error:      uploadStatsBodyBytesErr.Error(),
		}
	}
	// Iterate over the Data array and get the jobId and error detail and store in jobIdToErrorMap
	x := 1
	for _, item := range uploadStatsResp.Data {
		// pp.Println("loop counter", x)
		x++
		// pp.Println("Item: ", item)
		orgPayload := item.Attributes.OriginalPayload
		var identifierId string
		if orgPayload.Id != "" {
			identifierId = orgPayload.Id
		} else {
			identifierId = orgPayload.AnonymousId
		}
		jobId := ErrorMap[identifierId]
		failedJobIds = append(failedJobIds, jobId)
		errorDetail := item.Attributes.Detail
		// fmt.Print("JobId: ", jobId)
		// fmt.Println("--Error Detail: ", errorDetail)

		// Store the jobId and errorDetail in the map
		jobIdToErrorMap[jobId] = errorDetail
	}

	// orgPayload := uploadStatsResp.Data[0].Attributes.OriginalPayload
	// errDetail := uploadStatsResp.Data[0].Attributes.Detail
	// identifierId := orgPayload.Id
	// parsedIdentifierId, _ := strconv.ParseInt(identifierId, 10, 64)
	// jobId := ErrorMap[parsedIdentifierId]
	// parsedJobId, _ := strconv.ParseInt(jobId, 10, 64)
	// failedJobIds = append(failedJobIds, parsedJobId)
	// jobIdToErrorMap[parsedJobId] = errDetail

	successKeys, _ := lo.Difference(jobIDs, failedJobIds)

	// pp.Println(uploadStatsBodyBytesErr)

	// get all the jobIds from the jobIdToIdentifierMap and store in failedJobIds array
	// pp.Println("Failed Job Ids: ", failedJobIds)

	// var uploadStatusResponse common.GetUploadStatsResponse
	// fmt.Println("Failed Job Ids: ", failedJobIds)
	// fmt.Println("JobIdToErrorMap: ", jobIdToErrorMap)
	// fmt.Println("SucceededKeys: ", successKeys)
	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Error:      "The import job failed",
		Metadata: common.EventStatMeta{
			FailedKeys:    failedJobIds,
			FailedReasons: jobIdToErrorMap,
			SucceededKeys: successKeys,
		},
	}
}

func (kbu *KlaviyoBulkUploader) generateKlaviyoErrorOutput(errorString string, err error, importingJobIds []int64, destinationID string) common.AsyncUploadOutput {
	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": "KLAVIYO_BULK_UPLOAD",
	})
	eventsAbortedStat.Count(len(importingJobIds))
	// pp.Println("importingJobIds: ", importingJobIds, len(importingJobIds))
	return common.AsyncUploadOutput{
		AbortCount:    len(importingJobIds),
		DestinationID: destinationID,
		AbortJobIDs:   importingJobIds,
		AbortReason:   fmt.Sprintf("%s %v", errorString, err.Error()),
	}
}

func (kbu *KlaviyoBulkUploader) ExtractProfiles(input map[string]interface{}) ([]map[string]interface{}, error) {
	message, ok := input["message"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("message field not found or not a map")
	}

	data, ok := message["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data field not found or not a map")
	}

	attributes, ok := data["attributes"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("attributes field not found or not a map")
	}

	profilesContainer, ok := attributes["profiles"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("profiles field not found or not a map")
	}

	profiles, ok := profilesContainer["data"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("data field in profiles not found or not a slice")
	}

	var profileMaps []map[string]interface{}
	// var metadataMap map[string]interface{}
	for _, profile := range profiles {
		profileMap, ok := profile.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("profile is not a map")
		}
		attributes, _ := profileMap["attributes"].(map[string]interface{})
		jobIdentifier := attributes["jobIdentifier"].(string)
		// pp.Println("Job Identifier:", jobIdentifier)
		// split the jobIdentifier by : and store before : into jobId and after : into Identifier into the jobIdToIdentifierMap
		jobIdentifierArray := strings.Split(jobIdentifier, ":")
		jobIdentifierValue, _ := strconv.ParseInt(jobIdentifierArray[1], 10, 64)
		if kbu.jobIdToIdentifierMap == nil {
			kbu.jobIdToIdentifierMap = make(map[string]int64)
		}
		kbu.jobIdToIdentifierMap[jobIdentifierArray[0]] = jobIdentifierValue

		// delete jobIdentifier from the attributes map as it is not required in the final payload
		delete(attributes, "jobIdentifier")

		profileMaps = append(profileMaps, profileMap)
	}

	return profileMaps, nil
}

func (kbu *KlaviyoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	startTime := time.Now()
	destination := asyncDestStruct.Destination
	filePath := asyncDestStruct.FileName
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	// pp.Println("Importing Job IDs: ", importingJobIDs)
	destType := destination.DestinationDefinition.Name
	destinationID := destination.ID
	// var failedJobs []int64
	// var successJobs []int64
	// var importIds []string
	// var errors []string
	listId, _ := destination.Config["listId"].(string)
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
	}
	// destConfig, err := json.Marshal(destination.Config)
	// if err != nil {
	// 	return kbu.generateKlaviyoErrorOutput("Error while marshalling destination config. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	// }
	file, err := os.Open(filePath)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while opening file. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	}
	defer file.Close()
	// var input []common.AsyncJob
	var combinedProfiles []map[string]interface{}
	// decoder := json.NewDecoder(file)

	// for {
	// 	var input map[string]interface{}
	// 	if err := decoder.Decode(&input); err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		}
	// 		return kbu.generateKlaviyoErrorOutput("Error while parsing JSON.", err, importingJobIDs, destinationID)
	// 	}
	// 	profiles, err := extractProfiles(input)
	// 	if err != nil {
	// 		return kbu.generateKlaviyoErrorOutput("Error while extracting profiles.", err, importingJobIDs, destinationID)
	// 	}
	// 	combinedProfiles = append(combinedProfiles, profiles...)
	// }
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var input map[string]interface{}
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &input); err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON.", err, importingJobIDs, destinationID)
		}
		profiles, err := kbu.ExtractProfiles(input)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while extracting profiles.", err, importingJobIDs, destinationID)
		}
		combinedProfiles = append(combinedProfiles, profiles...)
	}

	if err := scanner.Err(); err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while reading file.", err, importingJobIDs, destinationID)
	}

	// Create the combined payload
	// check if listId is present in the destination config
	var combinedPayload map[string]interface{}
	if listId == "" {
		combinedPayload = map[string]interface{}{
			"data": map[string]interface{}{
				"type": "profile-bulk-import-job",
				"attributes": map[string]interface{}{
					"data": map[string]interface{}{
						"profiles": combinedProfiles,
					},
				},
			},
		}
	} else {
		combinedPayload = map[string]interface{}{
			"data": map[string]interface{}{
				"type": "profile-bulk-import-job",
				"attributes": map[string]interface{}{
					"profiles": map[string]interface{}{
						"data": combinedProfiles,
					},
				},
				"relationships": map[string]interface{}{
					"lists": map[string]interface{}{
						"data": []map[string]interface{}{
							{
								"type": "list",
								"id":   listId,
							},
						},
					},
				},
			},
		}
	}

	// Convert combined payload to JSON
	outputJSON, err := json.MarshalIndent(combinedPayload, "", "  ")
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling combined JSON.", err, importingJobIDs, destinationID)
	}
	outputFilePath := "combined_payload.json"
	if err := os.WriteFile(outputFilePath, outputJSON, 0o644); err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while writing JSON to file.", err, importingJobIDs, destinationID)
	}
	// pp.Println(filePath)
	// destConfigJson := string(destConfig)
	// pp.Println(destConfigJson)
	// failedJobIDs := asyncDestStruct.FailedJobIDs
	uploadURL := "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
	client := &http.Client{}
	req, err := http.NewRequest("POST", uploadURL, bytes.NewBuffer(outputJSON))
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while creating request.", err, importingJobIDs, destinationID)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Klaviyo-API-Key "+destination.Config["privateApiKey"].(string))
	req.Header.Set("revision", "2024-05-15")

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, statLabels)
	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType, statLabels)
	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, statLabels)
	payloadSizeStat.Observe(float64(len(outputJSON)))

	resp, err := client.Do(req)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while sending request.", err, importingJobIDs, destinationID)
	}

	var bodyBytes []byte
	bodyBytes, _ = io.ReadAll(resp.Body)
	defer func() { _ = resp.Body.Close() }()
	// pp.Println("Response Status:", resp.Status)
	// pp.Println("Response Body:", string(bodyBytes))
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != 202 {
		return kbu.generateKlaviyoErrorOutput("Error while sending request.", fmt.Errorf(string(bodyBytes)), importingJobIDs, destinationID)
	}
	var parameters common.ImportParameters
	var uploadresp UploadResp
	uploadRespErr := json.Unmarshal((bodyBytes), &uploadresp)
	// pp.Println("Error: ", err)
	if uploadRespErr != nil {
		return kbu.generateKlaviyoErrorOutput("Error while unmarshaling response.", uploadRespErr, importingJobIDs, destinationID)
	}
	parameters.ImportId = uploadresp.Data.Id
	importParameters, err := json.Marshal(parameters)
	println("Import Parameters: ", string(importParameters))
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling parameters.", err, importingJobIDs, destinationID)
	}
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		ImportingParameters: importParameters,
		ImportingJobIDs:     importingJobIDs,
		SuccessResponse:     string(bodyBytes),
		DestinationID:       destination.ID,
	}
}

func (kbu *KlaviyoBulkUploader) GetErrorStats() map[string]interface{} {
	return nil
}

func (kbu *KlaviyoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}
