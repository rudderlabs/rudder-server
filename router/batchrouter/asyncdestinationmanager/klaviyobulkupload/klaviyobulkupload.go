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

func createFinalPayload(combinedProfiles []map[string]interface{}, listId string) Payload {
	payload := Payload{
		Data: Data{
			Type: "profile-bulk-import-job",
			Attributes: Attributes{
				Profiles: Profiles{
					Data: combinedProfiles,
				},
			},
		},
	}

	if listId != "" {
		payload.Data.Relationships = &Relationships{
			Lists: Lists{
				Data: []List{
					{
						Type: "list",
						ID:   listId,
					},
				},
			},
		}
	}

	return payload
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
	pollUrl := "https://a.klaviyo.com/api/profile-bulk-import-jobs/" + importId
	req, err := http.NewRequest("GET", pollUrl, nil)
	if err != nil {
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
	failCount := pollresp.Data.Attributes.Failed_count
	status := pollresp.Data.Attributes.Status
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

	ErrorMap := kbu.jobIdToIdentifierMap

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
		x++
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
		jobIdToErrorMap[jobId] = errorDetail
	}
	successKeys, _ := lo.Difference(jobIDs, failedJobIds)

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

	body, ok := message["body"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("body field not found or not a map")
	}

	json, ok := body["JSON"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data field not found or not a map")
	}

	data, ok := json["data"].(map[string]interface{})
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
	for _, profile := range profiles {
		profileMap, ok := profile.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("profile is not a map")
		}
		attributes, _ := profileMap["attributes"].(map[string]interface{})
		fmt.Println("Attributes: ", attributes)
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
	destType := destination.DestinationDefinition.Name
	destinationID := destination.ID
	listId, _ := destination.Config["listId"].(string)
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
	}
	file, err := os.Open(filePath)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while opening file. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	}
	defer file.Close()
	var combinedProfiles []map[string]interface{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var input map[string]interface{}
		line := scanner.Text()
		err := json.Unmarshal([]byte(line), &input)
		if err != nil {
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

	combinedPayload := createFinalPayload(combinedProfiles, listId)

	// Convert combined payload to JSON
	outputJSON, err := json.MarshalIndent(combinedPayload, "", "  ")
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling combined JSON.", err, importingJobIDs, destinationID)
	}
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
	uploadTimeStat.Since(startTime)

	if resp.StatusCode != 202 {
		return kbu.generateKlaviyoErrorOutput("Error while sending request.", fmt.Errorf(string(bodyBytes)), importingJobIDs, destinationID)
	}
	var parameters common.ImportParameters
	var uploadresp UploadResp
	uploadRespErr := json.Unmarshal((bodyBytes), &uploadresp)
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
