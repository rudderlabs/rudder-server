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

const (
	KlaviyoAPIURL       = "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
	BATCHSIZE           = 10000
	MAXPAYLOADSIZE      = 4900000
	IMPORT_ID_SEPARATOR = ":"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func createFinalPayload(combinedProfiles []Profile, listId string) Payload {
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

func chunkBySizeAndElements(combinedProfiles []Profile, maxBytes, maxElements int) ([][]Profile, error) {
	var chunks [][]Profile
	chunk := make([]Profile, 0)
	var chunkSize int = 0

	for _, profile := range combinedProfiles {
		profileJSON, err := json.Marshal(profile)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal profile: %w", err)
		}

		profileSize := len(profileJSON)

		if (chunkSize+profileSize > maxBytes || chunkSize+profileSize == maxBytes || len(chunk) == maxElements) && len(chunk) > 0 {
			chunks = append(chunks, chunk)
			chunk = make([]Profile, 0)
			chunkSize = 0
		}

		chunk = append(chunk, profile)
		chunkSize += profileSize
	}

	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func (kbu *KlaviyoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	client := &http.Client{}
	destConfig := kbu.destinationConfig
	privateApiKey, _ := destConfig["privateApiKey"].(string)
	importIds := strings.Split(pollInput.ImportId, IMPORT_ID_SEPARATOR)
	importStatuses := make(map[string]string)
	failedImports := make([]string, 0)
	for _, importId := range importIds {
		importStatuses[importId] = "queued"
	}

	for {
		allComplete := true
		for importId, status := range importStatuses {
			if status != "complete" {
				allComplete = false
				pollUrl := KlaviyoAPIURL + importId
				req, err := http.NewRequest("GET", pollUrl, nil)
				if err != nil {
					return common.PollStatusResponse{
						Complete:   true,
						InProgress: false,
						HasFailed:  true,
						Error:      err.Error(),
					}
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Klaviyo-API-Key "+privateApiKey)
				req.Header.Set("revision", "2024-05-15")
				resp, err := client.Do(req)
				if err != nil {
					return common.PollStatusResponse{
						Complete:   true,
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
						Complete:   true,
						InProgress: false,
						StatusCode: 0,
						HasFailed:  true,
						Error:      pollRespErr.Error(),
						HasWarning: false,
					}
				}
				// Update the status in the map
				importStatuses[importId] = pollresp.Data.Attributes.Status

				// If Failed_count > 0, add the importId to failedImports
				if pollresp.Data.Attributes.Failed_count > 0 {
					failedImports = append(failedImports, importId)
				}
			}
		}
		if allComplete {
			break
		}
	}
	if len(failedImports) == 0 {
		return common.PollStatusResponse{
			Complete:      true,
			HasFailed:     false,
			HasWarning:    false,
			StatusCode:    200,
			InProgress:    false,
			FailedJobURLs: "",
		}
	}
	return common.PollStatusResponse{
		Complete:      true,
		HasFailed:     true,
		HasWarning:    false,
		StatusCode:    200,
		InProgress:    false,
		FailedJobURLs: strings.Join(failedImports, IMPORT_ID_SEPARATOR),
	}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	client := &http.Client{}
	destConfig := kbu.destinationConfig
	privateApiKey, _ := destConfig["privateApiKey"].(string)
	pollResultImportIds := strings.Split(UploadStatsInput.FailedJobURLs, IMPORT_ID_SEPARATOR)

	// make a map of jobId to error reason
	jobIdToErrorMap := make(map[int64]string)

	importingList := UploadStatsInput.ImportingList
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}

	ErrorMap := kbu.jobIdToIdentifierMap
	var successKeys []int64

	var failedJobIds []int64
	for _, pollResultImportId := range pollResultImportIds {
		importErrorUrl := KlaviyoAPIURL + pollResultImportId + "/import-errors"
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
		var uploadStatsResp UploadStatusResp
		uploadStatsBodyBytes, _ = io.ReadAll(resp.Body)
		defer func() { _ = resp.Body.Close() }()

		uploadStatsBodyBytesErr := json.Unmarshal(uploadStatsBodyBytes, &uploadStatsResp)
		if uploadStatsBodyBytesErr != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 400,
				Error:      uploadStatsBodyBytesErr.Error(),
			}
		}
		// Iterate over the Data array and get the jobId and error detail and store in jobIdToErrorMap
		for _, item := range uploadStatsResp.Data {
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
	}
	successKeys, _ = lo.Difference(jobIDs, failedJobIds)
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

func (kbu *KlaviyoBulkUploader) ExtractProfile(input Input) Profile {
	Message := input.Message
	Body := Message.Body
	Json := Body.JSON
	Data := Json.Data
	Attributes := Data.Attributes
	if len(Attributes.Profiles.Data) == 0 {
		return Profile{}
	}
	profileObject := Attributes.Profiles.Data[0]

	jobIdentifier := profileObject.Attributes.JobIdentifier
	jobIdentifierArray := strings.Split(jobIdentifier, ":")
	jobIdentifierValue, _ := strconv.ParseInt(jobIdentifierArray[1], 10, 64)
	if kbu.jobIdToIdentifierMap == nil {
		kbu.jobIdToIdentifierMap = make(map[string]int64)
	}
	kbu.jobIdToIdentifierMap[jobIdentifierArray[0]] = jobIdentifierValue

	// delete jobIdentifier from the attributes map as it is not required in the final payload
	profileObject.Attributes.JobIdentifier = ""

	return profileObject
}

func (kbu *KlaviyoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	startTime := time.Now()
	destination := asyncDestStruct.Destination
	var failedJobs []int64
	var successJobs []int64
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
	var combinedProfiles []Profile
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var input Input
		line := scanner.Text()
		err := json.Unmarshal([]byte(line), &input)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON.", err, importingJobIDs, destinationID)
		}
		profileStructure := kbu.ExtractProfile(input)
		combinedProfiles = append(combinedProfiles, profileStructure)
	}

	chunks, _ := chunkBySizeAndElements(combinedProfiles, MAXPAYLOADSIZE, BATCHSIZE)

	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	var importIds []string // DelimitedImportIds is : separated importIds
	var DelimitedUploadRespErr string

	for idx, chunk := range chunks {
		combinedPayload := createFinalPayload(chunk, listId)

		// Convert combined payload to JSON
		outputJSON, err := json.Marshal(combinedPayload)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while marshaling combined JSON.", err, importingJobIDs, destinationID)
		}
		uploadURL := KlaviyoAPIURL
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
		payloadSizeStat.Observe(float64(len(outputJSON)))

		resp, err := client.Do(req)
		if err != nil {
			failedJobs = append(failedJobs, importingJobIDs[idx])
			kbu.logger.Error("Error while sending request.", err)
		}

		var bodyBytes []byte
		bodyBytes, _ = io.ReadAll(resp.Body)
		defer func() { _ = resp.Body.Close() }()
		uploadTimeStat.Since(startTime)

		if resp.StatusCode != 202 {
			failedJobs = append(failedJobs, importingJobIDs[idx])
			kbu.logger.Error("Got non 202 as statusCode.", fmt.Errorf(string(bodyBytes)))
		}
		var uploadresp UploadResp
		uploadRespErr := json.Unmarshal((bodyBytes), &uploadresp)
		if uploadRespErr != nil {
			failedJobs = append(failedJobs, importingJobIDs[idx])
			kbu.logger.Error("Error while unmarshaling response.", uploadRespErr)
		}
		importIds = append(importIds, uploadresp.Data.Id)
	}
	importParameters, err := json.Marshal(common.ImportParameters{
		ImportId: strings.Join(importIds, IMPORT_ID_SEPARATOR),
	})
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling parameters.", err, importingJobIDs, destinationID)
	}
	successJobs, _ = lo.Difference(importingJobIDs, failedJobs)
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))

	return common.AsyncUploadOutput{
		ImportingParameters: importParameters,
		SucceededJobIDs:     successJobs,
		FailedJobIDs:        failedJobs,
		FailedCount:         len(failedJobs),
		ImportingJobIDs:     successJobs,
		SuccessResponse:     DelimitedUploadRespErr,
		DestinationID:       destination.ID,
	}
}

func (kbu *KlaviyoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}
