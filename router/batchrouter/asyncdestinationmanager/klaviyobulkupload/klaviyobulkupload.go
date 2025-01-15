package klaviyobulkupload

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

const (
	BATCHSIZE             = 10000
	MAXALLOWEDPROFILESIZE = 512000
	MAXPAYLOADSIZE        = 4600000
	IMPORT_ID_SEPARATOR   = ":"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func createFinalPayload(combinedProfiles []Profile, listId string) Payload {
	payload := Payload{
		Data: Data{
			Type: "profile-bulk-import-job",
			Attributes: PayloadAttributes{
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

func NewManager(logger logger.Logger, StatsFactory stats.Stats, destination *backendconfig.DestinationT) (*KlaviyoBulkUploader, error) {
	klaviyoLogger := logger.Child("KlaviyoBulkUpload").Child("KlaviyoBulkUploader")
	apiService, err := NewKlaviyoAPIService(destination, klaviyoLogger, StatsFactory)
	if err != nil {
		return nil, err
	}
	return &KlaviyoBulkUploader{
		DestName:          destination.DestinationDefinition.Name,
		DestinationConfig: destination.Config,
		Logger:            klaviyoLogger,
		StatsFactory:      StatsFactory,
		KlaviyoAPIService: apiService,
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

		profileSize := len(profileJSON) + 1 // +1 for comma character

		if (chunkSize+profileSize >= maxBytes || len(chunk) == maxElements) && len(chunk) > 0 {
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
	importIds := strings.Split(pollInput.ImportId, IMPORT_ID_SEPARATOR)
	importStatuses := make(map[string]string)
	failedImports := make([]string, 0)
	for _, importId := range importIds {
		if importId != "" {
			importStatuses[importId] = "queued"
		}
	}

	for {
		allComplete := true
		for importId, status := range importStatuses {
			if status != "complete" {
				allComplete = false
				pollresp, err := kbu.KlaviyoAPIService.GetUploadStatus(importId)
				if err != nil {
					kbu.Logger.Errorn("Error during fetching Klaviyo Bulk Upload status", obskit.Error(err))
					return common.PollStatusResponse{
						StatusCode: 500,
						Complete:   true,
						HasFailed:  true,
						Error:      `Error during fetching upload status ` + err.Error(),
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
			Complete:            true,
			HasFailed:           false,
			HasWarning:          false,
			StatusCode:          200,
			InProgress:          false,
			FailedJobParameters: "",
		}
	}
	return common.PollStatusResponse{
		Complete:            true,
		HasFailed:           true,
		HasWarning:          false,
		StatusCode:          200,
		InProgress:          false,
		FailedJobParameters: strings.Join(failedImports, IMPORT_ID_SEPARATOR),
	}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	pollResultImportIds := strings.Split(UploadStatsInput.FailedJobParameters, IMPORT_ID_SEPARATOR)
	// make a map of jobId to error reason
	jobIdToErrorMap := make(map[int64]string)

	importingList := UploadStatsInput.ImportingList
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}

	ErrorMap := kbu.JobIdToIdentifierMap
	var successKeys []int64

	var abortedJobIDs []int64
	for _, pollResultImportId := range pollResultImportIds {
		uploadStatsResp, err := kbu.KlaviyoAPIService.GetUploadErrors(pollResultImportId)
		if err != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 400,
				Error:      err.Error(),
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
			abortedJobIDs = append(abortedJobIDs, jobId)
			errorDetail := item.Attributes.Detail
			jobIdToErrorMap[jobId] = errorDetail
		}
	}
	successKeys, _ = lo.Difference(jobIDs, abortedJobIDs)
	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Error:      "The import job failed",
		Metadata: common.EventStatMeta{
			AbortedKeys:    abortedJobIDs,
			AbortedReasons: jobIdToErrorMap,
			SucceededKeys:  successKeys,
		},
	}
}

func (kbu *KlaviyoBulkUploader) generateKlaviyoErrorOutput(errorString string, err error, importingJobIds []int64, destinationID string) common.AsyncUploadOutput {
	eventsAbortedStat := kbu.StatsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
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

func (kbu *KlaviyoBulkUploader) ExtractProfile(Data Data) Profile {
	Attributes := Data.Attributes
	if len(Attributes.Profiles.Data) == 0 {
		return Profile{}
	}
	profileObject := Attributes.Profiles.Data[0]

	jobIdentifier := profileObject.Attributes.JobIdentifier
	jobIdentifierArray := strings.Split(jobIdentifier, ":")
	jobIdentifierValue, _ := strconv.ParseInt(jobIdentifierArray[1], 10, 64)
	if kbu.JobIdToIdentifierMap == nil {
		kbu.JobIdToIdentifierMap = make(map[string]int64)
	}
	kbu.JobIdToIdentifierMap[jobIdentifierArray[0]] = jobIdentifierValue

	// delete jobIdentifier from the attributes map as it is not required in the final payload
	profileObject.Attributes.JobIdentifier = ""

	return profileObject
}

func (kbu *KlaviyoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	var failedJobs []int64
	var abortedJobs []int64
	var abortReason string
	var successJobs []int64
	filePath := asyncDestStruct.FileName
	importingJobIDs := asyncDestStruct.ImportingJobIDs
	destType := destination.DestinationDefinition.Name
	destinationID := destination.ID
	listId, _ := destination.Config["listId"].(string)
	statLabels := stats.Tags{
		"module":   "batch_router",
		"destType": destType,
		"destID":   destinationID,
	}
	file, err := os.Open(filePath)
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while opening file. ", err, asyncDestStruct.ImportingJobIDs, destinationID)
	}
	defer file.Close()
	var combinedProfiles []Profile
	scanner := bufio.NewScanner(file)
	profileSizeStat := kbu.StatsFactory.NewTaggedStat("profile_size", stats.HistogramType, statLabels)
	for scanner.Scan() {
		var data Data
		var metadata Metadata
		line := scanner.Text()

		err := json.Unmarshal([]byte(gjson.Get(line, "message.body.JSON.data").String()), &data)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON Data.", err, importingJobIDs, destinationID)
		}
		err = json.Unmarshal([]byte(gjson.Get(line, "metadata").String()), &metadata)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON Metadata.", err, importingJobIDs, destinationID)
		}
		profileStructure := kbu.ExtractProfile(data)
		// if profileStructure length is more than 500 kB, throw an error
		profileStructureJSON, _ := json.Marshal(profileStructure)
		profileSize := float64(len(profileStructureJSON))
		profileSizeStat.Observe(profileSize) // Record the size in the histogram
		if len(profileStructureJSON) >= MAXALLOWEDPROFILESIZE {
			abortReason = "Error while marshaling profiles. The profile size exceeds Klaviyo's limit of 500 kB for a single profile."
			abortedJobs = append(abortedJobs, int64(metadata.JobID))
			continue
		}
		combinedProfiles = append(combinedProfiles, profileStructure)
	}

	chunks, _ := chunkBySizeAndElements(combinedProfiles, MAXPAYLOADSIZE, BATCHSIZE)

	eventsSuccessStat := kbu.StatsFactory.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	var importIds []string // DelimitedImportIds is : separated importIds

	for idx, chunk := range chunks {
		combinedPayload := createFinalPayload(chunk, listId)
		uploadResp, err := kbu.KlaviyoAPIService.UploadProfiles(combinedPayload)
		if err != nil {
			failedJobs = append(failedJobs, importingJobIDs[idx])
			kbu.Logger.Error("Error while uploading profiles", err, uploadResp.Errors, destinationID)
			continue
		}

		importIds = append(importIds, uploadResp.Data.Id)
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
		FailedJobIDs:        failedJobs,
		AbortJobIDs:         abortedJobs,
		AbortReason:         abortReason,
		FailedCount:         len(failedJobs),
		ImportingJobIDs:     successJobs,
		DestinationID:       destination.ID,
	}
}

func (kbu *KlaviyoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}
