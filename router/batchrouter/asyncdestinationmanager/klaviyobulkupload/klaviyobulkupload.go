package klaviyobulkupload

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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

// profileIndexRegex extracts the profile index from a Klaviyo error pointer such
// as "/data/attributes/profiles/data/3/attributes/email".
var profileIndexRegex = regexp.MustCompile(`/profiles/data/(\d+)`)

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
		DestName:              destination.DestinationDefinition.Name,
		DestinationConfig:     destination.Config,
		Logger:                klaviyoLogger,
		StatsFactory:          StatsFactory,
		KlaviyoAPIService:     apiService,
		BatchSize:             BATCHSIZE,
		MaxPayloadSize:        MAXPAYLOADSIZE,
		MaxAllowedProfileSize: MAXALLOWEDPROFILESIZE,
	}, nil
}

func chunkBySizeAndElements(combinedProfiles []Profile, jobIDs []int64, maxBytes, maxElements int) ([][]Profile, [][]int64, error) {
	var profileChunks [][]Profile
	var jobIDChunks [][]int64
	profileChunk := make([]Profile, 0)
	jobIDChunk := make([]int64, 0)
	chunkSize := 0

	for idx, profile := range combinedProfiles {
		profileJSON, err := jsonrs.Marshal(profile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal profile: %w", err)
		}

		profileSize := len(profileJSON) + 1 // +1 for comma character

		if (chunkSize+profileSize >= maxBytes || len(profileChunk) == maxElements) && len(profileChunk) > 0 {
			profileChunks = append(profileChunks, profileChunk)
			jobIDChunks = append(jobIDChunks, jobIDChunk)
			profileChunk = make([]Profile, 0)
			jobIDChunk = make([]int64, 0)
			chunkSize = 0
		}

		profileChunk = append(profileChunk, profile)
		jobIDChunk = append(jobIDChunk, jobIDs[idx])
		chunkSize += profileSize
	}

	if len(profileChunk) > 0 {
		profileChunks = append(profileChunks, profileChunk)
		jobIDChunks = append(jobIDChunks, jobIDChunk)
	}

	return profileChunks, jobIDChunks, nil
}

func (kbu *KlaviyoBulkUploader) Poll(_ context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
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

func (kbu *KlaviyoBulkUploader) Upload(_ context.Context, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	var failedJobs []int64
	var failedReason string
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
	var jobIDsForProfiles []int64 // Track job IDs for each profile
	scanner := bufio.NewScanner(file)
	profileSizeStat := kbu.StatsFactory.NewTaggedStat("profile_size", stats.HistogramType, statLabels)
	for scanner.Scan() {
		var data Data
		var metadata Metadata
		line := scanner.Text()

		err := jsonrs.Unmarshal([]byte(gjson.Get(line, "message.body.JSON.data").String()), &data)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON Data.", err, importingJobIDs, destinationID)
		}
		err = jsonrs.Unmarshal([]byte(gjson.Get(line, "metadata").String()), &metadata)
		if err != nil {
			return kbu.generateKlaviyoErrorOutput("Error while parsing JSON Metadata.", err, importingJobIDs, destinationID)
		}
		profileStructure := kbu.ExtractProfile(data)
		// if profileStructure length is more than 500 kB, throw an error
		profileStructureJSON, _ := jsonrs.Marshal(profileStructure)
		profileSize := float64(len(profileStructureJSON))
		profileSizeStat.Observe(profileSize) // Record the size in the histogram
		if len(profileStructureJSON) >= kbu.MaxAllowedProfileSize {
			abortReason = "Error while marshaling profiles. The profile size exceeds Klaviyo's limit of 500 kB for a single profile."
			abortedJobs = append(abortedJobs, int64(metadata.JobID))
			continue
		}
		combinedProfiles = append(combinedProfiles, profileStructure)
		jobIDsForProfiles = append(jobIDsForProfiles, int64(metadata.JobID))
	}

	profileChunks, jobIDChunks, _ := chunkBySizeAndElements(combinedProfiles, jobIDsForProfiles, kbu.MaxPayloadSize, kbu.BatchSize)

	eventsSuccessStat := kbu.StatsFactory.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	var importIds []string // DelimitedImportIds is : separated importIds

	for idx, profileChunk := range profileChunks {
		chunkResult := kbu.uploadChunk(profileChunk, jobIDChunks[idx], listId, destinationID)
		if len(chunkResult.abortedJobs) > 0 {
			abortedJobs = append(abortedJobs, chunkResult.abortedJobs...)
			abortReason = chunkResult.abortReason
		}
		if len(chunkResult.failedJobs) > 0 {
			failedJobs = append(failedJobs, chunkResult.failedJobs...)
			failedReason = chunkResult.failReason
		}
		if chunkResult.importID != "" {
			importIds = append(importIds, chunkResult.importID)
		}
	}
	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId:    strings.Join(importIds, IMPORT_ID_SEPARATOR),
		ImportCount: len(successJobs),
	})
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling parameters.", err, importingJobIDs, destinationID)
	}
	successJobs, _ = lo.Difference(importingJobIDs, append(failedJobs, abortedJobs...))
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))

	return common.AsyncUploadOutput{
		ImportingParameters: importParameters,
		FailedJobIDs:        failedJobs,
		FailedReason:        failedReason,
		FailedCount:         len(failedJobs),
		AbortJobIDs:         abortedJobs,
		AbortReason:         abortReason,
		AbortCount:          len(abortedJobs),
		ImportingJobIDs:     successJobs,
		ImportingCount:      len(successJobs),
		DestinationID:       destination.ID,
	}
}

// uploadChunk uploads a single chunk of profiles. Klaviyo validates a chunk
// all-or-nothing, so one invalid profile fails the whole batch with a 400. On a
// 400 that pinpoints specific profiles, it aborts only those and retries ONCE
// with the rest. There is no further looping: the single retry's outcome is
// final. A 400 it can't attribute (e.g. an invalid list ID) and non-4xx errors
// abort/fail the whole chunk respectively.
func (kbu *KlaviyoBulkUploader) uploadChunk(profiles []Profile, jobIDs []int64, listId, destinationID string) chunkUploadResult {
	var res chunkUploadResult

	// First attempt with the full chunk.
	resp, statusCode, detail, ok := kbu.tryUpload(profiles, listId, destinationID)
	if ok {
		res.importID = resp.Data.Id
		return res
	}

	badIndices, canStrip := strippableProfiles(resp, statusCode)
	if !canStrip {
		res.recordError(jobIDs, statusCode, detail)
		return res
	}

	// Abort the invalid profiles; keep the rest for a single retry.
	var validProfiles []Profile
	var validJobIDs []int64
	for i := range profiles {
		if reason, bad := badIndices[i]; bad {
			res.abortedJobs = append(res.abortedJobs, jobIDs[i])
			res.abortReason = fmt.Sprintf("profile rejected by Klaviyo's synchronous validation: %s", reason)
			continue
		}
		validProfiles = append(validProfiles, profiles[i])
		validJobIDs = append(validJobIDs, jobIDs[i])
	}
	if len(validProfiles) == 0 {
		return res // every profile was rejected; all already aborted
	}

	// Single retry with the surviving profiles; its outcome is final.
	retryResp, retryStatus, retryDetail, ok := kbu.tryUpload(validProfiles, listId, destinationID)
	if ok {
		res.importID = retryResp.Data.Id
		return res
	}
	res.recordError(validJobIDs, retryStatus, retryDetail)
	return res
}

// tryUpload performs a single upload attempt, logging and extracting the status
// code and error detail on failure. resp may be nil on a transport error.
func (kbu *KlaviyoBulkUploader) tryUpload(profiles []Profile, listId, destinationID string) (resp *UploadResp, statusCode int, detail string, ok bool) {
	resp, err := kbu.KlaviyoAPIService.UploadProfiles(createFinalPayload(profiles, listId))
	if err == nil {
		return resp, 0, "", true
	}
	if resp != nil {
		statusCode = resp.StatusCode
		if len(resp.Errors) > 0 {
			detail = resp.Errors.String()
		}
	}
	kbu.Logger.Errorn("Error while uploading profiles",
		obskit.Error(err),
		obskit.DestinationID(destinationID),
		logger.NewStringField("uploadErrors", detail))
	return resp, statusCode, detail, false
}

// strippableProfiles returns the rejected profiles' indices (mapped to their error
// detail) only when the failure is a synchronous 400 that attributes every error
// to a specific profile. For any other failure (transport/5xx/429, a 4xx we can't
// attribute, or no per-profile errors) it returns ok=false, telling the caller to
// abort/fail the whole chunk rather than retry.
func strippableProfiles(resp *UploadResp, statusCode int) (map[int]string, bool) {
	if resp == nil || !isClientError(statusCode) {
		return nil, false
	}
	indices, allAttributed := extractInvalidProfileIndices(resp.Errors)
	if !allAttributed || len(indices) == 0 {
		return nil, false
	}
	return indices, true
}

// extractInvalidProfileIndices inspects the errors returned by a synchronous 400
// from Klaviyo and maps each offending profile's index (within the uploaded
// payload) to its error detail. The second return value is true only when every
// error could be attributed to a specific profile; if any error is structural
// (e.g. an invalid list ID, which has no profile pointer) it returns false so the
// caller can fall back to aborting the whole chunk instead of retrying blindly.
func extractInvalidProfileIndices(errs ErrorDetailList) (map[int]string, bool) {
	indices := make(map[int]string)
	allAttributed := true
	for _, e := range errs {
		match := profileIndexRegex.FindStringSubmatch(e.Source.Pointer)
		if match == nil {
			allAttributed = false
			continue
		}
		idx, err := strconv.Atoi(match[1])
		if err != nil {
			allAttributed = false
			continue
		}
		indices[idx] = e.Detail
	}
	return indices, allAttributed
}

// isClientError reports whether a status code is a non-retryable 4xx (excluding
// 429, which is retryable rate limiting).
func isClientError(statusCode int) bool {
	return statusCode >= http.StatusBadRequest && statusCode < http.StatusInternalServerError && statusCode != http.StatusTooManyRequests
}

// recordError classifies a set of jobs against a failed upload: a non-retryable
// 4xx aborts them, anything else (5xx/429/transport) is reported as failed so the
// batch router retries.
func (r *chunkUploadResult) recordError(jobIDs []int64, statusCode int, detail string) {
	if isClientError(statusCode) {
		r.abortedJobs = append(r.abortedJobs, jobIDs...)
		r.abortReason = fmt.Sprintf("upload rejected by Klaviyo with status %d: %s", statusCode, detail)
	} else {
		r.failedJobs = append(r.failedJobs, jobIDs...)
		r.failReason = fmt.Sprintf("upload failed with status %d: %s", statusCode, detail)
	}
}

func (kbu *KlaviyoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}
