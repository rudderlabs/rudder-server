package klaviyobulkupload

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
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
	ambiguousResultReason = "cannot reconcile Klaviyo import result: missing, duplicate or unmatched correlation data; profile may already have been imported"
)

// importingMetadata is persisted on each accepted job's importing status. Only
// the typed hash of an identifier actually sent to Klaviyo is stored (not PII).
type importingMetadata struct {
	IdentifierHash string `json:"identifierHash"`
	ChunkImportID  string `json:"chunkImportId"`
}

func hashIdentifier(kind, value string) string {
	sum := sha256.Sum256([]byte(kind + ":" + value))
	return hex.EncodeToString(sum[:])
}

// Prefer id when Klaviyo sends one; otherwise use a profile attribute actually
// serialized in the outbound request. anonymous_id/jobIdentifier is not sent.
func profileIdentifier(profile Profile) (string, string) {
	return originalIdentifier(profile.ID, profile.Attributes.Email, profile.Attributes.Phone, profile.Attributes.ExternalId)
}

func originalIdentifier(profileID, email, phone, externalID string) (string, string) {
	switch {
	case profileID != "":
		return "id", profileID
	case email != "":
		return "email", email
	case phone != "":
		return "phone_number", phone
	case externalID != "":
		return "external_id", externalID
	default:
		return "", ""
	}
}

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

	if len(importStatuses) == 0 {
		return common.PollStatusResponse{StatusCode: http.StatusInternalServerError, HasFailed: true, Error: "Klaviyo import ID is missing"}
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

				// Carry terminal counts to stats for every chunk, including clean ones.
				if pollresp.Data.Attributes.Status == "complete" {
					failedImports = append(failedImports, fmt.Sprintf("%s=%d,%d,%d", importId, pollresp.Data.Attributes.Failed_count, pollresp.Data.Attributes.Completed_count, pollresp.Data.Attributes.Total_count))
				}
			}
		}
		if allComplete {
			break
		}
	}
	// Always enter GetUploadStats, including for clean imports: status params
	// may be missing on some jobs, and the router's clean fast path skips them.
	return common.PollStatusResponse{
		Complete:            true,
		HasFailed:           true,
		StatusCode:          http.StatusOK,
		FailedJobParameters: strings.Join(failedImports, IMPORT_ID_SEPARATOR),
	}
}

func (kbu *KlaviyoBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	strict := config.GetBoolVar(true, "BatchRouter.KLAVIYO_BULK_UPLOAD.strictResultReconciliation")
	metadata := common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	pollImports := make(map[string]bool)
	for id := range strings.SplitSeq(gjson.GetBytes(input.Parameters, "importId").String(), IMPORT_ID_SEPARATOR) {
		if id != "" {
			pollImports[id] = true
		}
	}
	sharedImportID := gjson.GetBytes(input.Parameters, "importId").String()
	sharedImportCount := gjson.GetBytes(input.Parameters, "importCount").Int()
	byImport := make(map[string]map[string]int64)
	chunkJobs := make(map[string][]int64)
	ambiguous := make(map[int64]bool)
	duplicateHashes := make(map[string]map[string]bool)
	for _, job := range input.ImportingList {
		params := job.LastJobStatus.Parameters
		importID := gjson.GetBytes(params, "metadata.chunkImportId").String()
		chunkJobs[importID] = append(chunkJobs[importID], job.JobID)
		hash := gjson.GetBytes(params, "metadata.identifierHash").String()
		if importID == "" || hash == "" || len(hash) != 64 || !pollImports[importID] || gjson.GetBytes(params, "importId").String() != sharedImportID || !gjson.GetBytes(params, "importCount").Exists() || gjson.GetBytes(params, "importCount").Int() != sharedImportCount {
			ambiguous[job.JobID] = true
			continue
		}
		if byImport[importID] == nil {
			byImport[importID] = make(map[string]int64)
		}
		if _, exists := byImport[importID][hash]; exists {
			if duplicateHashes[importID] == nil {
				duplicateHashes[importID] = make(map[string]bool)
			}
			duplicateHashes[importID][hash] = true
		} else {
			byImport[importID][hash] = job.JobID
		}
	}

	counts, invalidCounts := parseChunkCounts(input.FailedJobParameters, pollImports)
	allAmbiguous := invalidCounts || len(pollImports) == 0 && len(input.ImportingList) > 0
	if expected := int(sharedImportCount); expected != len(input.ImportingList) {
		return common.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: fmt.Sprintf("Klaviyo importing list incomplete: got %d, expected %d", len(input.ImportingList), expected)}
	}
	for importID := range pollImports {
		count, ok := counts[importID]
		if !ok || len(chunkJobs[importID]) == 0 || count.total != len(chunkJobs[importID]) || count.completed+count.failed != count.total || (count.failed > 0 && len(byImport[importID]) == 0) {
			if len(chunkJobs[importID]) == 0 {
				allAmbiguous = true
			} else {
				for _, jobID := range chunkJobs[importID] {
					ambiguous[jobID] = true
				}
			}
		}
	}
	// Fetch error rows only for failed chunks; clean chunks still require
	// complete terminal counts and persisted correlation metadata.
	seen, ambiguousChunks, err := kbu.reconcileChunkErrors(counts, pollImports, chunkJobs, byImport, duplicateHashes, ambiguous, strict, &metadata)
	if err != nil {
		return common.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: err.Error()}
	}
	ambiguousCount := classifyChunkJobs(input.ImportingList, &metadata, seen, ambiguous, ambiguousChunks, allAmbiguous)
	if ambiguousCount > 0 {
		kbu.Logger.Errorn("Ambiguous Klaviyo import reconciliation", logger.NewIntField("jobs", int64(ambiguousCount)))
		kbu.StatsFactory.NewTaggedStat("klaviyo_result_reconciliation_ambiguous", stats.CountType, stats.Tags{
			"module": "batch_router", "destType": "KLAVIYO_BULK_UPLOAD",
		}).Count(ambiguousCount)
	}
	return common.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: metadata}
}

func (kbu *KlaviyoBulkUploader) reconcileChunkErrors(counts map[string]chunkCounts, pollImports map[string]bool, chunkJobs map[string][]int64, byImport map[string]map[string]int64, duplicateHashes map[string]map[string]bool, ambiguous map[int64]bool, strict bool, metadata *common.EventStatMeta) (map[int64]bool, map[string]bool, error) {
	seen := make(map[int64]bool)
	ambiguousChunks := make(map[string]bool)
	for importID, count := range counts {
		if !pollImports[importID] || count.failed == 0 {
			continue
		}
		// Strict policy refuses to infer outcomes for failed chunks with
		// repeated identifiers, even when errors name other jobs.
		if strict && len(duplicateHashes[importID]) > 0 {
			ambiguousChunks[importID] = true
		}
		resp, err := kbu.KlaviyoAPIService.GetUploadErrors(importID)
		if err != nil || resp == nil {
			return nil, nil, fmt.Errorf("fetching Klaviyo import errors for %s: %v", importID, err)
		}
		if len(resp.Data) != count.failed || count.failed > len(chunkJobs[importID]) || resp.Links.Next != "" {
			ambiguousChunks[importID] = true
		}
		for _, item := range resp.Data {
			// Only correlate fields serialized on the outbound profile.
			kind, value := originalIdentifier(item.Attributes.OriginalPayload.Id, item.Attributes.OriginalPayload.Attributes.Email, item.Attributes.OriginalPayload.Attributes.Phone, item.Attributes.OriginalPayload.Attributes.ExternalId)
			hash := hashIdentifier(kind, value)
			jobID, ok := byImport[importID][hash]
			if kind == "" || !ok || duplicateHashes[importID][hash] || seen[jobID] || ambiguous[jobID] {
				ambiguousChunks[importID] = true
				continue
			}
			seen[jobID] = true
			metadata.AbortedReasons[jobID] = item.Attributes.Detail
		}
	}
	return seen, ambiguousChunks, nil
}

type chunkCounts struct{ failed, completed, total int }

func parseChunkCounts(parameters string, pollImports map[string]bool) (map[string]chunkCounts, bool) {
	counts := make(map[string]chunkCounts)
	invalid := false
	for token := range strings.SplitSeq(parameters, IMPORT_ID_SEPARATOR) {
		if token == "" {
			continue
		}
		id, text, ok := strings.Cut(token, "=")
		if !ok || !pollImports[id] {
			invalid = true
			continue
		}
		parts := strings.Split(text, ",")
		if len(parts) != 3 {
			invalid = true
			continue
		}
		values := [3]int{}
		valid := true
		for i, part := range parts {
			value, err := strconv.Atoi(part)
			if err != nil || value < 0 {
				valid = false
				break
			}
			values[i] = value
		}
		if _, duplicate := counts[id]; duplicate || !valid {
			invalid = true
			continue
		}
		counts[id] = chunkCounts{values[0], values[1], values[2]}
	}
	return counts, invalid
}

func classifyChunkJobs(jobs []*jobsdb.JobT, metadata *common.EventStatMeta, seen, ambiguous map[int64]bool, ambiguousChunks map[string]bool, allAmbiguous bool) int {
	ambiguousCount := 0
	for _, job := range jobs {
		id := job.JobID
		if allAmbiguous || ambiguous[id] || ambiguousChunks[gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.chunkImportId").String()] {
			ambiguousCount++
			metadata.AbortedKeys = append(metadata.AbortedKeys, id)
			metadata.AbortedReasons[id] = ambiguousResultReason
		} else if seen[id] {
			metadata.AbortedKeys = append(metadata.AbortedKeys, id)
		} else {
			metadata.SucceededKeys = append(metadata.SucceededKeys, id)
		}
	}
	return ambiguousCount
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

func (kbu *KlaviyoBulkUploader) ExtractProfile(data Data) Profile {
	if len(data.Attributes.Profiles.Data) == 0 {
		return Profile{}
	}
	profile := data.Attributes.Profiles.Data[0]
	// jobIdentifier is transformer-only metadata; it is not an outbound key.
	profile.Attributes.JobIdentifier = ""
	return profile
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
	jobHashes := make(map[int64]string)
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
		kind, value := profileIdentifier(profileStructure)
		if kind == "" {
			kbu.Logger.Errorn("Missing Klaviyo outbound correlation identifier", logger.NewIntField("jobId", int64(metadata.JobID)))
			kbu.StatsFactory.NewTaggedStat("klaviyo_result_reconciliation_ambiguous", stats.CountType, statLabels).Count(1)
			abortedJobs = append(abortedJobs, int64(metadata.JobID))
			abortReason = "profile has no outbound Klaviyo correlation identifier"
			continue
		}
		jobHashes[int64(metadata.JobID)] = hashIdentifier(kind, value)
		combinedProfiles = append(combinedProfiles, profileStructure)
		jobIDsForProfiles = append(jobIDsForProfiles, int64(metadata.JobID))
	}
	if err := scanner.Err(); err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while scanning profiles.", err, importingJobIDs, destinationID)
	}

	// Duplicate identifiers are valid profile updates. Reconcile each chunk
	// independently using its import ID and terminal counts.
	profileChunks, jobIDChunks, _ := chunkBySizeAndElements(combinedProfiles, jobIDsForProfiles, kbu.MaxPayloadSize, kbu.BatchSize)

	eventsSuccessStat := kbu.StatsFactory.NewTaggedStat("success_job_count", stats.CountType, statLabels)

	var importIds []string // DelimitedImportIds is : separated importIds
	jobImportingParameters := make(map[int64]stdjson.RawMessage)

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
			for _, jobID := range chunkResult.acceptedJobIDs {
				jobImportingParameters[jobID], err = jsonrs.Marshal(importingMetadata{IdentifierHash: jobHashes[jobID], ChunkImportID: chunkResult.importID})
				if err != nil {
					return kbu.generateKlaviyoErrorOutput("Error while marshaling job metadata.", err, importingJobIDs, destinationID)
				}
			}
		}
	}

	successJobs, _ = lo.Difference(importingJobIDs, append(failedJobs, abortedJobs...))
	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId:    strings.Join(importIds, IMPORT_ID_SEPARATOR),
		ImportCount: len(successJobs),
	})
	if err != nil {
		return kbu.generateKlaviyoErrorOutput("Error while marshaling parameters.", err, importingJobIDs, destinationID)
	}
	eventsSuccessStat.Count(len(asyncDestStruct.ImportingJobIDs))

	return common.AsyncUploadOutput{
		ImportingParameters:    importParameters,
		JobImportingParameters: jobImportingParameters,
		FailedJobIDs:           failedJobs,
		FailedReason:           failedReason,
		FailedCount:            len(failedJobs),
		AbortJobIDs:            abortedJobs,
		AbortReason:            abortReason,
		AbortCount:             len(abortedJobs),
		ImportingJobIDs:        successJobs,
		ImportingCount:         len(successJobs),
		DestinationID:          destination.ID,
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
		res.acceptedJobIDs = jobIDs
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
		res.acceptedJobIDs = validJobIDs
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
	return extractInvalidProfileIndices(resp.Errors)
}

// extractInvalidProfileIndices inspects the errors returned by a synchronous 400
// from Klaviyo and maps each offending profile's index (within the uploaded
// payload) to its error detail. The second return value is true only when there
// is at least one error and every error was attributed to a specific profile. If
// any error is structural (e.g. an invalid list ID, which has no profile pointer)
// it returns false so the caller aborts the whole chunk instead of retrying blindly.
func extractInvalidProfileIndices(errs ErrorDetailList) (map[int]string, bool) {
	indices := make(map[int]string)
	for _, e := range errs {
		match := profileIndexRegex.FindStringSubmatch(e.Source.Pointer)
		if match == nil {
			return nil, false
		}
		idx, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, false
		}
		indices[idx] = e.Detail
	}
	return indices, len(indices) > 0
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
