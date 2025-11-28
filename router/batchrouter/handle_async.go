package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
	common "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

func (brt *Handle) getImportingJobs(ctx context.Context, destinationID string, limit int) (jobsdb.JobsResult, error) {
	parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
	return misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
		return brt.jobsDB.GetImporting(
			ctx,
			jobsdb.GetQueryParams{
				CustomValFilters: []string{brt.destType},
				JobsLimit:        limit,
				ParameterFilters: parameterFilters,
				PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit.Load()),
			},
		)
	}, brt.sendQueryRetryStats)
}

func (brt *Handle) updateJobStatuses(ctx context.Context, destinationID string, allJobs, completedJobs []*jobsdb.JobT, statusList []*jobsdb.JobStatusT) error {
	reportMetrics := brt.getReportMetrics(getReportMetricsParams{
		StatusList:    statusList,
		ParametersMap: brt.getParamertsFromJobs(allJobs),
		JobsList:      allJobs,
	})

	parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
	return misc.RetryWithNotify(ctx, brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
			if err != nil {
				return fmt.Errorf("updating %s job statuses: %w", brt.destType, err)
			}

			// rsources stats
			err = brt.updateRudderSourcesStats(ctx, tx, completedJobs, statusList)
			if err != nil {
				return err
			}

			if brt.reporting != nil && brt.reportingEnabled {
				if err = brt.reporting.Report(ctx, reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
			}
			tx.Tx().AddSuccessListener(func() {
				for _, job := range completedJobs {
					brt.pendingEventsRegistry.DecreasePendingEvents("batch_rt", job.WorkspaceId, brt.destType, float64(1))
				}
			})
			return nil
		})
	}, brt.sendRetryUpdateStats)
}

func getPollInput(job *jobsdb.JobT) common.AsyncPoll {
	parameters := job.LastJobStatus.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	return common.AsyncPoll{ImportId: importId}
}

func enhanceResponseWithFirstAttemptedAt(msg stdjson.RawMessage, resp []byte) []byte {
	return routerutils.EnhanceJsonWithTime(getFirstAttemptAtFromErrorResponse(msg), "firstAttemptedAt", resp)
}

func getFirstAttemptAtFromErrorResponse(msg stdjson.RawMessage) time.Time {
	res := time.Now()
	if firstAttemptedAtString := gjson.GetBytes(msg, "firstAttemptedAt").Str; firstAttemptedAtString != "" {
		if firstAttemptedAt, err := time.Parse(misc.RFC3339Milli, firstAttemptedAtString); err == nil {
			res = firstAttemptedAt
		}
	}
	return res
}

func (brt *Handle) prepareJobStatusList(importingList []*jobsdb.JobT, defaultStatus jobsdb.JobStatusT, sourceID, destinationID string) ([]*jobsdb.JobStatusT, []*jobsdb.JobT, map[int64]jobsdb.ConnectionDetails) {
	var abortedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	if defaultStatus.ErrorResponse == nil {
		defaultStatus.ErrorResponse = routerutils.EmptyPayload
	}
	jobIdConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	for _, job := range importingList {
		resp := enhanceResponseWithFirstAttemptedAt(job.LastJobStatus.ErrorResponse, defaultStatus.ErrorResponse)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      defaultStatus.JobState,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     defaultStatus.ErrorCode,
			ErrorResponse: resp,
			Parameters:    routerutils.EmptyPayload,
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
		jobIdConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      sourceID,
			DestinationID: destinationID,
		}

		if defaultStatus.JobState == jobsdb.Failed.State {
			if brt.retryLimitReached(&status) {
				status.JobState = jobsdb.Aborted.State
				abortedJobsList = append(abortedJobsList, job)
			}
		}
		statusList = append(statusList, &status)
	}
	return statusList, abortedJobsList, jobIdConnectionDetailsMap
}

func (brt *Handle) getParamertsFromJobs(jobs []*jobsdb.JobT) map[int64]stdjson.RawMessage {
	parametersMap := make(map[int64]stdjson.RawMessage)
	for _, job := range jobs {
		parametersMap[job.JobID] = job.Parameters
	}
	return parametersMap
}

func (brt *Handle) updatePollStatusToDB(
	ctx context.Context,
	destinationID string,
	sourceID string,
	importingJob *jobsdb.JobT,
	pollResp common.PollStatusResponse,
) ([]*jobsdb.JobStatusT, error) {
	var statusList []*jobsdb.JobStatusT
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	list, err := brt.getImportingJobs(ctx, destinationID, brt.maxEventsInABatch)
	if err != nil {
		return statusList, err
	}
	importingList := list.Jobs
	if pollResp.StatusCode == http.StatusOK && pollResp.Complete {
		if !pollResp.HasFailed && !pollResp.HasWarning {
			statusList, _, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Succeeded.State}, sourceID, destinationID)
			if err := brt.updateJobStatuses(ctx, destinationID, importingList, importingList, statusList); err != nil {
				brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
				return statusList, err
			}
			brt.asyncSuccessfulJobCount.Count(len(statusList))
		} else {
			getUploadStatsInput := common.GetUploadStatsInput{
				FailedJobParameters:  pollResp.FailedJobParameters,
				WarningJobParameters: pollResp.WarningJobParameters,
				Parameters:           importingJob.LastJobStatus.Parameters,
				ImportingList:        importingList,
			}
			startFailedJobsPollTime := time.Now()
			brt.logger.Debugn("[Batch Router] Fetching Failed Jobs Started", obskit.DestinationType(brt.destType))
			uploadStatsResp := brt.asyncDestinationStruct[destinationID].Manager.GetUploadStats(getUploadStatsInput)
			brt.asyncFailedJobsTimeStat.Since(startFailedJobsPollTime)

			if uploadStatsResp.StatusCode != http.StatusOK {
				brt.logger.Errorn("[Batch Router] Failed to fetch failed jobs",
					obskit.DestinationType(brt.destType),
					logger.NewIntField("statusCode", int64(uploadStatsResp.StatusCode)),
					obskit.Error(errors.New(uploadStatsResp.Error)),
				)
				return statusList, fmt.Errorf("failed to fetch failed jobs error %v", uploadStatsResp.Error)
			}

			var completedJobsList []*jobsdb.JobT
			var abortedJobs []*jobsdb.JobT
			var failedJobs []*jobsdb.JobT
			successfulJobIDs := append(uploadStatsResp.Metadata.SucceededKeys, uploadStatsResp.Metadata.WarningKeys...)
			for _, job := range importingList {
				jobID := job.JobID
				jobIDConnectionDetailsMap[jobID] = jobsdb.ConnectionDetails{
					SourceID:      sourceID,
					DestinationID: destinationID,
				}
				if slices.Contains(successfulJobIDs, jobID) {
					warningRespString := uploadStatsResp.Metadata.WarningReasons[jobID]
					warningResp, _ := jsonrs.Marshal(WarningResponse{Remarks: warningRespString})
					resp := enhanceResponseWithFirstAttemptedAt(job.LastJobStatus.ErrorResponse, warningResp)
					status := &jobsdb.JobStatusT{
						JobID:         jobID,
						JobState:      jobsdb.Succeeded.State,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "200",
						ErrorResponse: resp,
						Parameters:    routerutils.EmptyPayload,
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					completedJobsList = append(completedJobsList, job)
					statusList = append(statusList, status)
				} else if slices.Contains(uploadStatsResp.Metadata.FailedKeys, jobID) {
					errorRespString := uploadStatsResp.Metadata.FailedReasons[jobID]
					errorResp, _ := jsonrs.Marshal(ErrorResponse{Error: errorRespString})
					resp := enhanceResponseWithFirstAttemptedAt(job.LastJobStatus.ErrorResponse, errorResp)
					status := &jobsdb.JobStatusT{
						JobID:         jobID,
						JobState:      jobsdb.Failed.State,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "400",
						ErrorResponse: resp,
						Parameters:    routerutils.EmptyPayload,
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", errorRespString)
					failedJobs = append(failedJobs, job)
					completedJobsList = append(completedJobsList, job)
					statusList = append(statusList, status)
				} else if slices.Contains(uploadStatsResp.Metadata.AbortedKeys, jobID) {
					errorRespString := uploadStatsResp.Metadata.AbortedReasons[jobID]
					errorResp, _ := jsonrs.Marshal(ErrorResponse{Error: errorRespString})
					resp := enhanceResponseWithFirstAttemptedAt(job.LastJobStatus.ErrorResponse, errorResp)
					status := &jobsdb.JobStatusT{
						JobID:         jobID,
						JobState:      jobsdb.Aborted.State,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "400",
						ErrorResponse: resp,
						Parameters:    routerutils.EmptyPayload,
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", errorRespString)
					abortedJobs = append(abortedJobs, job)
					completedJobsList = append(completedJobsList, job)
					statusList = append(statusList, status)
				}
			}
			brt.asyncSuccessfulJobCount.Count(len(statusList) - len(failedJobs) - len(abortedJobs))
			brt.asyncFailedJobCount.Count(len(failedJobs))
			brt.asyncAbortedJobCount.Count(len(abortedJobs))
			if err := brt.updateJobStatuses(ctx, destinationID, importingList, completedJobsList, statusList); err != nil {
				brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
				return statusList, err
			}
		}
	} else if pollResp.StatusCode == http.StatusBadRequest {
		statusList, _, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Aborted.State, ErrorResponse: misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", pollResp.Error)}, sourceID, destinationID)
		if err := brt.updateJobStatuses(ctx, destinationID, importingList, importingList, statusList); err != nil {
			brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
			return statusList, err
		}
		brt.asyncAbortedJobCount.Count(len(statusList))
	} else {
		var abortedJobsList []*jobsdb.JobT
		statusList, abortedJobsList, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Failed.State, ErrorCode: strconv.Itoa(pollResp.StatusCode), ErrorResponse: misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", pollResp.Error)}, sourceID, destinationID)
		if err := brt.updateJobStatuses(ctx, destinationID, importingList, abortedJobsList, statusList); err != nil {
			brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
			return statusList, err
		}
		brt.asyncFailedJobCount.Count(len(statusList))
	}
	routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, statusList, jobIDConnectionDetailsMap)
	return statusList, nil
}

func (brt *Handle) pollAsyncStatus(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(brt.pollStatusLoopSleep.Load()):
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			brt.configSubscriberMu.RUnlock()
			for destinationID := range destinationsMap {
				brt.logger.Debugn("pollAsyncStatus Started", obskit.DestinationType(brt.destType))
				job, err := brt.getImportingJobs(ctx, destinationID, 1)
				if err != nil {
					// TODO: Add metrics
					brt.logger.Errorn("Error while getting job", obskit.DestinationType(brt.destType), obskit.Error(err))
					continue
				}
				importingJobs := job.Jobs
				if len(importingJobs) == 0 {
					continue
				}
				importingJob := importingJobs[0]
				pollInput := getPollInput(importingJob)
				sourceID := gjson.GetBytes(importingJob.Parameters, "source_id").String()
				startPollTime := time.Now()
				brt.logger.Debugn("[Batch Router] Poll Status Started", obskit.DestinationType(brt.destType))
				pollResp := brt.asyncDestinationStruct[destinationID].Manager.Poll(pollInput)
				brt.logger.Debugn("[Batch Router] Poll Status Finished", obskit.DestinationType(brt.destType))
				brt.asyncPollTimeStat.Since(startPollTime)
				if pollResp.InProgress {
					continue
				}
				statusList, err := brt.updatePollStatusToDB(ctx, destinationID, sourceID, importingJob, pollResp)
				if err == nil {
					brt.recordAsyncDestinationDeliveryStatus(sourceID, destinationID, statusList)
					brt.asyncStructCleanUp(destinationID)
				}
			}
		}
	}
}

func (brt *Handle) asyncUploadWorker(ctx context.Context) {
	if !common.IsAsyncDestination(brt.destType) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(brt.asyncUploadWorkerTimeout.Load()):
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			uploadIntervalMap := brt.uploadIntervalMap
			brt.configSubscriberMu.RUnlock()

			for destinationID := range destinationsMap {
				_, ok := brt.asyncDestinationStruct[destinationID]
				if !ok || brt.asyncDestinationStruct[destinationID].UploadInProgress {
					continue
				}

				timeElapsed := time.Since(brt.asyncDestinationStruct[destinationID].CreatedAt)
				brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()

				timeout := uploadIntervalMap[destinationID]
				if brt.asyncDestinationStruct[destinationID].Exists && (brt.asyncDestinationStruct[destinationID].CanUpload || timeElapsed > timeout) {
					brt.asyncDestinationStruct[destinationID].CanUpload = true
					brt.asyncDestinationStruct[destinationID].PartFileNumber++
					uploadResponse := brt.asyncDestinationStruct[destinationID].Manager.Upload(brt.asyncDestinationStruct[destinationID])

					brt.setMultipleJobStatus(setMultipleJobStatusParams{
						asyncJobMetadata: newAsyncJobMetadataFromDestinationStruct(brt.asyncDestinationStruct[destinationID]),
						AsyncOutput:      uploadResponse,
						Attempted:        true,
					})
					if uploadResponse.ImportingParameters != nil && len(uploadResponse.ImportingJobIDs) > 0 {
						brt.asyncDestinationStruct[destinationID].UploadInProgress = true
					} else {
						brt.asyncStructCleanUp(destinationID)
					}
				}
				brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
			}
		}
	}
}

func (brt *Handle) asyncStructSetup(sourceID, destinationID string, jobsList []*jobsdb.JobT) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()

	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", sourceID, uuid.String()))
	jsonPath := fmt.Sprintf(`%v.txt`, path)
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}

	existingJobRunID := brt.asyncDestinationStruct[destinationID].SourceJobRunID
	asyncJobMetadata := newAsyncJobMetadata(jobsList)
	newJobRunID := getFirstSourceJobRunID(asyncJobMetadata.JobParameters)
	if newJobRunID != existingJobRunID {
		brt.asyncDestinationStruct[destinationID].PartFileNumber = 0
	}
	brt.asyncDestinationStruct[destinationID].Exists = true
	brt.asyncDestinationStruct[destinationID].AttemptNums = asyncJobMetadata.AttemptNums
	brt.asyncDestinationStruct[destinationID].FirstAttemptedAts = asyncJobMetadata.FirstAttemptedAts
	brt.asyncDestinationStruct[destinationID].JobParameters = asyncJobMetadata.JobParameters
	brt.asyncDestinationStruct[destinationID].PartitionIDs = asyncJobMetadata.PartitionIDs
	brt.asyncDestinationStruct[destinationID].FileName = jsonPath
	brt.asyncDestinationStruct[destinationID].CreatedAt = brt.now()
	brt.asyncDestinationStruct[destinationID].SourceJobRunID = newJobRunID
}

func (brt *Handle) asyncStructCleanUp(destinationID string) {
	misc.RemoveFilePaths(brt.asyncDestinationStruct[destinationID].FileName)
	brt.asyncDestinationStruct[destinationID].ImportingJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].FailedJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].UploadInProgress = false
	brt.asyncDestinationStruct[destinationID].Size = 0
	brt.asyncDestinationStruct[destinationID].Exists = false
	brt.asyncDestinationStruct[destinationID].Count = 0
	brt.asyncDestinationStruct[destinationID].CanUpload = false
	brt.asyncDestinationStruct[destinationID].DestinationUploadURL = ""

	brt.asyncDestinationStruct[destinationID].AttemptNums = make(map[int64]int)
	brt.asyncDestinationStruct[destinationID].FirstAttemptedAts = make(map[int64]time.Time)
	brt.asyncDestinationStruct[destinationID].JobParameters = make(map[int64]stdjson.RawMessage)
	brt.asyncDestinationStruct[destinationID].PartitionIDs = make(map[int64]string)
}

func (brt *Handle) sendJobsToStorage(batchJobs BatchedJobs) error {
	destinationID := batchJobs.Connection.Destination.ID
	if brt.disableEgress {
		out := common.AsyncUploadOutput{
			DestinationID: destinationID,
		}
		for _, job := range batchJobs.Jobs {
			out.SucceededJobIDs = append(out.SucceededJobIDs, job.JobID)
			out.SuccessResponse = fmt.Sprintf(`{"error":"%s"`, rterror.ErrDisabledEgress.Error()) // skipcq: GO-R4002
		}

		brt.setMultipleJobStatus(setMultipleJobStatusParams{
			asyncJobMetadata: newAsyncJobMetadata(batchJobs.Jobs),
			AsyncOutput:      out,
			JobsList:         batchJobs.Jobs,
		})
		return nil
	}

	_, ok := brt.asyncDestinationStruct[destinationID]
	if ok {
		if invalidManager, ok := brt.asyncDestinationStruct[destinationID].Manager.(*common.InvalidManager); ok {
			failedAsyncJobs := BatchedJobs{
				Jobs:       batchJobs.Jobs,
				Connection: batchJobs.Connection,
				TimeWindow: batchJobs.TimeWindow,
				JobState:   jobsdb.Aborted.State,
			}
			brt.updateJobStatus(&failedAsyncJobs, false, invalidManager.Error, false)
			return invalidManager.Error
		}
		brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()
		defer brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
		if brt.asyncDestinationStruct[destinationID].CanUpload {
			out := common.AsyncUploadOutput{
				DestinationID: destinationID,
			}
			for _, job := range batchJobs.Jobs {
				out.FailedJobIDs = append(out.FailedJobIDs, job.JobID)
				out.FailedReason = `Jobs flowed over the prescribed limit`
			}

			brt.setMultipleJobStatus(setMultipleJobStatusParams{
				asyncJobMetadata: newAsyncJobMetadata(batchJobs.Jobs),
				AsyncOutput:      out,
				JobsList:         batchJobs.Jobs,
			})
			return nil
		}
	}

	var skipUpdateStructJobMetadataMaps bool
	if !ok || !brt.asyncDestinationStruct[destinationID].Exists {
		if !ok {
			asyncStruct := &common.AsyncDestinationStruct{}
			asyncStruct.UploadMutex.Lock()
			defer asyncStruct.UploadMutex.Unlock()
			brt.asyncDestinationStruct[destinationID] = asyncStruct
		}
		brt.asyncStructSetup(batchJobs.Connection.Source.ID, destinationID, batchJobs.Jobs)
		skipUpdateStructJobMetadataMaps = true
	}

	file, err := os.OpenFile(brt.asyncDestinationStruct[destinationID].FileName, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		err = fmt.Errorf("BRT: %s: file open failed : %s", brt.destType, err.Error())
		panic(err)
	}
	defer func() { _ = file.Close() }()
	var overFlow bool
	var overFlownJobs []*jobsdb.JobT
	writeAtBytes := brt.asyncDestinationStruct[destinationID].Size
	for _, job := range batchJobs.Jobs {
		if !IsAsyncDestinationLimitNotReached(brt, destinationID) {
			overFlow = true
			overFlownJobs = append(overFlownJobs, job)
			continue
		}

		fileData, err := brt.asyncDestinationStruct[destinationID].Manager.Transform(job)
		if err != nil {
			failedAsyncJobs := BatchedJobs{
				Jobs:       []*jobsdb.JobT{job},
				Connection: batchJobs.Connection,
				TimeWindow: batchJobs.TimeWindow,
				JobState:   jobsdb.Aborted.State,
			}
			brt.updateJobStatus(&failedAsyncJobs, false, err, false)
			continue
		}

		brt.asyncDestinationStruct[destinationID].Size = brt.asyncDestinationStruct[destinationID].Size + len([]byte(fileData+"\n"))
		_, err = file.WriteAt([]byte(fileData+"\n"), int64(writeAtBytes))
		if err != nil {
			err = fmt.Errorf("BRT: %s: file write failed : %s", brt.destType, err.Error())
			panic(err)
		}
		writeAtBytes += len([]byte(fileData + "\n"))
		brt.asyncDestinationStruct[destinationID].ImportingJobIDs = append(brt.asyncDestinationStruct[destinationID].ImportingJobIDs, job.JobID)
		brt.asyncDestinationStruct[destinationID].Count = brt.asyncDestinationStruct[destinationID].Count + 1
		brt.asyncDestinationStruct[destinationID].DestinationUploadURL = gjson.Get(string(job.EventPayload), "endpoint").String()

	}

	if len(overFlownJobs) > 0 {
		out := common.AsyncUploadOutput{
			DestinationID: destinationID,
		}
		for _, job := range overFlownJobs {
			out.FailedJobIDs = append(out.FailedJobIDs, job.JobID)
			out.FailedReason = `Jobs flowed over the prescribed limit`
		}

		brt.setMultipleJobStatus(
			setMultipleJobStatusParams{
				asyncJobMetadata: newAsyncJobMetadata(batchJobs.Jobs),
				AsyncOutput:      out,
				JobsList:         batchJobs.Jobs,
			},
		)

		return nil
	}

	if !skipUpdateStructJobMetadataMaps { // update job metadata maps of asyncDestinationStruct
		for _, job := range batchJobs.Jobs {
			brt.asyncDestinationStruct[destinationID].AttemptNums[job.JobID] = job.LastJobStatus.AttemptNum
			brt.asyncDestinationStruct[destinationID].FirstAttemptedAts[job.JobID] = getFirstAttemptAtFromErrorResponse(job.LastJobStatus.ErrorResponse)
			brt.asyncDestinationStruct[destinationID].JobParameters[job.JobID] = job.Parameters
			brt.asyncDestinationStruct[destinationID].PartitionIDs[job.JobID] = job.PartitionID
		}
	}
	if overFlow {
		brt.asyncDestinationStruct[destinationID].CanUpload = true
		return nil
	}

	return nil
}

func (brt *Handle) createFakeJob(jobID int64, parameters stdjson.RawMessage) *jobsdb.JobT {
	return &jobsdb.JobT{
		JobID:      jobID,
		Parameters: parameters,
	}
}

func (brt *Handle) getReportMetrics(params getReportMetricsParams) []*utilTypes.PUReportedMetric {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	routerWorkspaceJobStatusCount := make(map[string]int)
	jobsMap := lo.SliceToMap(params.JobsList, func(j *jobsdb.JobT) (int64, *jobsdb.JobT) {
		return j.JobID, j
	})
	for _, status := range params.StatusList {
		var parameters routerutils.JobParameters
		err := jsonrs.Unmarshal(params.ParametersMap[status.JobID], &parameters)
		if err != nil {
			brt.logger.Errorn("Unmarshal of job parameters failed", logger.NewStringField("parameters", string(params.ParametersMap[status.JobID])))
		}
		workspaceID := status.WorkspaceId
		eventName := parameters.EventName
		eventType := parameters.EventType
		key := parameters.SourceID + ":" + parameters.DestinationID + ":" + parameters.SourceJobRunID + ":" + status.JobState + ":" + status.ErrorCode + ":" + eventName + ":" + eventType
		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := &utilTypes.ConnectionDetails{
				SourceID:                parameters.SourceID,
				DestinationID:           parameters.DestinationID,
				SourceTaskRunID:         parameters.SourceTaskRunID,
				SourceJobID:             parameters.SourceJobID,
				SourceJobRunID:          parameters.SourceJobRunID,
				SourceDefinitionID:      parameters.SourceDefinitionID,
				DestinationDefinitionID: parameters.DestinationDefinitionID,
				SourceCategory:          parameters.SourceCategory,
			}
			connectionDetailsMap[key] = cd
			transformedAtMap[key] = parameters.TransformAt
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			errorCode, err := strconv.Atoi(status.ErrorCode)
			if err != nil {
				errorCode = 0
			}
			sampleEvent := routerutils.EmptyPayload
			if job, ok := jobsMap[status.JobID]; ok {
				sampleEvent = job.EventPayload
			}
			sd = &utilTypes.StatusDetail{
				Status:         status.JobState,
				StatusCode:     errorCode,
				SampleResponse: string(status.ErrorResponse),
				SampleEvent:    sampleEvent,
				EventName:      eventName,
				EventType:      eventType,
			}
			statusDetailsMap[key] = sd
		}

		switch status.JobState {
		case jobsdb.Failed.State:
			if status.ErrorCode != strconv.Itoa(types.RouterUnMarshalErrorCode) {
				if status.AttemptNum == 1 {
					sd.Count++
				}
			}
		case jobsdb.Succeeded.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
		case jobsdb.Aborted.State:
			sd.FailedMessages = append(sd.FailedMessages, &utilTypes.FailedMessage{MessageID: parameters.MessageID, ReceivedAt: parameters.ParseReceivedAtTime()})
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
		}
	}

	utilTypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
	for k, cd := range connectionDetailsMap {
		var inPu string
		if transformedAtMap[k] == "processor" {
			inPu = utilTypes.DEST_TRANSFORMER
		} else {
			inPu = utilTypes.EVENT_FILTER
		}
		m := &utilTypes.PUReportedMetric{
			ConnectionDetails: *cd,
			PUDetails:         *utilTypes.CreatePUDetails(inPu, utilTypes.BATCH_ROUTER, true, false),
			StatusDetail:      statusDetailsMap[k],
		}
		if m.StatusDetail.Count != 0 {
			reportMetrics = append(reportMetrics, m)
		}
	}

	return reportMetrics
}

func (brt *Handle) setMultipleJobStatus(params setMultipleJobStatusParams) {
	workspaceID := brt.GetWorkspaceIDForDestID(params.AsyncOutput.DestinationID)
	var completedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	if len(params.AsyncOutput.ImportingJobIDs) > 0 {
		for _, jobId := range lo.Uniq(params.AsyncOutput.ImportingJobIDs) {
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionDetails{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(params.JobParameters[jobId], "source_id").String(),
			}
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Importing.State,
				AttemptNum:    params.AttemptNums[jobId] + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: routerutils.EnhanceJsonWithTime(params.FirstAttemptedAts[jobId], "firstAttemptedAt", routerutils.EmptyPayload),
				Parameters:    params.AsyncOutput.ImportingParameters,
				JobParameters: params.JobParameters[jobId],
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(params.AsyncOutput.SucceededJobIDs) > 0 {
		for _, jobId := range lo.Uniq(params.AsyncOutput.SucceededJobIDs) {
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionDetails{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(params.JobParameters[jobId], "source_id").String(),
			}
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Succeeded.State,
				AttemptNum:    params.AttemptNums[jobId],
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: routerutils.EnhanceJsonWithTime(params.FirstAttemptedAts[jobId], "firstAttemptedAt", stdjson.RawMessage(params.AsyncOutput.SuccessResponse)),
				Parameters:    routerutils.EmptyPayload,
				JobParameters: params.JobParameters[jobId],
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
			completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, params.JobParameters[jobId]))
		}
	}
	if len(params.AsyncOutput.FailedJobIDs) > 0 {
		for _, jobId := range lo.Uniq(params.AsyncOutput.FailedJobIDs) {
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionDetails{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(params.JobParameters[jobId], "source_id").String(),
			}
			resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", params.AsyncOutput.FailedReason)
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Failed.State,
				AttemptNum:    params.AttemptNums[jobId],
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "500",
				ErrorResponse: routerutils.EnhanceJsonWithTime(params.FirstAttemptedAts[jobId], "firstAttemptedAt", resp),
				Parameters:    routerutils.EmptyPayload,
				JobParameters: params.JobParameters[jobId],
				WorkspaceId:   workspaceID,
			}
			if params.Attempted {
				status.AttemptNum = params.AttemptNums[jobId] + 1
			}

			if brt.retryLimitReached(&status) {
				status.JobState = jobsdb.Aborted.State
				completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, params.JobParameters[jobId]))
			}
			statusList = append(statusList, &status)
		}
	}
	if len(params.AsyncOutput.AbortJobIDs) > 0 {
		for _, jobId := range lo.Uniq(params.AsyncOutput.AbortJobIDs) {
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionDetails{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(params.JobParameters[jobId], "source_id").String(),
			}
			resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", params.AsyncOutput.AbortReason)
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Aborted.State,
				AttemptNum:    params.AttemptNums[jobId],
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "400",
				ErrorResponse: routerutils.EnhanceJsonWithTime(params.FirstAttemptedAts[jobId], "firstAttemptedAt", stdjson.RawMessage(resp)),
				Parameters:    routerutils.EmptyPayload,
				JobParameters: params.JobParameters[jobId],
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
			completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, params.JobParameters[jobId]))
		}
	}

	if len(statusList) == 0 {
		return
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:  "destination_id",
			Value: params.AsyncOutput.DestinationID,
		},
	}

	reportMetrics := brt.getReportMetrics(getReportMetricsParams{
		StatusList:    statusList,
		ParametersMap: params.JobParameters,
		JobsList:      params.JobsList,
	})

	// Mark the status of the jobs
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
			if err != nil {
				brt.logger.Errorn("[Batch Router] Error occurred while updating jobs statuses. Panicking", obskit.DestinationType(brt.destType), obskit.Error(err))
				return err
			}
			// rsources stats
			err = brt.updateRudderSourcesStats(ctx, tx, completedJobsList, statusList)
			if err != nil {
				return err
			}

			if brt.reporting != nil && brt.reportingEnabled {
				if err = brt.reporting.Report(ctx, reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
			}
			return nil
		})
	}, brt.sendRetryUpdateStats)
	if err != nil {
		panic(err)
	}
	routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, statusList, jobIDConnectionDetailsMap)
	brt.pendingEventsRegistry.DecreasePendingEvents("batch_rt", workspaceID, brt.destType, float64(len(completedJobsList)))
	if params.Attempted {
		var sourceID string
		if len(statusList) > 0 {
			sourceID = gjson.GetBytes(params.JobParameters[statusList[0].JobID], "source_id").String()
		}
		brt.recordAsyncDestinationDeliveryStatus(sourceID, params.AsyncOutput.DestinationID, statusList)
	}
}

func (brt *Handle) GetWorkspaceIDForDestID(destID string) string {
	var workspaceID string

	brt.configSubscriberMu.RLock()
	defer brt.configSubscriberMu.RUnlock()
	workspaceID = brt.destinationsMap[destID].Sources[0].WorkspaceID

	return workspaceID
}
