package batchrouter

import (
	"bytes"
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
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	common "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

func (brt *Handle) getImportingJobs(ctx context.Context, augmentQueryParams func(*jobsdb.GetQueryParams), limit int) (jobsdb.JobsResult, error) {
	var jobsResult jobsdb.JobsResult
	// we need to get all importing jobs based on limit, overcoming dsLimits (no payload size limit is applied here)
	var stop bool
	var moreToken any
	var iterations int
	maxIterations := brt.maxImportingQueryIterations.Load()

	for !stop {
		jobsLimit := limit - len(jobsResult.Jobs)
		queryParams := jobsdb.GetQueryParams{
			CustomValFilters: []string{brt.destType},
			JobsLimit:        jobsLimit,
		}
		augmentQueryParams(&queryParams)
		r, err := misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) (*jobsdb.MoreJobsResult, error) {
			return brt.jobsDB.GetImporting(
				ctx,
				queryParams,
				moreToken,
			)
		}, brt.sendQueryRetryStats)
		if err != nil {
			return jobsdb.JobsResult{}, err
		}
		jobsResult.Jobs = append(jobsResult.Jobs, r.Jobs...)
		moreToken = r.More
		iterations++
		stop = len(jobsResult.Jobs) == limit || // stop when we have enough jobs
			(len(r.Jobs) == 0 && !r.DSLimitsReached) || // or we are confident there are no more jobs to fetch
			iterations == maxIterations // or we have reached max iterations
	}
	brt.asyncGetImportingIterations.Observe(float64(iterations))

	if iterations == maxIterations {
		// This is a safeguard against infinite loops and should never be hit under normal
		// circumstances. If we are here, the loop was cut short by the iteration cap and there
		// may still be importing jobs left unfetched.
		brt.logger.Warnn("GetImportingJobs reached the maximum iteration count, importing jobs may have been left unfetched",
			logger.NewIntField("iterations", int64(iterations)),
			logger.NewIntField("maxIterations", int64(maxIterations)),
			obskit.DestinationType(brt.destType),
		)
	}
	return jobsResult, nil
}

func (brt *Handle) updateJobStatuses(ctx context.Context, allJobs, completedJobs []*jobsdb.JobT, statusList []*jobsdb.JobStatusT) error {
	reportMetrics := brt.getReportMetrics(getReportMetricsParams{
		StatusList:    statusList,
		ParametersMap: brt.getParamertsFromJobs(allJobs),
		JobsList:      allJobs,
	})

	return misc.RetryWithNotify(ctx, brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
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
			return nil
		})
	}, brt.sendRetryUpdateStats)
}

func getPollInput(job *jobsdb.JobT) common.AsyncPoll {
	parameters := job.LastJobStatus.Parameters
	return common.AsyncPoll{
		ImportId:    gjson.GetBytes(parameters, "importId").String(),
		ImportCount: int(gjson.GetBytes(parameters, "importCount").Int()),
	}
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

func (brt *Handle) prepareJobStatusList(importingList []*jobsdb.JobT, defaultStatus jobsdb.JobStatusT, sourceID, destinationID string) ([]*jobsdb.JobStatusT, []*jobsdb.JobT, map[int64]jobsdb.ConnectionID) {
	var abortedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	if defaultStatus.ErrorResponse == nil {
		defaultStatus.ErrorResponse = routerutils.EmptyPayload
	}
	jobIdConnectionDetailsMap := make(map[int64]jobsdb.ConnectionID)
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
			PartitionID:   job.PartitionID,
			CustomVal:     job.CustomVal,
		}
		jobIdConnectionDetailsMap[job.JobID] = jobsdb.ConnectionID{
			SourceID:      sourceID,
			DestinationID: destinationID,
		}

		if defaultStatus.JobState == jobsdb.Failed.State {
			if brt.retryLimitReached(&status) {
				status.JobState = jobsdb.Aborted.State
				status.ErrorCode = routerutils.DRAIN_ERROR_CODE
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

func (brt *Handle) updatePollStatusToDB(ctx context.Context, destinationID, sourceID string, importingJob *jobsdb.JobT, importingCount int, pollResp common.PollStatusResponse) ([]*jobsdb.JobStatusT, error) {
	var statusList []*jobsdb.JobStatusT
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionID)
	list, err := brt.getImportingJobs(ctx, func(gqp *jobsdb.GetQueryParams) {
		gqp.ParameterFilters = []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
	}, importingCount)
	if err != nil {
		return statusList, err
	}
	importingList := list.Jobs
	if len(importingList) != importingCount {
		brt.logger.Warnn("[Batch Router] Mismatch in importing list and importing count",
			obskit.DestinationID(destinationID),
			obskit.SourceID(sourceID),
			logger.NewIntField("importingListSize", int64(len(importingList))),
			logger.NewIntField("importingCount", int64(importingCount)),
		)
	}
	if pollResp.StatusCode == http.StatusOK && pollResp.Complete {
		if !pollResp.HasFailed && !pollResp.HasWarning {
			statusList, _, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Succeeded.State}, sourceID, destinationID)
			if err := brt.updateJobStatuses(ctx, importingList, importingList, statusList); err != nil {
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
			brt.asyncDestinationStructMu.RLock()
			asyncDestStruct := brt.asyncDestinationStruct[destinationID]
			brt.asyncDestinationStructMu.RUnlock()
			if asyncDestStruct == nil {
				return statusList, fmt.Errorf("async destination struct not found for destinationID: %s", destinationID)
			}
			asyncDestStruct.UploadMutex.RLock()
			manager := asyncDestStruct.Manager
			if manager == nil {
				asyncDestStruct.UploadMutex.RUnlock()
				return statusList, fmt.Errorf("async destination manager not found for destinationID: %s", destinationID)
			}
			startFailedJobsPollTime := time.Now()
			brt.logger.Debugn("[Batch Router] Fetching Failed Jobs Started", obskit.DestinationType(brt.destType))
			uploadStatsResp := manager.GetUploadStats(getUploadStatsInput)
			asyncDestStruct.UploadMutex.RUnlock()
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
				jobIDConnectionDetailsMap[jobID] = jobsdb.ConnectionID{
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
						PartitionID:   job.PartitionID,
						CustomVal:     job.CustomVal,
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
						PartitionID:   job.PartitionID,
						CustomVal:     job.CustomVal,
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
						PartitionID:   job.PartitionID,
						CustomVal:     job.CustomVal,
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
			if err := brt.updateJobStatuses(ctx, importingList, completedJobsList, statusList); err != nil {
				brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
				return statusList, err
			}
		}
	} else if pollResp.StatusCode == http.StatusBadRequest {
		statusList, _, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Aborted.State, ErrorResponse: misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", pollResp.Error)}, sourceID, destinationID)
		if err := brt.updateJobStatuses(ctx, importingList, importingList, statusList); err != nil {
			brt.logger.Errorn("[Batch Router] Failed to update job status", obskit.DestinationType(brt.destType), obskit.Error(err))
			return statusList, err
		}
		brt.asyncAbortedJobCount.Count(len(statusList))
	} else {
		var abortedJobsList []*jobsdb.JobT
		statusList, abortedJobsList, jobIDConnectionDetailsMap = brt.prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Failed.State, ErrorCode: strconv.Itoa(pollResp.StatusCode), ErrorResponse: misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "error", pollResp.Error)}, sourceID, destinationID)
		if err := brt.updateJobStatuses(ctx, importingList, abortedJobsList, statusList); err != nil {
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
			brt.logger.Infon("pollAsyncStatus stopped", obskit.DestinationType(brt.destType))
			return
		case <-time.After(brt.pollStatusLoopSleep.Load()):
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			brt.configSubscriberMu.RUnlock()
			for destinationID := range destinationsMap {
				if ctx.Err() != nil {
					brt.logger.Infon("pollAsyncStatus context done", obskit.DestinationType(brt.destType))
					return
				}
				brt.logger.Debugn("pollAsyncStatus Started", obskit.DestinationType(brt.destType))
				jobsResult, err := brt.getImportingJobs(ctx, func(gqp *jobsdb.GetQueryParams) {
					gqp.ParameterFilters = []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
				}, 1)
				if err != nil {
					// TODO: Add metrics
					brt.logger.Errorn("Error while getting job", obskit.DestinationType(brt.destType), obskit.Error(err))
					continue
				}
				if len(jobsResult.Jobs) == 0 {
					continue
				}
				importingJob := jobsResult.Jobs[0]
				pollInput := getPollInput(importingJob)
				sourceID := gjson.GetBytes(importingJob.Parameters, "source_id").String()
				brt.asyncDestinationStructMu.RLock()
				asyncDestStruct := brt.asyncDestinationStruct[destinationID]
				brt.asyncDestinationStructMu.RUnlock()
				if asyncDestStruct == nil {
					brt.logger.Errorn("Async destination struct not found", obskit.DestinationType(brt.destType), obskit.DestinationID(destinationID))
					continue
				}
				asyncDestStruct.UploadMutex.RLock()
				manager := asyncDestStruct.Manager
				if manager == nil {
					asyncDestStruct.UploadMutex.RUnlock()
					brt.logger.Errorn("Async destination manager not found", obskit.DestinationType(brt.destType), obskit.DestinationID(destinationID))
					continue
				}
				startPollTime := time.Now()
				brt.logger.Debugn("[Batch Router] Poll Status Started", obskit.DestinationType(brt.destType))
				pollResp := manager.Poll(ctx, pollInput)
				asyncDestStruct.UploadMutex.RUnlock()
				brt.logger.Debugn("[Batch Router] Poll Status Finished", obskit.DestinationType(brt.destType))
				brt.asyncPollTimeStat.Since(startPollTime)
				if pollResp.InProgress {
					continue
				}
				importingCount := pollInput.ImportCount
				if importingCount == 0 { // fallback to maxEventsInABatch if import count is not set
					importingCount = brt.maxEventsInABatch
				}
				statusList, err := brt.updatePollStatusToDB(ctx, destinationID, sourceID, importingJob, importingCount, pollResp)
				if err == nil {
					brt.recordAsyncDestinationDeliveryStatus(sourceID, destinationID, statusList)
					brt.asyncDestinationStructMu.RLock()
					asyncDestStruct := brt.asyncDestinationStruct[destinationID]
					brt.asyncDestinationStructMu.RUnlock()
					if asyncDestStruct == nil {
						continue
					}
					asyncDestStruct.UploadMutex.Lock()
					if pollInput.ImportCount > 0 && len(statusList) != pollInput.ImportCount {
						// Log a warning if there is a mismatch in the lengths
						var minStatusJobID, maxStatusJobID int64
						for _, status := range statusList {
							if minStatusJobID == 0 || status.JobID < minStatusJobID {
								minStatusJobID = status.JobID
							}
							if status.JobID > maxStatusJobID {
								maxStatusJobID = status.JobID
							}
						}
						structImportingJobIDs := asyncDestStruct.ImportingJobIDs
						brt.logger.Errorn("Async Destination Bug: mismatch in updated status list and importing jobs in asyncDestinationStruct",
							obskit.DestinationType(brt.destType),
							obskit.DestinationID(destinationID),
							logger.NewIntField("statusListSize", int64(len(statusList))),
							logger.NewIntField("minStatusJobID", minStatusJobID),
							logger.NewIntField("maxStatusJobID", maxStatusJobID),
							logger.NewIntField("importingJobIDsSize", int64(pollInput.ImportCount)),
							logger.NewIntField("structImportingJobIDsSize", int64(len(structImportingJobIDs))),
							logger.NewIntField("minStructImportingJobID", lo.Min(structImportingJobIDs)),
							logger.NewIntField("maxStructImportingJobID", lo.Max(structImportingJobIDs)),
						)
					}
					brt.asyncStructCleanUp(asyncDestStruct)
					asyncDestStruct.UploadMutex.Unlock()
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
			brt.logger.Infon("asyncUploadWorker stopped", obskit.DestinationType(brt.destType))
			return
		case <-time.After(brt.asyncUploadWorkerTimeout.Load()):
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			uploadIntervalMap := brt.uploadIntervalMap
			brt.configSubscriberMu.RUnlock()

			for destinationID := range destinationsMap {
				brt.asyncDestinationStructMu.RLock()
				asyncDestStruct, ok := brt.asyncDestinationStruct[destinationID]
				brt.asyncDestinationStructMu.RUnlock()
				if !ok {
					continue
				}
				if ctx.Err() != nil {
					brt.logger.Infon("asyncUploadWorker context done", obskit.DestinationType(brt.destType))
					return
				}

				asyncDestStruct.UploadMutex.Lock()
				if asyncDestStruct.UploadInProgress {
					asyncDestStruct.UploadMutex.Unlock()
					continue
				}
				timeElapsed := time.Since(asyncDestStruct.CreatedAt)
				timeout := uploadIntervalMap[destinationID]
				if asyncDestStruct.Exists && (asyncDestStruct.CanUpload || timeElapsed > timeout) {
					asyncDestStruct.CanUpload = true
					asyncDestStruct.PartFileNumber++
					manager := asyncDestStruct.Manager
					if manager == nil {
						asyncDestStruct.UploadMutex.Unlock()
						continue
					}
					uploadResponse := manager.Upload(ctx, asyncDestStruct)
					asyncJobMetadata := newAsyncJobMetadataFromDestinationStruct(asyncDestStruct)

					brt.setMultipleJobStatus(setMultipleJobStatusParams{
						asyncJobMetadata: asyncJobMetadata,
						AsyncOutput:      uploadResponse,
						Attempted:        true,
					})
					if uploadResponse.ImportingParameters != nil && len(uploadResponse.ImportingJobIDs) > 0 {
						asyncDestStruct.UploadInProgress = true
					} else {
						brt.asyncStructCleanUp(asyncDestStruct)
					}
				}
				asyncDestStruct.UploadMutex.Unlock()
			}
		}
	}
}

func (brt *Handle) asyncStructSetup(sourceID, destinationID string, jobsList []*jobsdb.JobT) {
	brt.asyncDestinationStructMu.RLock()
	asyncDestStruct := brt.asyncDestinationStruct[destinationID]
	brt.asyncDestinationStructMu.RUnlock()
	if asyncDestStruct == nil {
		return
	}

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

	existingJobRunID := asyncDestStruct.SourceJobRunID
	asyncJobMetadata := newAsyncJobMetadata(jobsList)
	newJobRunID := getFirstSourceJobRunID(asyncJobMetadata.JobParameters)
	if newJobRunID != existingJobRunID {
		asyncDestStruct.PartFileNumber = 0
	}
	asyncDestStruct.Exists = true
	asyncDestStruct.AttemptNums = asyncJobMetadata.AttemptNums
	asyncDestStruct.FirstAttemptedAts = asyncJobMetadata.FirstAttemptedAts
	asyncDestStruct.JobParameters = asyncJobMetadata.JobParameters
	asyncDestStruct.PartitionIDs = asyncJobMetadata.PartitionIDs
	asyncDestStruct.FileName = jsonPath
	asyncDestStruct.CreatedAt = brt.now()
	asyncDestStruct.SourceJobRunID = newJobRunID
}

func (brt *Handle) asyncStructCleanUp(asyncDestStruct *common.AsyncDestinationStruct) {
	if asyncDestStruct == nil {
		return
	}
	misc.RemoveFilePaths(asyncDestStruct.FileName)
	asyncDestStruct.ImportingJobIDs = []int64{}
	asyncDestStruct.FailedJobIDs = []int64{}
	asyncDestStruct.UploadInProgress = false
	asyncDestStruct.Size = 0
	asyncDestStruct.Exists = false
	asyncDestStruct.Count = 0
	asyncDestStruct.CanUpload = false
	asyncDestStruct.DestinationUploadURL = ""

	asyncDestStruct.AttemptNums = make(map[int64]int)
	asyncDestStruct.FirstAttemptedAts = make(map[int64]time.Time)
	asyncDestStruct.JobParameters = make(map[int64]stdjson.RawMessage)
	asyncDestStruct.PartitionIDs = make(map[int64]string)
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

	brt.asyncDestinationStructMu.RLock()
	asyncDestStruct, ok := brt.asyncDestinationStruct[destinationID]
	brt.asyncDestinationStructMu.RUnlock()
	if !ok {
		brt.asyncDestinationStructMu.Lock()
		if existingAsyncDestStruct, exists := brt.asyncDestinationStruct[destinationID]; exists {
			asyncDestStruct = existingAsyncDestStruct
		} else {
			asyncDestStruct = &common.AsyncDestinationStruct{}
			brt.asyncDestinationStruct[destinationID] = asyncDestStruct
		}
		brt.asyncDestinationStructMu.Unlock()
	}
	asyncDestStruct.UploadMutex.Lock()
	defer asyncDestStruct.UploadMutex.Unlock()
	manager := asyncDestStruct.Manager
	if invalidManager, ok := manager.(*common.InvalidManager); ok {
		failedAsyncJobs := BatchedJobs{
			Jobs:       batchJobs.Jobs,
			Connection: batchJobs.Connection,
			TimeWindow: batchJobs.TimeWindow,
			JobState:   jobsdb.Aborted.State,
		}
		brt.updateJobStatus(&failedAsyncJobs, false, invalidManager.Error, false)
		return invalidManager.Error
	}
	if asyncDestStruct.CanUpload {
		// Waiting for previous upload to complete, mark all jobs as failed
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
	if !asyncDestStruct.Exists {
		brt.asyncStructSetup(batchJobs.Connection.Source.ID, destinationID, batchJobs.Jobs)
	}
	if manager == nil {
		return fmt.Errorf("async destination manager not found for destinationID: %s", destinationID)
	}
	file, err := os.OpenFile(asyncDestStruct.FileName, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		err = fmt.Errorf("BRT: %s: file open failed : %s", brt.destType, err.Error())
		panic(err)
	}
	defer func() { _ = file.Close() }()
	var overFlownJobs []*jobsdb.JobT
	writeAtBytes := asyncDestStruct.Size
	for _, job := range batchJobs.Jobs {
		if !IsAsyncDestinationLimitNotReached(brt, asyncDestStruct) {
			overFlownJobs = append(overFlownJobs, job)
			continue
		}
		fileData, err := manager.Transform(job)
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

		asyncDestStruct.Size = asyncDestStruct.Size + len([]byte(fileData+"\n"))
		_, err = file.WriteAt([]byte(fileData+"\n"), int64(writeAtBytes))
		if err != nil {
			err = fmt.Errorf("BRT: %s: file write failed : %s", brt.destType, err.Error())
			panic(err)
		}
		writeAtBytes += len([]byte(fileData + "\n"))
		asyncDestStruct.ImportingJobIDs = append(asyncDestStruct.ImportingJobIDs, job.JobID)
		asyncDestStruct.Count = asyncDestStruct.Count + 1
		asyncDestStruct.DestinationUploadURL = gjson.Get(string(job.EventPayload), "endpoint").String()

		asyncDestStruct.AttemptNums[job.JobID] = job.LastJobStatus.AttemptNum
		asyncDestStruct.FirstAttemptedAts[job.JobID] = getFirstAttemptAtFromErrorResponse(job.LastJobStatus.ErrorResponse)
		asyncDestStruct.JobParameters[job.JobID] = job.Parameters
		asyncDestStruct.PartitionIDs[job.JobID] = job.PartitionID

	}

	if len(overFlownJobs) > 0 {
		// mark overflown jobs as failed
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
		// turn on CanUpload flag to true
		asyncDestStruct.CanUpload = true
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
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionID)

	missingJobParameters := map[string][]int64{}
	getJobParameters := func(state string, jobID int64) stdjson.RawMessage {
		jobParameters, ok := params.JobParameters[jobID]
		if !ok {
			missingJobParameters[state] = append(missingJobParameters[state], jobID)
			// synthesize job parameters with the destination ID in case of missing job parameters
			jobParameters = []byte(`{"destination_id":"` + params.AsyncOutput.DestinationID + `"}`)
		}
		return jobParameters
	}
	if len(params.AsyncOutput.ImportingJobIDs) > 0 {
		importingJobIDs := lo.Uniq(params.AsyncOutput.ImportingJobIDs)
		for _, jobId := range importingJobIDs {
			jobParameters := getJobParameters(jobsdb.Importing.State, jobId)
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionID{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(jobParameters, "source_id").String(),
			}
			// Persist any per-job metadata onto the stored status Parameters, so a
			// destination can read it back at poll time without loading the job
			// payload or relying on in-memory state. The metadata shape is
			// destination-specific.
			statusParameters := params.AsyncOutput.ImportingParameters
			if jobImportingParameters, ok := params.AsyncOutput.JobImportingParameters[jobId]; ok {
				// copy the shared ImportingParameters before mutating, so each job's
				// metadata does not leak into the others
				base := bytes.Clone(params.AsyncOutput.ImportingParameters)
				if merged, mergedErr := sjson.SetBytes(base, "metadata", jobImportingParameters); mergedErr != nil {
					brt.logger.Errorn("[Batch Router] Failed to persist per-job importing metadata on status params",
						obskit.DestinationType(brt.destType),
						logger.NewIntField("jobId", jobId),
						obskit.Error(mergedErr),
					)
				} else {
					statusParameters = merged
				}
			}
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Importing.State,
				AttemptNum:    params.AttemptNums[jobId] + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: routerutils.EnhanceJsonWithTime(params.FirstAttemptedAts[jobId], "firstAttemptedAt", routerutils.EmptyPayload),
				Parameters:    statusParameters,
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
				PartitionID:   params.PartitionIDs[jobId],
				CustomVal:     brt.destType,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(params.AsyncOutput.SucceededJobIDs) > 0 {
		succeededJobIDs := lo.Uniq(params.AsyncOutput.SucceededJobIDs)
		for _, jobId := range succeededJobIDs {
			jobParameters := getJobParameters(jobsdb.Succeeded.State, jobId)
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionID{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(jobParameters, "source_id").String(),
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
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
				PartitionID:   params.PartitionIDs[jobId],
				CustomVal:     brt.destType,
			}
			statusList = append(statusList, &status)
			completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, jobParameters))
		}
	}
	if len(params.AsyncOutput.FailedJobIDs) > 0 {
		failedJobIDs := lo.Uniq(params.AsyncOutput.FailedJobIDs)
		for _, jobId := range failedJobIDs {
			jobParameters := getJobParameters(jobsdb.Failed.State, jobId)
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionID{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(jobParameters, "source_id").String(),
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
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
				PartitionID:   params.PartitionIDs[jobId],
				CustomVal:     brt.destType,
			}
			if params.Attempted {
				status.AttemptNum = params.AttemptNums[jobId] + 1
			}

			if brt.retryLimitReached(&status) {
				status.JobState = jobsdb.Aborted.State
				status.ErrorCode = routerutils.DRAIN_ERROR_CODE
				completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, jobParameters))
			}
			statusList = append(statusList, &status)
		}
	}
	if len(params.AsyncOutput.AbortJobIDs) > 0 {
		toAbortJobIDs := lo.Uniq(params.AsyncOutput.AbortJobIDs)
		if len(params.AsyncOutput.SucceededJobIDs) > 0 {
			if common := lo.Intersect(lo.Uniq(params.AsyncOutput.SucceededJobIDs), toAbortJobIDs); len(common) > 0 {
				// Debugging negative pending events count issue
				brt.logger.Errorn("Async Destination Bug: same async job IDs are present in both SucceededJobIDs and AbortJobIDs. Removing them from AbortJobIDs",
					obskit.DestinationType(brt.destType),
					logger.NewStringField("jobIDs", fmt.Sprintf("%+v", common)),
				)
				toAbortJobIDs, _ = lo.Difference(toAbortJobIDs, common)
			}
		}
		for _, jobId := range toAbortJobIDs {
			jobParameters := getJobParameters(jobsdb.Aborted.State, jobId)
			jobIDConnectionDetailsMap[jobId] = jobsdb.ConnectionID{
				DestinationID: params.AsyncOutput.DestinationID,
				SourceID:      gjson.GetBytes(jobParameters, "source_id").String(),
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
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
				PartitionID:   params.PartitionIDs[jobId],
				CustomVal:     brt.destType,
			}
			statusList = append(statusList, &status)
			completedJobsList = append(completedJobsList, brt.createFakeJob(jobId, jobParameters))
		}
	}

	if len(missingJobParameters) > 0 {
		loggerFields := []logger.Field{
			obskit.DestinationType(brt.destType),
			obskit.DestinationID(params.AsyncOutput.DestinationID),
			logger.NewBoolField("attempted", params.Attempted),
			logger.NewStringField("availableJobIDs", fmt.Sprintf("%+v", lo.Keys(params.JobParameters))),
		}
		for state, jobIDs := range missingJobParameters {
			loggerFields = append(loggerFields, logger.NewStringField(state+"MissingJobIDs", fmt.Sprintf("%+v", jobIDs)))
		}
		brt.logger.Errorn("Async Destination Bug: missing job parameters for async jobs", loggerFields...)
	}

	if len(statusList) == 0 {
		return
	}

	reportMetrics := brt.getReportMetrics(getReportMetricsParams{
		StatusList:    statusList,
		ParametersMap: params.JobParameters,
		JobsList:      params.JobsList,
	})

	// Mark the status of the jobs
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
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
