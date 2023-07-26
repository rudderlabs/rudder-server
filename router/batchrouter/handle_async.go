package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (brt *Handle) getImportingJobs(ctx context.Context, destinationID string, limit int) (jobsdb.JobsResult, error) {
	parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
	return misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return brt.jobsDB.GetImporting(
			ctx,
			jobsdb.GetQueryParamsT{
				CustomValFilters: []string{brt.destType},
				JobsLimit:        limit,
				ParameterFilters: parameterFilters,
				PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit),
			},
		)
	}, brt.sendQueryRetryStats)
}

func (brt *Handle) updateJobStatuses(ctx context.Context, destinationID string, statusList []*jobsdb.JobStatusT) error {
	parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destinationID}}
	return misc.RetryWithNotify(ctx, brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
			if err != nil {
				return fmt.Errorf("updating %s job statuses: %w", brt.destType, err)
			}
			// no need to update rsources stats here since no terminal job state is recorded
			return nil
		})
	}, brt.sendRetryUpdateStats)
}

func getPollInput(job *jobsdb.JobT) common.AsyncPoll {
	parameters := job.LastJobStatus.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	return common.AsyncPoll{ImportId: importId}
}

func prepareJobStatusList(importingList []*jobsdb.JobT, defaultStatus jobsdb.JobStatusT) []*jobsdb.JobStatusT {
	var statusList []*jobsdb.JobStatusT
	if defaultStatus.ErrorResponse == nil {
		defaultStatus.ErrorResponse = []byte(`{}`)
	}

	for _, job := range importingList {
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      defaultStatus.JobState,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     defaultStatus.ErrorCode,
			ErrorResponse: defaultStatus.ErrorResponse,
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
		statusList = append(statusList, &status)
	}
	return statusList
}

func (brt *Handle) updatePollStatusToDB(ctx context.Context, destinationID string,
	importingJob *jobsdb.JobT, pollResp common.PollStatusResponse,
) error {
	list, err := brt.getImportingJobs(ctx, destinationID, brt.maxEventsInABatch)
	if err != nil {
		return err
	}
	importingList := list.Jobs
	if pollResp.StatusCode == http.StatusOK && pollResp.Complete {
		// TODO: check about maxEventsInABatch
		if !pollResp.HasFailed {
			statusList := prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Succeeded.State})
			if err := brt.updateJobStatuses(ctx, destinationID, statusList); err != nil {
				brt.logger.Errorf("[Batch Router] Failed to update job status for Dest Type %v with error %v", brt.destType, err)
				return err
			}
			brt.asyncSuccessfulJobCount.Count(len(statusList))
			brt.updateProcessedEventsMetrics(statusList)
			return nil
		} else {
			getUploadStatsInput := common.GetUploadStatsInput{
				FailedJobURLs: pollResp.FailedJobURLs,
				Parameters:    importingJob.LastJobStatus.Parameters,
				ImportingList: importingList,
			}
			startFailedJobsPollTime := time.Now()
			brt.logger.Debugf("[Batch Router] Fetching Failed Jobs Started for Dest Type %v", brt.destType)
			uploadStatsResp := brt.asyncDestinationStruct[destinationID].Manager.GetUploadStats(getUploadStatsInput)
			brt.asyncFailedJobsTimeStat.Since(startFailedJobsPollTime)

			if uploadStatsResp.StatusCode != http.StatusOK {
				brt.logger.Errorf("[Batch Router] Failed to fetch failed jobs for Dest Type %v with statusCode %v", brt.destType, uploadStatsResp.StatusCode)
				return errors.New("failed to fetch failed jobs")
			}

			var statusList []*jobsdb.JobStatusT
			var abortedJobs []*jobsdb.JobT
			successfulJobIDs := append(uploadStatsResp.Metadata.SucceededKeys, uploadStatsResp.Metadata.WarningKeys...)
			for _, job := range importingList {
				jobID := job.JobID
				var status *jobsdb.JobStatusT
				if slices.Contains(successfulJobIDs, jobID) {
					status = &jobsdb.JobStatusT{
						JobID:         jobID,
						JobState:      jobsdb.Succeeded.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorResponse: []byte(`{}`),
						Parameters:    []byte(`{}`),
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
				} else if slices.Contains(uploadStatsResp.Metadata.FailedKeys, jobID) {
					errorRespString := uploadStatsResp.Metadata.FailedReasons[jobID]
					errorResp, _ := json.Marshal(ErrorResponse{Error: errorRespString})
					status = &jobsdb.JobStatusT{
						JobID:         jobID,
						JobState:      jobsdb.Aborted.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorResponse: errorResp,
						Parameters:    []byte(`{}`),
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					abortedJobs = append(abortedJobs, job)
				}
				statusList = append(statusList, status)
			}
			brt.asyncSuccessfulJobCount.Count(len(statusList) - len(abortedJobs))
			brt.asyncAbortedJobCount.Count(len(abortedJobs))
			if len(abortedJobs) > 0 {
				err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
					return brt.errorDB.Store(ctx, abortedJobs)
				}, brt.sendRetryStoreStats)
				if err != nil {
					brt.logger.Errorf("[Batch Router] Failed to store aborted jobs for Dest Type %v with error %v", brt.destType, err)
					// TODO: check this error
					return err
				}
			}
			if err := brt.updateJobStatuses(ctx, destinationID, statusList); err != nil {
				brt.logger.Errorf("[Batch Router] Failed to update job status for Dest Type %v with error %v", brt.destType, err)
				return err
			}
			brt.updateProcessedEventsMetrics(statusList)
		}
	} else if pollResp.StatusCode == http.StatusBadRequest {
		statusList := prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Aborted.State})
		if err := brt.updateJobStatuses(ctx, destinationID, statusList); err != nil {
			brt.logger.Errorf("[Batch Router] Failed to update job status for Dest Type %v with error %v", brt.destType, err)
			return err
		}
		brt.asyncAbortedJobCount.Count(len(statusList))
		brt.updateProcessedEventsMetrics(statusList)
	} else {
		statusList := prepareJobStatusList(importingList, jobsdb.JobStatusT{JobState: jobsdb.Failed.State})
		if err := brt.updateJobStatuses(ctx, destinationID, statusList); err != nil {
			brt.logger.Errorf("[Batch Router] Failed to update job status for Dest Type %v with error %v", brt.destType, err)
			return err
		}
		brt.asyncFailedJobCount.Count(len(statusList))
		brt.updateProcessedEventsMetrics(statusList)
	}
	return nil
}

func (brt *Handle) pollAsyncStatus(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(brt.pollStatusLoopSleep):
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			brt.configSubscriberMu.RUnlock()

			for destinationID := range destinationsMap {
				brt.logger.Debugf("pollAsyncStatus Started for Dest type: %s", brt.destType)
				job, err := brt.getImportingJobs(ctx, destinationID, 1)
				if err != nil {
					// TODO: Add metrics
					brt.logger.Errorf("Error while getting job for dest type: %s, err: %v", brt.destType, err)
					continue
				}
				importingJobs := job.Jobs
				if len(importingJobs) != 0 {
					importingJob := importingJobs[0]
					pollInput := getPollInput(importingJob)
					startPollTime := time.Now()
					brt.logger.Debugf("[Batch Router] Poll Status Started for Dest Type %v", brt.destType)
					pollResp := brt.asyncDestinationStruct[destinationID].Manager.Poll(pollInput)
					brt.logger.Debugf("[Batch Router] Poll Status Finished for Dest Type %v", brt.destType)
					brt.asyncPollTimeStat.Since(startPollTime)
					if pollResp.InProgress {
						continue
					}
					err = brt.updatePollStatusToDB(ctx, destinationID, importingJob, pollResp)
					if err == nil {
						brt.asyncDestinationStruct[destinationID].UploadInProgress = false
					}
				}
			}
		}
	}
}

func (brt *Handle) asyncUploadWorker(ctx context.Context) {
	if !slices.Contains(asyncDestinations, brt.destType) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
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
					uploadResponse := brt.asyncDestinationStruct[destinationID].Manager.Upload(brt.asyncDestinationStruct[destinationID])
					if uploadResponse.ImportingParameters != nil {
						brt.asyncDestinationStruct[destinationID].UploadInProgress = true
					}
					brt.setMultipleJobStatus(uploadResponse, brt.asyncDestinationStruct[destinationID].RsourcesStats)
					brt.asyncStructCleanUp(destinationID)
				}
				brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
			}
		}
	}
}

func (brt *Handle) asyncStructSetup(sourceID, destinationID string) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", sourceID, uuid.String()))
	jsonPath := fmt.Sprintf(`%v.txt`, path)
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	brt.asyncDestinationStruct[destinationID].Exists = true
	brt.asyncDestinationStruct[destinationID].FileName = jsonPath
	brt.asyncDestinationStruct[destinationID].CreatedAt = time.Now()
	brt.asyncDestinationStruct[destinationID].RsourcesStats = rsources.NewStatsCollector(brt.rsourcesService)
}

func (brt *Handle) asyncStructCleanUp(destinationID string) {
	misc.RemoveFilePaths(brt.asyncDestinationStruct[destinationID].FileName)
	brt.asyncDestinationStruct[destinationID].ImportingJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].FailedJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].Size = 0
	brt.asyncDestinationStruct[destinationID].Exists = false
	brt.asyncDestinationStruct[destinationID].Count = 0
	brt.asyncDestinationStruct[destinationID].CanUpload = false
	brt.asyncDestinationStruct[destinationID].URL = ""
	brt.asyncDestinationStruct[destinationID].RsourcesStats = rsources.NewStatsCollector(brt.rsourcesService)
}

func (brt *Handle) sendJobsToStorage(batchJobs BatchedJobs) {
	destinationID := batchJobs.Connection.Destination.ID
	if brt.disableEgress {
		out := common.AsyncUploadOutput{
			DestinationID: destinationID,
		}
		for _, job := range batchJobs.Jobs {
			out.SucceededJobIDs = append(out.SucceededJobIDs, job.JobID)
			out.SuccessResponse = fmt.Sprintf(`{"error":"%s"`, rterror.DisabledEgress.Error()) // skipcq: GO-R4002
		}

		// rsources stats
		rsourcesStats := rsources.NewStatsCollector(brt.rsourcesService)
		rsourcesStats.BeginProcessing(batchJobs.Jobs)

		brt.setMultipleJobStatus(out, rsourcesStats)
		return
	}

	_, ok := brt.asyncDestinationStruct[destinationID]
	if ok {
		brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()
		defer brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
		if brt.asyncDestinationStruct[destinationID].CanUpload {
			out := common.AsyncUploadOutput{
				DestinationID: destinationID,
			}
			for _, job := range batchJobs.Jobs {
				out.FailedJobIDs = append(out.FailedJobIDs, job.JobID)
				out.FailedReason = `{"error":"Jobs flowed over the prescribed limit"}`
			}

			// rsources stats
			rsourcesStats := rsources.NewStatsCollector(brt.rsourcesService)
			rsourcesStats.BeginProcessing(batchJobs.Jobs)

			brt.setMultipleJobStatus(out, rsourcesStats)
			return
		}
	}

	if !ok || !brt.asyncDestinationStruct[destinationID].Exists {
		if !ok {
			asyncStruct := &common.AsyncDestinationStruct{}
			asyncStruct.UploadMutex.Lock()
			defer asyncStruct.UploadMutex.Unlock()
			brt.asyncDestinationStruct[destinationID] = asyncStruct
		}
		brt.asyncStructSetup(batchJobs.Connection.Source.ID, destinationID)
	}

	file, err := os.OpenFile(brt.asyncDestinationStruct[destinationID].FileName, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		panic(fmt.Errorf("BRT: %s: file open failed : %s", brt.destType, err.Error()))
	}
	defer func() { _ = file.Close() }()
	var jobString string
	writeAtBytes := brt.asyncDestinationStruct[destinationID].Size

	brt.asyncDestinationStruct[destinationID].RsourcesStats.BeginProcessing(batchJobs.Jobs)
	for _, job := range batchJobs.Jobs {
		transformedData := common.GetTransformedData(job.EventPayload)
		if brt.asyncDestinationStruct[destinationID].Count < brt.maxEventsInABatch ||
			!brt.asyncDestinationStruct[destinationID].UploadInProgress {
			fileData := asyncdestinationmanager.GetMarshalledData(transformedData, job.JobID)
			brt.asyncDestinationStruct[destinationID].Size = brt.asyncDestinationStruct[destinationID].Size + len([]byte(fileData+"\n"))
			jobString = jobString + fileData + "\n"
			brt.asyncDestinationStruct[destinationID].ImportingJobIDs = append(brt.asyncDestinationStruct[destinationID].ImportingJobIDs, job.JobID)
			brt.asyncDestinationStruct[destinationID].Count = brt.asyncDestinationStruct[destinationID].Count + 1
			brt.asyncDestinationStruct[destinationID].URL = gjson.Get(string(job.EventPayload), "endpoint").String()
		} else {
			brt.logger.Debugf("BRT: Max Event Limit Reached.Stopped writing to File  %s", brt.asyncDestinationStruct[destinationID].FileName)
			brt.asyncDestinationStruct[destinationID].URL = gjson.Get(string(job.EventPayload), "endpoint").String()
			brt.asyncDestinationStruct[destinationID].FailedJobIDs = append(brt.asyncDestinationStruct[destinationID].FailedJobIDs, job.JobID)
		}
	}

	_, err = file.WriteAt([]byte(jobString), int64(writeAtBytes))
	// there can be some race condition with asyncUploadWorker
	if brt.asyncDestinationStruct[destinationID].Count >= brt.maxEventsInABatch {
		brt.asyncDestinationStruct[destinationID].CanUpload = true
	}
	if err != nil {
		panic(fmt.Errorf("BRT: %s: file write failed : %s", brt.destType, err.Error()))
	}
}

func (brt *Handle) setMultipleJobStatus(asyncOutput common.AsyncUploadOutput, rsourcesStats rsources.StatsCollector) {
	jobParameters := []byte(fmt.Sprintf(`{"destination_id": %q}`, asyncOutput.DestinationID)) // TODO: there should be a consistent way of finding the actual job parameters
	workspaceID := brt.GetWorkspaceIDForDestID(asyncOutput.DestinationID)
	var statusList []*jobsdb.JobStatusT
	if len(asyncOutput.ImportingJobIDs) > 0 {
		for _, jobId := range asyncOutput.ImportingJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Importing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{}`),
				Parameters:    asyncOutput.ImportingParameters, // pollUrl remains here
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.SucceededJobIDs) > 0 {
		for _, jobId := range asyncOutput.FailedJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Succeeded.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: stdjson.RawMessage(asyncOutput.SuccessResponse),
				Parameters:    []byte(`{}`),
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.FailedJobIDs) > 0 {
		for _, jobId := range asyncOutput.FailedJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Failed.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "500",
				ErrorResponse: stdjson.RawMessage(asyncOutput.FailedReason),
				Parameters:    []byte(`{}`),
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.AbortJobIDs) > 0 {
		for _, jobId := range asyncOutput.AbortJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "400",
				ErrorResponse: stdjson.RawMessage(asyncOutput.AbortReason),
				Parameters:    []byte(`{}`),
				JobParameters: jobParameters,
				WorkspaceId:   workspaceID,
			}
			statusList = append(statusList, &status)
		}
	}

	if len(statusList) == 0 {
		return
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:  "destination_id",
			Value: asyncOutput.DestinationID,
		},
	}

	// Mark the status of the jobs
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
			if err != nil {
				brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
				return err
			}
			// rsources stats
			rsourcesStats.JobStatusesUpdated(statusList)
			err = rsourcesStats.Publish(context.TODO(), tx.SqlTx())
			if err != nil {
				brt.logger.Errorf("publishing rsources stats: %w", err)
			}
			return err
		})
	}, brt.sendRetryUpdateStats)
	if err != nil {
		panic(err)
	}
	brt.updateProcessedEventsMetrics(statusList)
}

func (brt *Handle) GetWorkspaceIDForDestID(destID string) string {
	var workspaceID string

	brt.configSubscriberMu.RLock()
	defer brt.configSubscriberMu.RUnlock()
	workspaceID = brt.destinationsMap[destID].Sources[0].WorkspaceID

	return workspaceID
}
