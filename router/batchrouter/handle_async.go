// file related to marketo
package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/rterror"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
)

func (brt *Handle) pollAsyncStatus(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(brt.pollStatusLoopSleep): // 10 sec
			brt.configSubscriberMu.RLock()
			destinationsMap := brt.destinationsMap
			brt.configSubscriberMu.RUnlock()

			for key := range destinationsMap {
				if slices.Contains(asyncDestinations, brt.destType) {
					brt.logger.Debugf("pollAsyncStatus Started for Dest type: %s", brt.destType)
					// below line parameterFilters eg: [{Name: "destination_id", Value: "2PriULYidWaynFpp6jeAF3ugZUc"}]
					parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: key}}
					// job means Jobs [], LimitsReached bool, EventsCount, PayloadSize
					job, err := misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
						return brt.jobsDB.GetImporting(
							ctx,
							jobsdb.GetQueryParamsT{
								CustomValFilters: []string{brt.destType},
								JobsLimit:        1,
								ParameterFilters: parameterFilters,
								PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit),
							},
						)
					}, brt.sendQueryRetryStats)
					if err != nil {
						brt.logger.Errorf("Error while getting job for dest type: %s, err: %v", brt.destType, err)
						panic(err)
					}
					// ?? if job parameters will look same for bingAds as well
					importingJob := job.Jobs
					// in this importing job eventPayload resides, after file upload , transformer response
					//, eg: "{\"body\": {\"XML\": {}, \"FORM\": {}, \"JSON\": {\"email\": \"test@kinesis.com\", \"address\": 23, \"anonymousId\": \"Test Kinesis\"}, \"JSON_ARRAY\": {}}, \"type\": \"REST\", \"files\": {}, \"method\": \"POST\", \"params\": {}, \"userId\": \"\", \"headers\": {}, \"version\": \"1\", \"endpoint\": \"/fileUpload\"}"
					if len(importingJob) != 0 {
						importingJob := importingJob[0]
						// fields needs to be added
						// parameters eg : "{\"pollURL\": \"/pollStatus\", \"importId\": \"3090\", \"metadata\": {\"csvHeader\": \"anonymousId,address,email\"}}" <-- response of
						// transformer fileupload.js file
						parameters := importingJob.LastJobStatus.Parameters
						importId := gjson.GetBytes(parameters, "importId").String()
						var pollStruct common.AsyncPoll
						pollStruct.ImportId = importId
						pollStruct.Config = destinationsMap[key].Destination.Config
						pollStruct.DestType = strings.ToLower(brt.destType)
						// payload, err := json.Marshal(pollStruct)
						// if err != nil {
						// 	panic("JSON Marshal Failed" + err.Error())
						// }

						startPollTime := time.Now()
						brt.logger.Debugf("[Batch Router] Poll Status Started for Dest Type %v", brt.destType)
						var bodyBytes []byte
						var statusCode int
						var pollResp common.AsyncStatusResponse
						// payload to be sent to poll : "{\"config\":{\"clientId\":\"01a70f1f-ff37-46fc-bdff-42e92a3f2bb3\",\"clientSecret\":\"rziQBHtZ34Vl1CE3x3OiA3n8Wr45lwar\",\"columnFieldsMapping\":[{\"from\":\"anonymousId\",\"to\":\"anonymousId\"},{\"from\":\"address\",\"to\":\"address\"},{\"from\":\"email\",\"to\":\"email\"}],\"deDuplicationField\":\"\",\"munchkinId\":\"585-AXP-425\",\"oneTrustCookieCategories\":[],\"uploadInterval\":\"10\"},\"importId\":\"3090\",\"destType\":\"marketo_bulk_upload\"}"
						pollResp, statusCode = brt.asyncdestinationmanager.Poll(pollStruct)
						bodyBytes, err := stdjson.Marshal(pollResp)
						if err != nil {
							panic(err)
						}
						// bodyBytes eg: "{\"success\":true,\"statusCode\":200,\"hasFailed\":false,\"failedJobsURL\":\"/getFailedJobs\",\"hasWarnings\":false,\"warningJobsURL\":\"/getWarningJobs\"}"
						brt.logger.Debugf("[Batch Router] Poll Status Finished for Dest Type %v", brt.destType)
						brt.asyncPollTimeStat.Since(startPollTime)

						if err != nil {
							panic("HTTP Request Failed" + err.Error())
						}
						if statusCode == 200 {
							var asyncResponse common.AsyncStatusResponse
							if err != nil {
								panic("Read Body Failed" + err.Error())
							}
							err = json.Unmarshal(bodyBytes, &asyncResponse)
							if err != nil {
								panic("JSON Unmarshal Failed" + err.Error())
							}

							// need to think if these parameters calculation can
							// be different in bingAds and marketo
							uploadStatus := asyncResponse.Success
							statusCode := asyncResponse.StatusCode
							abortedJobs := make([]*jobsdb.JobT, 0)
							// when we can mark upload status as false?
							if uploadStatus {
								var statusList []*jobsdb.JobStatusT
								list, err := misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
									return brt.jobsDB.GetImporting(
										ctx,
										jobsdb.GetQueryParamsT{
											CustomValFilters: []string{brt.destType},
											JobsLimit:        brt.maxEventsInABatch,
											ParameterFilters: parameterFilters,
											PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit),
										},
									)
								}, brt.sendQueryRetryStats)
								if err != nil {
									panic(err)
								}

								// list.Jobs has parameters eg : "{\"record_id\": null, \"source_id\": \"2PuNovG1jg5F3UIUZpnhlwMhw7c\", \"event_name\": \"\", \"event_type\": \"identify\", \"message_id\": \"e686323c-f926-4e2b-b606-121e1948eafb\", \"received_at\": \"2023-05-29T19:58:14.380+05:30\", \"workspaceId\": \"1kgRLW9E68SaJx6WiHavEABeSAl\", \"transform_at\": \"processor\", \"source_job_id\": \"\", \"destination_id\": \"2PriULYidWaynFpp6jeAF3ugZUc\", \"gateway_job_id\": 5, \"source_category\": \"\", \"source_job_run_id\": \"\", \"source_task_run_id\": \"\", \"source_definition_id\": \"1b6gJdqOPOCadT3cddw8eidV591\", \"destin"
								// list.Jobs has eventPaylaod. Eg: "{\"body\": {\"XML\": {}, \"FORM\": {}, \"JSON\": {\"email\": \"test@kinesis.com\", \"address\": 23, \"anonymousId\": \"Test Kinesis\"}, \"JSON_ARRAY\": {}}, \"type\": \"REST\", \"files\": {}, \"method\": \"POST\", \"params\": {}, \"userId\": \"\", \"headers\": {}, \"version\": \"1\", \"endpoint\": \"/fileUpload\"}"
								importingList := list.Jobs
								var failedJobsStatus common.FetchFailedStatus
								failedJobsStatus.FailedJobsURL = asyncResponse.FailedJobsURL
								failedJobsStatus.Parameters = importingJob.LastJobStatus.Parameters
								failedJobsStatus.ImportingList = importingList
								failedJobsStatus.OutputFilePath = asyncResponse.OutputFilePath
								// asyncResponse eg : {Success: true, StatusCode: 200, HasFailed: false, HasWarning: false, FailedJobsURL: "/getFailedJobs", WarningJobsURL: "/getWarningJobs"}
								if !asyncResponse.HasFailed {
									for _, job := range importingList {
										// status eg github.com/rudderlabs/rudder-server/jobsdb.JobStatusT {JobID: 3, JobState: "succeeded", AttemptNum: 0, ExecTime: time.Time(2023-05-29T21:20:11+05:30, +252581321648){wall: 13913114914768609472, ext: 252581321648, loc: *(*time.Location)(0x105adafa0)}, RetryT...
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Succeeded.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											JobParameters: job.Parameters,
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
									}
									brt.asyncSuccessfulJobCount.Count(len(statusList))
								} else {
									startFailedJobsPollTime := time.Now()
									brt.logger.Debugf("[Batch Router] Fetching Failed Jobs Started for Dest Type %v", brt.destType)
									failedBodyBytes, statusCode := brt.asyncdestinationmanager.FetchFailedEvents(failedJobsStatus)
									brt.logger.Debugf("[Batch Router] Fetching Failed Jobs for Dest Type %v", brt.destType)
									brt.asyncFailedJobsTimeStat.Since(startFailedJobsPollTime)

									if statusCode != 200 {
										continue
									}
									var failedJobsResponse map[string]interface{}
									err = json.Unmarshal(failedBodyBytes, &failedJobsResponse)
									if err != nil {
										panic("JSON Unmarshal Failed" + err.Error())
									}
									internalStatusCode, ok := failedJobsResponse["status"].(string)
									if internalStatusCode != "200" || !ok {
										brt.logger.Errorf("[Batch Router] Failed to fetch failed jobs for Dest Type %v with statusCode %v and body %v", brt.destType, internalStatusCode, string(failedBodyBytes))
										continue
									}
									metadata, ok := failedJobsResponse["metadata"].(map[string]interface{})
									if !ok {
										brt.logger.Errorf("[Batch Router] Failed to typecast failed jobs response for Dest Type %v with statusCode %v and body %v with error %v", brt.destType, internalStatusCode, string(failedBodyBytes), err)
										continue
									}

									failedKeys, errFailed := brt.asyncdestinationmanager.RetrieveImportantKeys(metadata, "failedKeys")
									warningKeys, errWarning := brt.asyncdestinationmanager.RetrieveImportantKeys(metadata, "warningKeys")
									succeededKeys, errSuccess := brt.asyncdestinationmanager.RetrieveImportantKeys(metadata, "succeededKeys")
									var status *jobsdb.JobStatusT
									if errFailed != nil || errWarning != nil || errSuccess != nil || statusCode != 200 {
										for _, job := range importingList {
											jobID := job.JobID
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Failed.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     strconv.Itoa(statusCode),
												ErrorResponse: []byte(`{}`),
												Parameters:    []byte(`{}`),
												JobParameters: job.Parameters,
												WorkspaceId:   job.WorkspaceId,
											}
											statusList = append(statusList, status)
										}
										brt.asyncFailedJobCount.Count(len(statusList))
										err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
											return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
												err = brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
												if err != nil {
													return fmt.Errorf("updating %s job statuses: %w", brt.destType, err)
												}
												// no need to update rsources stats here since no terminal job state is recorded
												return nil
											})
										}, brt.sendRetryUpdateStats)
										if err != nil {
											panic(err)
										}
										brt.updateProcessedEventsMetrics(statusList)
										continue
									}
									for _, job := range importingList {
										jobID := job.JobID
										if slices.Contains(append(succeededKeys, warningKeys...), jobID) {
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Succeeded.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     "200",
												ErrorResponse: []byte(`{}`),
												Parameters:    []byte(`{}`),
												JobParameters: job.Parameters,
												WorkspaceId:   job.WorkspaceId,
											}
										} else if slices.Contains(failedKeys, job.JobID) {
											errorResp, _ := json.Marshal(ErrorResponse{Error: gjson.GetBytes(failedBodyBytes, fmt.Sprintf("metadata.failedReasons.%v", job.JobID)).String()})
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Aborted.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     "",
												ErrorResponse: errorResp,
												Parameters:    []byte(`{}`),
												JobParameters: job.Parameters,
												WorkspaceId:   job.WorkspaceId,
											}
											abortedJobs = append(abortedJobs, job)
										}
										statusList = append(statusList, status)
									}
								}
								brt.asyncSuccessfulJobCount.Count(len(statusList) - len(abortedJobs))
								brt.asyncAbortedJobCount.Count(len(abortedJobs))

								if len(abortedJobs) > 0 {
									err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
										return brt.errorDB.Store(ctx, abortedJobs)
									}, brt.sendRetryStoreStats)
									if err != nil {
										panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
									}
								}
								err = misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
									return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
										err = brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
										if err != nil {
											return fmt.Errorf("updating %s job statuses: %w", brt.destType, err)
										}

										// rsources stats
										return brt.updateRudderSourcesStats(ctx, tx, importingList, statusList)
									})
								}, brt.sendRetryUpdateStats)
								if err != nil {
									panic(err)
								}
								brt.updateProcessedEventsMetrics(statusList)
							} else if statusCode != 0 {
								var statusList []*jobsdb.JobStatusT
								list, err := misc.QueryWithRetriesAndNotify(ctx, brt.jobdDBQueryRequestTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
									return brt.jobsDB.GetImporting(
										ctx,
										jobsdb.GetQueryParamsT{
											CustomValFilters: []string{brt.destType},
											JobsLimit:        brt.maxEventsInABatch,
											ParameterFilters: parameterFilters,
											PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit),
										},
									)
								}, brt.sendQueryRetryStats)
								if err != nil {
									panic(err)
								}

								importingList := list.Jobs
								if IsJobTerminated(statusCode) {
									for _, job := range importingList {
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Aborted.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											JobParameters: job.Parameters,
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
										abortedJobs = append(abortedJobs, job)
									}
									brt.asyncAbortedJobCount.Count(len(importingList))
								} else {
									for _, job := range importingList {
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Failed.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											JobParameters: job.Parameters,
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
									}
									brt.asyncFailedJobCount.Count(len(importingList))
								}
								if len(abortedJobs) > 0 {
									err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
										return brt.errorDB.Store(ctx, abortedJobs)
									}, brt.sendRetryStoreStats)
									if err != nil {
										panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
									}
								}

								err = misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout, brt.jobdDBMaxRetries, func(ctx context.Context) error {
									return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
										err = brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
										if err != nil {
											return fmt.Errorf("updating %s job statuses: %w", brt.destType, err)
										}
										// rsources stats
										return brt.updateRudderSourcesStats(ctx, tx, importingList, statusList)
									})
								}, brt.sendRetryUpdateStats)
								if err != nil {
									panic(err)
								}
								brt.updateProcessedEventsMetrics(statusList)
							} else {
								continue
							}
						} else {
							continue
						}

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
				if !ok {
					continue
				}

				timeElapsed := time.Since(brt.asyncDestinationStruct[destinationID].CreatedAt)
				brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()

				timeout := uploadIntervalMap[destinationID]
				if brt.asyncDestinationStruct[destinationID].Exists && (brt.asyncDestinationStruct[destinationID].CanUpload || timeElapsed > timeout) {
					brt.asyncDestinationStruct[destinationID].CanUpload = true
					var uploadResponse common.AsyncUploadOutput
					uploadResponse = brt.asyncdestinationmanager.Upload(brt.destination, brt.asyncDestinationStruct[destinationID])
					// pollUrl remains under ImportingParameters
					// eg : uploadResponse.ImportingParameters =  "{\"importId\":\"3100\",\"pollURL\":\"/pollStatus\",\"metadata\":{\"csvHeader\":\"anonymousId,address,email\"}}"
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
	if brt.disableEgress {
		out := common.AsyncUploadOutput{}
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

	destinationID := batchJobs.Connection.Destination.ID
	_, ok := brt.asyncDestinationStruct[destinationID]
	if ok {
		brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()
		defer brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
		if brt.asyncDestinationStruct[destinationID].CanUpload {
			out := common.AsyncUploadOutput{}
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
		if brt.asyncDestinationStruct[destinationID].Count < brt.maxEventsInABatch {
			fileData := asyncdestinationmanager.GetMarshalledData(transformedData, job.JobID)
			brt.asyncDestinationStruct[destinationID].Size = brt.asyncDestinationStruct[destinationID].Size + len([]byte(fileData+"\n"))
			jobString = jobString + fileData + "\n"
			brt.asyncDestinationStruct[destinationID].ImportingJobIDs = append(brt.asyncDestinationStruct[destinationID].ImportingJobIDs, job.JobID)
			brt.asyncDestinationStruct[destinationID].Count = brt.asyncDestinationStruct[destinationID].Count + 1
			brt.asyncDestinationStruct[destinationID].URL = gjson.Get(string(job.EventPayload), "endpoint").String()
		} else {
			// brt.asyncDestinationStruct[destinationID].CanUpload = true
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
	workspace := brt.GetWorkspaceIDForDestID(asyncOutput.DestinationID)
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
				WorkspaceId:   workspace,
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
				WorkspaceId:   workspace,
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
				WorkspaceId:   workspace,
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
				WorkspaceId:   workspace,
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
