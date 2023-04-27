package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
)

// worker a structure to define a worker for sending events to sinks
type worker struct {
	id int // identifies the worker

	rt *HandleT // handle to router

	input             chan workerMessageT // the worker's input channel
	inputReservations int                 // number of slots reserved in the input channel
	barrier           *eventorder.Barrier // barrier to ensure ordering of events

	routerJobs      []types.RouterJobT      // slice to hold router jobs to send to destination transformer
	destinationJobs []types.DestinationJobT // slice to hold destination jobs

	deliveryTimeStat          stats.Measurement
	routerDeliveryLatencyStat stats.Measurement
	routerProxyStat           stats.Measurement
	batchTimeStat             stats.Measurement
	latestAssignedTime        time.Time
	processingStartTime       time.Time
}

// workerSlot represents a reserved slot in the worker's input channel
type workerSlot struct {
	worker *worker
}

// Release releases the reserved slot from the worker's input channel
func (s *workerSlot) Release() {
	s.worker.releaseSlot()
}

// Use sends a job into the worker's input channel
func (s *workerSlot) Use(msg workerMessageT) {
	s.worker.accept(msg)
}

type workerMessageT struct {
	job        *jobsdb.JobT
	assignedAt time.Time
}

// AvailableSlots returns the number of available slots in the worker's input channel
func (w *worker) AvailableSlots() int {
	return cap(w.input) - len(w.input) - w.inputReservations
}

// Reserve tries to reserve a slot in the worker's input channel, if available
func (w *worker) ReserveSlot() *workerSlot {
	if w.AvailableSlots() > 0 {
		w.inputReservations++
		return &workerSlot{worker: w}
	}
	return nil
}

// releaseSlot releases a slot from the worker's input channel
func (w *worker) releaseSlot() {
	if w.inputReservations > 0 {
		w.inputReservations--
	}
}

// accept accepts a job into the worker's input channel
func (w *worker) accept(msg workerMessageT) {
	w.releaseSlot()
	w.input <- msg
}

func (w *worker) WorkerProcess() {
	timeout := time.After(jobsBatchTimeout)
	for {
		select {
		case message, hasMore := <-w.input:
			if !hasMore {
				if len(w.routerJobs) == 0 {
					w.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", w.rt.destName, len(w.routerJobs))
					return
				}

				if w.rt.enableBatching {
					w.destinationJobs = w.batchTransform(w.routerJobs)
				} else {
					w.destinationJobs = w.transform(w.routerJobs)
				}
				w.processDestinationJobs()
				w.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", w.rt.destName, len(w.routerJobs))
				return
			}

			w.rt.logger.Debugf("[%v Router] :: performing checks to send payload.", w.rt.destName)

			job := message.job
			userID := job.UserID

			var parameters JobParametersT
			if err := json.Unmarshal(job.Parameters, &parameters); err != nil {
				panic(fmt.Errorf("unmarshalling of job parameters failed for job %d (%s): %w", job.JobID, string(job.Parameters), err))
			}
			w.rt.configSubscriberLock.RLock()
			abort, abortReason := routerutils.ToBeDrained(job, parameters.DestinationID, toAbortDestinationIDs, w.rt.destinationsMap)
			w.rt.configSubscriberLock.RUnlock()

			if !abort {
				abort = w.retryLimitReached(&job.LastJobStatus)
				abortReason = "retry limit reached"
			}
			if abort {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.Aborted.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     strconv.Itoa(routerutils.DRAIN_ERROR_CODE),
					Parameters:    routerutils.EmptyPayload,
					JobParameters: job.Parameters,
					ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "reason", abortReason),
					WorkspaceId:   job.WorkspaceId,
				}
				// Enhancing job parameter with the drain reason.
				job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "router")
				job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", abortReason)
				w.rt.responseQ <- jobResponseT{status: &status, worker: w, userID: userID, JobT: job}
				stats.Default.NewTaggedStat(`drained_events`, stats.CountType, stats.Tags{
					"destType":    w.rt.destName,
					"destId":      parameters.DestinationID,
					"module":      "router",
					"reasons":     abortReason,
					"workspaceId": job.WorkspaceId,
				}).Count(1)
				continue
			}

			if w.rt.guaranteeUserEventOrder {
				orderKey := jobOrderKey(userID, parameters.DestinationID)
				if wait, previousFailedJobID := w.barrier.Wait(orderKey, job.JobID); wait {
					previousFailedJobIDStr := "<nil>"
					if previousFailedJobID != nil {
						previousFailedJobIDStr = strconv.FormatInt(*previousFailedJobID, 10)
					}
					w.rt.logger.Debugf("EventOrder: [%d] job %d of key %s must wait (previousFailedJobID: %s)",
						w.id, job.JobID, orderKey, previousFailedJobIDStr,
					)

					// mark job as waiting if prev job from same user has not succeeded yet
					w.rt.logger.Debugf(
						"[%v Router] :: skipping processing job for orderKey: %v since prev failed job exists, prev id %v, current id %v",
						w.rt.destName, orderKey, previousFailedJobID, job.JobID,
					)
					resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", *previousFailedJobID)
					resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", userID)
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						JobState:      jobsdb.Waiting.State,
						ErrorResponse: resp, // check
						Parameters:    routerutils.EmptyPayload,
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					w.rt.responseQ <- jobResponseT{status: &status, worker: w, userID: userID, JobT: job}
					continue
				}
			}

			firstAttemptedAt := gjson.GetBytes(job.LastJobStatus.ErrorResponse, "firstAttemptedAt").Str
			jobMetadata := types.JobMetadataT{
				UserID:             userID,
				JobID:              job.JobID,
				SourceID:           parameters.SourceID,
				DestinationID:      parameters.DestinationID,
				AttemptNum:         job.LastJobStatus.AttemptNum,
				ReceivedAt:         parameters.ReceivedAt,
				CreatedAt:          job.CreatedAt.Format(misc.RFC3339Milli),
				FirstAttemptedAt:   firstAttemptedAt,
				TransformAt:        parameters.TransformAt,
				JobT:               job,
				WorkspaceID:        parameters.WorkspaceID,
				WorkerAssignedTime: message.assignedAt,
			}

			w.rt.configSubscriberLock.RLock()
			batchDestination, ok := w.rt.destinationsMap[parameters.DestinationID]
			w.rt.configSubscriberLock.RUnlock()
			if !ok {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.Failed.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorResponse: []byte(`{"reason":"failed because destination is not available in the config"}`),
					Parameters:    routerutils.EmptyPayload,
					JobParameters: job.Parameters,
					WorkspaceId:   job.WorkspaceId,
				}
				if w.rt.guaranteeUserEventOrder {
					orderKey := jobOrderKey(job.UserID, parameters.DestinationID)
					w.rt.logger.Debugf("EventOrder: [%d] job %d for key %s failed", w.id, status.JobID, orderKey)
					if err := w.barrier.StateChanged(orderKey, job.JobID, status.JobState); err != nil {
						panic(err)
					}
				}
				w.rt.responseQ <- jobResponseT{status: &status, worker: w, userID: userID, JobT: job}
				continue
			}
			destination := batchDestination.Destination
			if authType := oauth.GetAuthType(destination.DestinationDefinition.Config); authType == oauth.OAuth {
				rudderAccountID := oauth.GetAccountId(destination.Config, oauth.DeliveryAccountIdKey)

				if routerutils.IsNotEmptyString(rudderAccountID) {
					w.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method to be called`, destination.DestinationDefinition.Name)
					// Get Access Token Information to send it as part of the event
					tokenStatusCode, accountSecretInfo := w.rt.oauth.FetchToken(&oauth.RefreshTokenParams{
						AccountId:       rudderAccountID,
						WorkspaceId:     jobMetadata.WorkspaceID,
						DestDefName:     destination.DestinationDefinition.Name,
						EventNamePrefix: "fetch_token",
					})
					w.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method finished (statusCode, value): (%v, %+v)`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo)
					if tokenStatusCode == http.StatusOK {
						jobMetadata.Secret = accountSecretInfo.Account.Secret
					} else {
						w.rt.logger.Errorf(`[%s][FetchToken] Token Fetch Method error (statusCode, error): (%d, %s)`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo.Err)
					}
				}
			}

			if w.rt.enableBatching {
				w.routerJobs = append(w.routerJobs, types.RouterJobT{
					Message:     job.EventPayload,
					JobMetadata: jobMetadata,
					Destination: destination,
				})

				if len(w.routerJobs) >= w.rt.noOfJobsToBatchInAWorker {
					w.destinationJobs = w.batchTransform(w.routerJobs)
					w.processDestinationJobs()
				}
			} else if parameters.TransformAt == "router" {
				w.routerJobs = append(w.routerJobs, types.RouterJobT{
					Message:     job.EventPayload,
					JobMetadata: jobMetadata,
					Destination: destination,
				})

				if len(w.routerJobs) >= w.rt.noOfJobsToBatchInAWorker {
					w.destinationJobs = w.transform(w.routerJobs)
					w.processDestinationJobs()
				}
			} else {
				w.destinationJobs = append(w.destinationJobs, types.DestinationJobT{
					Message:          job.EventPayload,
					Destination:      destination,
					JobMetadataArray: []types.JobMetadataT{jobMetadata},
				})
				w.processDestinationJobs()
			}

		case <-timeout:
			timeout = time.After(jobsBatchTimeout)

			if len(w.routerJobs) > 0 {
				if w.rt.enableBatching {
					w.destinationJobs = w.batchTransform(w.routerJobs)
				} else {
					w.destinationJobs = w.transform(w.routerJobs)
				}
				w.processDestinationJobs()
			}
		}
	}
}

func (w *worker) trackStuckDelivery() chan struct{} {
	var d time.Duration
	if w.rt.transformerProxy {
		d = (w.rt.backendProxyTimeout + w.rt.netClientTimeout) * 2
	} else {
		d = w.rt.netClientTimeout * 2
	}

	ch := make(chan struct{}, 1)
	rruntime.Go(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(d):
			w.rt.logger.Infof("[%s Router] Delivery to destination exceeded the 2 * configured timeout ", w.rt.destName)
			stat := stats.Default.NewTaggedStat("router_delivery_exceeded_timeout", stats.CountType, stats.Tags{
				"destType": w.rt.destName,
			})
			stat.Increment()
		}
	})
	return ch
}

func (w *worker) recordStatsForFailedTransforms(transformType string, transformedJobs []types.DestinationJobT) {
	for _, destJob := range transformedJobs {
		// Input Stats for batch/router transformation
		stats.Default.NewTaggedStat("router_transform_num_jobs", stats.CountType, stats.Tags{
			"destType":      w.rt.destName,
			"transformType": transformType,
			"statusCode":    strconv.Itoa(destJob.StatusCode),
			"workspaceId":   destJob.Destination.WorkspaceID,
			"destinationId": destJob.Destination.ID,
		}).Count(1)
		if destJob.StatusCode != http.StatusOK {
			transformFailedCountStat := stats.Default.NewTaggedStat("router_transform_num_failed_jobs", stats.CountType, stats.Tags{
				"destType":      w.rt.destName,
				"transformType": transformType,
				"statusCode":    strconv.Itoa(destJob.StatusCode),
				"destination":   destJob.Destination.ID,
			})
			transformFailedCountStat.Count(1)
		}
	}
}

func (w *worker) transform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	w.rt.routerTransformInputCountStat.Count(len(routerJobs))
	destinationJobs := w.rt.transformer.Transform(
		transformer.ROUTER_TRANSFORM,
		&types.TransformMessageT{Data: routerJobs, DestType: strings.ToLower(w.rt.destName)},
	)
	w.rt.routerTransformOutputCountStat.Count(len(destinationJobs))
	w.recordStatsForFailedTransforms("routerTransform", destinationJobs)
	return destinationJobs
}

func (w *worker) batchTransform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	inputJobsLength := len(routerJobs)
	w.rt.batchInputCountStat.Count(inputJobsLength)
	destinationJobs := w.rt.transformer.Transform(
		transformer.BATCH,
		&types.TransformMessageT{
			Data:     routerJobs,
			DestType: strings.ToLower(w.rt.destName),
		},
	)
	w.rt.batchOutputCountStat.Count(len(destinationJobs))
	w.recordStatsForFailedTransforms("batch", destinationJobs)
	return destinationJobs
}

func (w *worker) processDestinationJobs() {
	ctx := context.TODO()
	defer w.batchTimeStat.RecordDuration()()

	var respContentType string
	var respStatusCode, prevRespStatusCode int
	var respBody string
	var respBodyTemp string

	var destinationResponseHandler ResponseHandlerI
	w.rt.configSubscriberLock.RLock()
	destinationResponseHandler = w.rt.destinationResponseHandler
	w.rt.configSubscriberLock.RUnlock()

	/*
		Batch
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		[b1, b2, b3]
		b1 will send if success
		b2 will send if b2 failed then will drop b3

		Router transform
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		200, 200, 500, 200, 200, 200

		Case 1:
		u1e1 will send - success
		u2e1 will send - success
		u1e2 will drop because transformer gave 500
		u2e2 will send - success
		u1e3 should be dropped because u1e2 should be retried
		u2e3 will send

		Case 2:
		u1e1 will send - success
		u2e1 will send - failed 5xx
		u1e2 will send
		u2e2 will drop - because request to destination failed with 5xx
		u1e3 will send
		u2e3 will drop - because request to destination failed with 5xx

		Case 3:
		u1e1 will send - success
		u2e1 will send - failed 4xx
		u1e2 will send
		u2e2 will send - because previous job is aborted
		u1e3 will send
		u2e3 will send
	*/

	failedJobOrderKeys := make(map[string]struct{})
	routerJobResponses := make([]*JobResponse, 0)

	sort.Slice(w.destinationJobs, func(i, j int) bool {
		return w.destinationJobs[i].MinJobID() < w.destinationJobs[j].MinJobID()
	})

	for _, destinationJob := range w.destinationJobs {
		var errorAt string
		respBodyArr := make([]string, 0)
		if destinationJob.StatusCode == 200 || destinationJob.StatusCode == 0 {
			if w.canSendJobToDestination(prevRespStatusCode, failedJobOrderKeys, &destinationJob) {
				diagnosisStartTime := time.Now()
				destinationID := destinationJob.JobMetadataArray[0].DestinationID
				transformAt := destinationJob.JobMetadataArray[0].TransformAt

				// START: request to destination endpoint
				workspaceID := destinationJob.JobMetadataArray[0].JobT.WorkspaceId
				deliveryLatencyStat := stats.Default.NewTaggedStat("delivery_latency", stats.TimerType, stats.Tags{
					"module":      "router",
					"destType":    w.rt.destName,
					"destination": misc.GetTagName(destinationJob.Destination.ID, destinationJob.Destination.Name),
					"workspaceId": workspaceID,
				})
				startedAt := time.Now()

				if w.latestAssignedTime != destinationJob.JobMetadataArray[0].WorkerAssignedTime {
					w.latestAssignedTime = destinationJob.JobMetadataArray[0].WorkerAssignedTime
					w.processingStartTime = time.Now()
				}
				// TODO: remove trackStuckDelivery once we verify it is not needed,
				//			router_delivery_exceeded_timeout -> goes to zero
				ch := w.trackStuckDelivery()

				// Assuming twice the overhead - defensive: 30% was just fine though
				// In fact, the timeout should be more than the maximum latency allowed by these workers.
				// Assuming 10s maximum latency
				elapsed := time.Since(w.processingStartTime)
				threshold := w.rt.routerTimeout
				if elapsed > threshold {
					respStatusCode = types.RouterTimedOutStatusCode
					respBody = fmt.Sprintf("Failed with status code %d as the jobs took more time than expected. Will be retried", types.RouterTimedOutStatusCode)
					w.rt.logger.Debugf(
						"Will drop with %d because of time expiry %v",
						types.RouterTimedOutStatusCode, destinationJob.JobMetadataArray[0].JobID,
					)
				} else if w.rt.customDestinationManager != nil {
					for _, destinationJobMetadata := range destinationJob.JobMetadataArray {
						if destinationID != destinationJobMetadata.DestinationID {
							panic(fmt.Errorf("different destinations are grouped together"))
						}
					}
					respStatusCode, respBody = w.rt.customDestinationManager.SendData(destinationJob.Message, destinationID)
					errorAt = routerutils.ERROR_AT_CUST
				} else {
					result, err := getIterableStruct(destinationJob.Message, transformAt)
					if err != nil {
						errorAt = routerutils.ERROR_AT_TF
						respStatusCode, respBody = types.RouterUnMarshalErrorCode, fmt.Errorf("transformer response unmarshal error: %w", err).Error()
					} else {
						for _, val := range result {
							err := integrations.ValidatePostInfo(val)
							if err != nil {
								errorAt = routerutils.ERROR_AT_TF
								respStatusCode, respBodyTemp = http.StatusBadRequest, fmt.Sprintf(`400 GetPostInfoFailed with error: %s`, err.Error())
								respBodyArr = append(respBodyArr, respBodyTemp)
							} else {
								// stat start
								pkgLogger.Debugf(`responseTransform status :%v, %s`, w.rt.transformerProxy, w.rt.destName)
								// transformer proxy start
								errorAt = routerutils.ERROR_AT_DEL
								if w.rt.transformerProxy {
									jobID := destinationJob.JobMetadataArray[0].JobID
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request started`, w.rt.destName, jobID)

									// setting metadata
									firstJobMetadata := destinationJob.JobMetadataArray[0]
									proxyReqparams := &transformer.ProxyRequestParams{
										DestName: w.rt.destName,
										JobID:    jobID,
										ResponseData: transformer.ProxyRequestPayload{
											PostParametersT: val,
											Metadata: transformer.ProxyRequestMetadata{
												SourceID:      firstJobMetadata.SourceID,
												DestinationID: firstJobMetadata.DestinationID,
												WorkspaceID:   firstJobMetadata.WorkspaceID,
												JobID:         firstJobMetadata.JobID,
												AttemptNum:    firstJobMetadata.AttemptNum,
												DestInfo:      firstJobMetadata.DestInfo,
												Secret:        firstJobMetadata.Secret,
											},
										},
									}
									rtlTime := time.Now()
									respStatusCode, respBodyTemp, respContentType = w.rt.transformer.ProxyRequest(ctx, proxyReqparams)
									w.routerProxyStat.SendTiming(time.Since(rtlTime))
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request ended`, w.rt.destName, jobID)
									authType := oauth.GetAuthType(destinationJob.Destination.DestinationDefinition.Config)
									if routerutils.IsNotEmptyString(string(authType)) && authType == oauth.OAuth {
										pkgLogger.Debugf(`Sending for OAuth destination`)
										// Token from header of the request
										respStatusCode, respBodyTemp = w.rt.HandleOAuthDestResponse(&HandleDestOAuthRespParamsT{
											ctx:            ctx,
											destinationJob: destinationJob,
											workerID:       w.id,
											trRespStCd:     respStatusCode,
											trRespBody:     respBodyTemp,
											secret:         firstJobMetadata.Secret,
										})
									}
								} else {
									sendCtx, cancel := context.WithTimeout(ctx, w.rt.netClientTimeout)
									rdlTime := time.Now()
									resp := w.rt.netHandle.SendPost(sendCtx, val)
									cancel()
									respStatusCode, respBodyTemp, respContentType = resp.StatusCode, string(resp.ResponseBody), resp.ResponseContentType
									// stat end
									w.routerDeliveryLatencyStat.SendTiming(time.Since(rdlTime))
								}
								// transformer proxy end
								if isSuccessStatus(respStatusCode) {
									respBodyArr = append(respBodyArr, respBodyTemp)
								} else {
									respBodyArr = []string{respBodyTemp}
									break
								}
							}
						}
						respBody = strings.Join(respBodyArr, " ")
						if w.rt.transformerProxy {
							stats.Default.NewTaggedStat("transformer_proxy.input_events_count", stats.CountType, stats.Tags{
								"destType":      w.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(result))

							pkgLogger.Debugf(`[TransformerProxy] (Dest-%v) {Job - %v} Input Router Events: %v, Out router events: %v`, w.rt.destName,
								destinationJob.JobMetadataArray[0].JobID,
								len(result),
								len(respBodyArr),
							)

							stats.Default.NewTaggedStat("transformer_proxy.output_events_count", stats.CountType, stats.Tags{
								"destType":      w.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(respBodyArr))
						}
					}
				}
				ch <- struct{}{}
				timeTaken := time.Since(startedAt)
				if respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode {
					w.rt.MultitenantI.UpdateWorkspaceLatencyMap(w.rt.destName, workspaceID, float64(timeTaken)/float64(time.Second))
				}

				// Using response status code and body to get response code rudder router logic is based on.
				// Works when transformer proxy in disabled
				if !w.rt.transformerProxy && destinationResponseHandler != nil {
					respStatusCode = destinationResponseHandler.IsSuccessStatus(respStatusCode, respBody)
				}

				w.deliveryTimeStat.SendTiming(timeTaken)
				deliveryLatencyStat.Since(startedAt)

				// END: request to destination endpoint

				// Failure - Save response body
				// Success - Skip saving response body
				// By default we get some config from dest def
				// We can override via env saveDestinationResponseOverride

				if isSuccessStatus(respStatusCode) && !getRouterConfigBool("saveDestinationResponseOverride", w.rt.destName, false) && !w.rt.saveDestinationResponse {
					respBody = ""
				}

				w.updateReqMetrics(respStatusCode, &diagnosisStartTime)
			} else {
				respStatusCode = http.StatusInternalServerError
				if !w.rt.enableBatching {
					respBody = "skipping sending to destination because previous job (of user) in batch is failed."
				}
				errorAt = routerutils.ERROR_AT_TF
			}
		} else {
			respStatusCode = destinationJob.StatusCode
			respBody = destinationJob.Error
			errorAt = routerutils.ERROR_AT_TF
		}

		prevRespStatusCode = respStatusCode

		if !isJobTerminated(respStatusCode) {
			for _, metadata := range destinationJob.JobMetadataArray {
				failedJobOrderKeys[jobOrderKey(metadata.UserID, metadata.DestinationID)] = struct{}{}
			}
		}

		// assigning the destinationJob to a local variable (_destinationJob), so that
		// elements in routerJobResponses have pointer to the right job.
		_destinationJob := destinationJob

		// TODO: remove this once we enforce the necessary validations in the transformer's response
		dedupedJobMetadata := lo.UniqBy(_destinationJob.JobMetadataArray, func(jobMetadata types.JobMetadataT) int64 {
			return jobMetadata.JobID
		})
		for _, destinationJobMetadata := range dedupedJobMetadata {
			_destinationJobMetadata := destinationJobMetadata
			// assigning the destinationJobMetadata to a local variable (_destinationJobMetadata), so that
			// elements in routerJobResponses have pointer to the right destinationJobMetadata.

			routerJobResponses = append(routerJobResponses, &JobResponse{
				jobID:                  destinationJobMetadata.JobID,
				destinationJob:         &_destinationJob,
				destinationJobMetadata: &_destinationJobMetadata,
				respStatusCode:         respStatusCode,
				respBody:               respBody,
				errorAt:                errorAt,
			})
		}
	}

	sort.Slice(routerJobResponses, func(i, j int) bool {
		return routerJobResponses[i].jobID < routerJobResponses[j].jobID
	})

	// Struct to hold unique users in the batch (worker.destinationJobs)
	jobOrderKeyToJobIDMap := make(map[string]int64)

	for _, routerJobResponse := range routerJobResponses {
		destinationJobMetadata := routerJobResponse.destinationJobMetadata
		destinationJob := routerJobResponse.destinationJob
		attemptNum := destinationJobMetadata.AttemptNum
		respStatusCode = routerJobResponse.respStatusCode
		status := jobsdb.JobStatusT{
			JobID:         destinationJobMetadata.JobID,
			AttemptNum:    attemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			Parameters:    routerutils.EmptyPayload,
			JobParameters: destinationJobMetadata.JobT.Parameters,
			WorkspaceId:   destinationJobMetadata.WorkspaceID,
		}

		routerJobResponse.status = &status

		if !isJobTerminated(respStatusCode) {
			orderKey := jobOrderKey(destinationJobMetadata.UserID, destinationJobMetadata.DestinationID)
			if prevFailedJobID, ok := jobOrderKeyToJobIDMap[orderKey]; ok {
				// This means more than two jobs of the same user are in the batch & the batch job is failed
				// Only one job is marked failed and the rest are marked waiting
				// Job order logic requires that at any point of time, we should have only one failed job per user
				// This is introduced to ensure the above statement
				resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", prevFailedJobID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", destinationJobMetadata.UserID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "moreinfo", "attempted to send in a batch")

				status.JobState = jobsdb.Waiting.State
				status.ErrorResponse = resp
				w.rt.responseQ <- jobResponseT{status: &status, worker: w, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
				continue
			}
			jobOrderKeyToJobIDMap[orderKey] = destinationJobMetadata.JobID
		}

		status.AttemptNum++
		status.ErrorResponse = routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", routerJobResponse.respBody)
		status.ErrorCode = strconv.Itoa(respStatusCode)

		w.postStatusOnResponseQ(respStatusCode, destinationJob.Message, respContentType, destinationJobMetadata, &status, routerJobResponse.errorAt)

		w.sendEventDeliveryStat(destinationJobMetadata, &status, &destinationJob.Destination)

		w.sendRouterResponseCountStat(&status, &destinationJob.Destination, routerJobResponse.errorAt)
	}

	// NOTE: Sending live events to config backend after the status objects are built completely.
	destLiveEventSentMap := make(map[*types.DestinationJobT]struct{})
	for _, routerJobResponse := range routerJobResponses {
		// Sending only one destination live event for every destinationJob
		if _, ok := destLiveEventSentMap[routerJobResponse.destinationJob]; !ok {
			payload := routerJobResponse.destinationJob.Message
			if routerJobResponse.destinationJob.Message == nil {
				payload = routerJobResponse.destinationJobMetadata.JobT.EventPayload
			}
			sourcesIDs := make([]string, 0)
			for _, metadata := range routerJobResponse.destinationJob.JobMetadataArray {
				if !slices.Contains(sourcesIDs, metadata.SourceID) {
					sourcesIDs = append(sourcesIDs, metadata.SourceID)
				}
			}
			w.sendDestinationResponseToConfigBackend(payload, routerJobResponse.destinationJobMetadata, routerJobResponse.status, sourcesIDs)
			destLiveEventSentMap[routerJobResponse.destinationJob] = struct{}{}
		}
	}

	// routerJobs/destinationJobs are processed. Clearing the queues.
	w.routerJobs = nil
	w.destinationJobs = nil
}

func (w *worker) canSendJobToDestination(prevRespStatusCode int, failedJobOrderKeys map[string]struct{}, destinationJob *types.DestinationJobT) bool {
	if prevRespStatusCode == 0 {
		return true
	}

	if !w.rt.guaranteeUserEventOrder {
		// if guaranteeUserEventOrder is false, letting the next jobs pass
		return true
	}

	// If batching is enabled, we send the request only if the previous one succeeds
	if w.rt.enableBatching {
		return isSuccessStatus(prevRespStatusCode)
	}

	// If the destinationJob has come through router transform,
	// drop the request if it is of a failed user, else send
	for i := range destinationJob.JobMetadataArray {
		if _, ok := failedJobOrderKeys[jobOrderKey(destinationJob.JobMetadataArray[i].UserID, destinationJob.JobMetadataArray[i].DestinationID)]; ok {
			return false
		}
	}

	return true
}

func (w *worker) updateReqMetrics(respStatusCode int, diagnosisStartTime *time.Time) {
	var reqMetric requestMetric

	if isSuccessStatus(respStatusCode) {
		reqMetric.RequestSuccess++
	} else {
		reqMetric.RequestRetries++
	}
	reqMetric.RequestCompletedTime = time.Since(*diagnosisStartTime)
	w.rt.trackRequestMetrics(reqMetric)
}

func (w *worker) allowRouterAbortedAlert(errorAt string) bool {
	switch errorAt {
	case routerutils.ERROR_AT_CUST:
		return true
	case routerutils.ERROR_AT_TF:
		return !w.rt.skipRtAbortAlertForTransformation
	case routerutils.ERROR_AT_DEL:
		return !w.rt.transformerProxy && !w.rt.skipRtAbortAlertForDelivery
	default:
		return true
	}
}

func (w *worker) updateAbortedMetrics(destinationID, workspaceId, statusCode, errorAt string) {
	alert := w.allowRouterAbortedAlert(errorAt)
	eventsAbortedStat := stats.Default.NewTaggedStat(`router_aborted_events`, stats.CountType, stats.Tags{
		"destType":       w.rt.destName,
		"respStatusCode": statusCode,
		"destId":         destinationID,
		"workspaceId":    workspaceId,

		// To indicate if the failure should be alerted for router-aborted-count
		"alert": strconv.FormatBool(alert),
		// To specify at which point failure happened
		"errorAt": errorAt,
	})
	eventsAbortedStat.Increment()
}

func (w *worker) postStatusOnResponseQ(respStatusCode int, payload json.RawMessage,
	respContentType string, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT,
	errorAt string,
) {
	// Enhancing status.ErrorResponse with firstAttemptedAt
	firstAttemptedAtTime := time.Now()
	if destinationJobMetadata.FirstAttemptedAt != "" {
		t, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.FirstAttemptedAt)
		if err == nil {
			firstAttemptedAtTime = t
		}
	}

	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "firstAttemptedAt", firstAttemptedAtTime.Format(misc.RFC3339Milli))
	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "content-type", respContentType)

	if isSuccessStatus(respStatusCode) {
		status.JobState = jobsdb.Succeeded.State
		w.rt.logger.Debugf("[%v Router] :: sending success status to response", w.rt.destName)
		w.rt.responseQ <- jobResponseT{status: status, worker: w, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
	} else {
		// Saving payload to DB only
		// 1. if job failed and
		// 2. if router job undergoes batching or dest transform.
		if payload != nil && (w.rt.enableBatching || destinationJobMetadata.TransformAt == "router") {
			if w.rt.savePayloadOnError {
				status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "payload", string(payload))
			}
		}
		// the job failed
		w.rt.logger.Debugf("[%v Router] :: Job failed to send, analyzing...", w.rt.destName)

		if isJobTerminated(respStatusCode) {
			status.JobState = jobsdb.Aborted.State
			w.updateAbortedMetrics(destinationJobMetadata.DestinationID, status.WorkspaceId, status.ErrorCode, errorAt)
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "stage", "router")
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "reason", status.ErrorResponse) // NOTE: Old key used was "error_response"
		} else {
			status.JobState = jobsdb.Failed.State
			if !w.retryLimitReached(status) { // don't delay retry time if retry limit is reached, so that the job can be aborted immediately on the next loop
				status.RetryTime = status.ExecTime.Add(durationBeforeNextAttempt(status.AttemptNum))
			}
		}

		if w.rt.guaranteeUserEventOrder {
			if status.JobState == jobsdb.Failed.State {

				orderKey := jobOrderKey(destinationJobMetadata.UserID, destinationJobMetadata.DestinationID)
				w.rt.logger.Debugf("EventOrder: [%d] job %d for key %s failed", w.id, status.JobID, orderKey)
				if err := w.barrier.StateChanged(orderKey, destinationJobMetadata.JobID, status.JobState); err != nil {
					panic(err)
				}
			}
		}
		w.rt.logger.Debugf("[%v Router] :: sending failed/aborted state as response", w.rt.destName)
		w.rt.responseQ <- jobResponseT{status: status, worker: w, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
	}
}

func (w *worker) sendRouterResponseCountStat(status *jobsdb.JobStatusT, destination *backendconfig.DestinationT, errorAt string) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	var alert bool
	alert = w.allowRouterAbortedAlert(errorAt)
	if status.JobState == jobsdb.Succeeded.State {
		alert = !w.rt.skipRtAbortAlertForTransformation || !w.rt.skipRtAbortAlertForDelivery
		errorAt = ""
	}
	routerResponseStat := stats.Default.NewTaggedStat("router_response_counts", stats.CountType, stats.Tags{
		"destType":       w.rt.destName,
		"respStatusCode": status.ErrorCode,
		"destination":    destinationTag,
		"destId":         destination.ID,
		"attempt_number": strconv.Itoa(status.AttemptNum),
		"workspaceId":    status.WorkspaceId,
		// To indicate if the failure should be alerted for router-aborted-count
		"alert": strconv.FormatBool(alert),
		// To specify at which point failure happened
		"errorAt": errorAt,
	})
	routerResponseStat.Count(1)
}

func (w *worker) sendEventDeliveryStat(destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, destination *backendconfig.DestinationT) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	if status.JobState == jobsdb.Succeeded.State {
		eventsDeliveredStat := stats.Default.NewTaggedStat("event_delivery", stats.CountType, stats.Tags{
			"module":         "router",
			"destType":       w.rt.destName,
			"destID":         destination.ID,
			"destination":    destinationTag,
			"attempt_number": strconv.Itoa(status.AttemptNum),
			"workspaceId":    status.WorkspaceId,
			"source":         destinationJobMetadata.SourceID,
		})
		eventsDeliveredStat.Count(1)
		if destinationJobMetadata.ReceivedAt != "" {
			receivedTime, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.ReceivedAt)
			if err == nil {
				eventsDeliveryTimeStat := stats.Default.NewTaggedStat(
					"event_delivery_time", stats.TimerType, map[string]string{
						"module":         "router",
						"destType":       w.rt.destName,
						"destID":         destination.ID,
						"destination":    destinationTag,
						"attempt_number": strconv.Itoa(status.AttemptNum),
						"workspaceId":    status.WorkspaceId,
					})

				eventsDeliveryTimeStat.SendTiming(time.Since(receivedTime))
			}
		}
	}
}

func (w *worker) sendDestinationResponseToConfigBackend(payload json.RawMessage, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, sourceIDs []string) {
	// Sending destination response to config backend
	if status.ErrorCode != fmt.Sprint(types.RouterUnMarshalErrorCode) && status.ErrorCode != fmt.Sprint(types.RouterTimedOutStatusCode) {
		deliveryStatus := destinationdebugger.DeliveryStatusT{
			DestinationID: destinationJobMetadata.DestinationID,
			SourceID:      strings.Join(sourceIDs, ","),
			Payload:       payload,
			AttemptNum:    status.AttemptNum,
			JobState:      status.JobState,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
			SentAt:        status.ExecTime.Format(misc.RFC3339Milli),
			EventName:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_name").String(),
			EventType:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_type").String(),
		}
		w.rt.debugger.RecordEventDeliveryStatus(destinationJobMetadata.DestinationID, &deliveryStatus)
	}
}

func (w *worker) retryLimitReached(status *jobsdb.JobStatusT) bool {
	firstAttemptedAtTime := time.Now()
	if firstAttemptedAt := gjson.GetBytes(status.ErrorResponse, "firstAttemptedAt").Str; firstAttemptedAt != "" {
		if t, err := time.Parse(misc.RFC3339Milli, firstAttemptedAt); err == nil {
			firstAttemptedAtTime = t
		}
	}
	respStatusCode, _ := strconv.Atoi(status.ErrorCode)
	return (respStatusCode >= 500 && respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode) && // 5xx errors
		(time.Since(firstAttemptedAtTime) > w.rt.retryTimeWindow && status.AttemptNum >= w.rt.maxFailedCountForJob) // retry time window exceeded
}
