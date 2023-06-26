package batchrouter

import (
	"context"
	"fmt"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse"
)

func (brt *Handle) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableBatchRouterMetric {
		return
	}

	for {
		brt.batchRequestsMetricMu.RLock()
		var diagnosisProperties map[string]interface{}
		success := 0
		failed := 0
		for _, batchReqMetric := range brt.batchRequestsMetric {
			success = success + batchReqMetric.batchRequestSuccess
			failed = failed + batchReqMetric.batchRequestFailed
		}
		if len(brt.batchRequestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				brt.destType: map[string]interface{}{
					diagnostics.BatchRouterSuccess: success,
					diagnostics.BatchRouterFailed:  failed,
				},
			}

			brt.Diagnostics.Track(diagnostics.BatchRouterEvents, diagnosisProperties)
		}

		brt.batchRequestsMetric = nil
		brt.batchRequestsMetricMu.RUnlock()

		select {
		case <-ctx.Done():
			return
		case <-brt.diagnosisTicker.C:
		}
	}
}

func sendDestStatusStats(batchDestination *Connection, jobStateCounts map[string]int, destType string, isWarehouse bool) {
	tags := map[string]string{
		"module":        "batch_router",
		"destType":      destType,
		"isWarehouse":   fmt.Sprintf("%t", isWarehouse),
		"destinationId": misc.GetTagName(batchDestination.Destination.ID, batchDestination.Destination.Name),
		"sourceId":      misc.GetTagName(batchDestination.Source.ID, batchDestination.Source.Name),
	}

	for jobState, count := range jobStateCounts {
		tags["job_state"] = jobState
		stats.Default.NewTaggedStat("event_status", stats.CountType, tags).Count(count)
	}
}

func (brt *Handle) recordDeliveryStatus(batchDestination Connection, output UploadResult, isWarehouse bool) {
	var (
		errorCode string
		jobState  string
		errorResp []byte
	)

	err := output.Error
	if err != nil {
		jobState = jobsdb.Failed.State
		errorCode = "500"
		if isWarehouse {
			jobState = warehouse.GeneratingStagingFileFailedState
		}
		errorResp, _ = json.Marshal(ErrorResponse{Error: err.Error()})
	} else {
		jobState = jobsdb.Succeeded.State
		errorCode = "200"
		if isWarehouse {
			jobState = warehouse.GeneratedStagingFileState
		}
		errorResp = []byte(`{"success":"OK"}`)
	}

	// Payload and AttemptNum don't make sense in recording batch router delivery status,
	// So they are set to default values.
	payload, err := sjson.SetBytes([]byte(`{}`), "location", output.FileLocation)
	if err != nil {
		payload = []byte(`{}`)
	}
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		EventName:     fmt.Sprint(output.TotalEvents) + " events",
		EventType:     "",
		SentAt:        time.Now().Format(misc.RFC3339Milli),
		DestinationID: batchDestination.Destination.ID,
		SourceID:      batchDestination.Source.ID,
		Payload:       payload,
		AttemptNum:    1,
		JobState:      jobState,
		ErrorCode:     errorCode,
		ErrorResponse: errorResp,
	}
	brt.debugger.RecordEventDeliveryStatus(batchDestination.Destination.ID, &deliveryStatus)
}

func (brt *Handle) trackRequestMetrics(batchReqDiagnostics batchRequestMetric) {
	if diagnostics.EnableBatchRouterMetric {
		brt.batchRequestsMetricMu.Lock()
		brt.batchRequestsMetric = append(brt.batchRequestsMetric, batchReqDiagnostics)
		brt.batchRequestsMetricMu.Unlock()
	}
}

func (brt *Handle) recordUploadStats(destination Connection, output UploadResult) {
	destinationTag := misc.GetTagName(destination.Destination.ID, destination.Destination.Name)
	eventDeliveryStat := stats.Default.NewTaggedStat("event_delivery", stats.CountType, map[string]string{
		"module":      "batch_router",
		"destType":    brt.destType,
		"destination": destinationTag,
		"workspaceId": destination.Source.WorkspaceID,
		"source":      destination.Source.ID,
	})
	eventDeliveryStat.Count(output.TotalEvents)

	receivedTime, err := time.Parse(misc.RFC3339Milli, output.FirstEventAt)
	if err == nil {
		eventDeliveryTimeStat := stats.Default.NewTaggedStat("event_delivery_time", stats.TimerType, map[string]string{
			"module":      "batch_router",
			"destType":    brt.destType,
			"destination": destinationTag,
			"workspaceId": destination.Source.WorkspaceID,
		})
		eventDeliveryTimeStat.SendTiming(time.Since(receivedTime))
	}
}

func (brt *Handle) sendRetryStoreStats(attempt int) {
	brt.logger.Warnf("Timeout during store jobs in batch router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "batch_router"}).Count(1)
}

func (brt *Handle) sendRetryUpdateStats(attempt int) {
	brt.logger.Warnf("Timeout during update job status in batch router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "batch_router"}).Count(1)
}

func (brt *Handle) sendQueryRetryStats(attempt int) {
	brt.logger.Warnf("Timeout during query jobs in batch router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "batch_router"}).Count(1)
}

func (brt *Handle) updateRudderSourcesStats(ctx context.Context, tx jobsdb.UpdateSafeTx, jobs []*jobsdb.JobT, jobStatuses []*jobsdb.JobStatusT) error {
	rsourcesStats := rsources.NewStatsCollector(brt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.JobStatusesUpdated(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	if err != nil {
		return fmt.Errorf("publishing rsources stats: %w", err)
	}
	return nil
}

func (brt *Handle) updateProcessedEventsMetrics(statusList []*jobsdb.JobStatusT) {
	eventsPerStateAndCode := map[string]map[string]int{}
	for i := range statusList {
		state := statusList[i].JobState
		code := statusList[i].ErrorCode
		if _, ok := eventsPerStateAndCode[state]; !ok {
			eventsPerStateAndCode[state] = map[string]int{}
		}
		eventsPerStateAndCode[state][code]++
	}
	for state, codes := range eventsPerStateAndCode {
		for code, count := range codes {
			stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
				"module":   "batch_router",
				"destType": brt.destType,
				"state":    state,
				"code":     code,
			}).Count(count)
		}
	}
}

// pipelineDelayStats reports the delay of the pipeline as a range:
//
// - max - time elapsed since the first job was created
//
// - min - time elapsed since the last job was created
func (brt *Handle) pipelineDelayStats(partition string, first, last *jobsdb.JobT) {
	var firstJobDelay float64
	var lastJobDelay float64
	if first != nil {
		firstJobDelay = time.Since(first.CreatedAt).Seconds()
	}
	if last != nil {
		lastJobDelay = time.Since(last.CreatedAt).Seconds()
	}
	stats.Default.NewTaggedStat("pipeline_delay_min_seconds", stats.GaugeType, stats.Tags{"destType": brt.destType, "partition": partition, "module": "batch_router"}).Gauge(lastJobDelay)
	stats.Default.NewTaggedStat("pipeline_delay_max_seconds", stats.GaugeType, stats.Tags{"destType": brt.destType, "partition": partition, "module": "batch_router"}).Gauge(firstJobDelay)
}
