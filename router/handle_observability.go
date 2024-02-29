package router

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/sqlutil"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

func (rt *Handle) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.telemetry.requestsMetricLock.Lock()
		rt.telemetry.requestsMetric = append(rt.telemetry.requestsMetric, reqMetric)
		rt.telemetry.requestsMetricLock.Unlock()
	}
}

func (rt *Handle) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableRouterMetric {
		return
	}

	for {
		select {
		case <-ctx.Done():
			rt.logger.Debugf("[%v Router] :: collectMetrics exiting", rt.destType)
			return
		case <-rt.telemetry.diagnosisTicker.C:
		}
		rt.telemetry.requestsMetricLock.RLock()
		var diagnosisProperties map[string]interface{}
		retries := 0
		aborted := 0
		success := 0
		var compTime time.Duration
		for _, reqMetric := range rt.telemetry.requestsMetric {
			retries += reqMetric.RequestRetries
			aborted += reqMetric.RequestAborted
			success += reqMetric.RequestSuccess
			compTime += reqMetric.RequestCompletedTime
		}
		if len(rt.telemetry.requestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				rt.destType: map[string]interface{}{
					diagnostics.RouterAborted:       aborted,
					diagnostics.RouterRetries:       retries,
					diagnostics.RouterSuccess:       success,
					diagnostics.RouterCompletedTime: (compTime / time.Duration(len(rt.telemetry.requestsMetric))) / time.Millisecond,
				},
			}
			if diagnostics.Diagnostics != nil {
				diagnostics.Diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
			}
		}

		rt.telemetry.requestsMetric = nil
		rt.telemetry.requestsMetricLock.RUnlock()

		// This lock will ensure we don't send out Track Request while filling up the
		// failureMetric struct
		rt.telemetry.failureMetricLock.Lock()
		for key, value := range rt.telemetry.failuresMetric {
			var err error
			stringValueBytes, err := jsonfast.Marshal(value)
			if err != nil {
				stringValueBytes = []byte{}
			}
			if diagnostics.Diagnostics != nil {
				diagnostics.Diagnostics.Track(key, map[string]interface{}{
					diagnostics.RouterDestination: rt.destType,
					diagnostics.Count:             len(value),
					diagnostics.ErrorCountMap:     string(stringValueBytes),
				})
			}
		}
		rt.telemetry.failuresMetric = make(map[string]map[string]int)
		rt.telemetry.failureMetricLock.Unlock()
	}
}

func (rt *Handle) updateRudderSourcesStats(
	ctx context.Context,
	tx jobsdb.UpdateSafeTx,
	jobs []*jobsdb.JobT,
	jobStatuses []*jobsdb.JobStatusT,
) error {
	rsourcesStats := rsources.NewStatsCollector(rt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.CollectStats(jobStatuses)
	rsourcesStats.CollectFailedRecords(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	if err != nil {
		rt.logger.Errorf("publishing rsources stats: %w", err)
	}
	return err
}

func (rt *Handle) sendRetryStoreStats(attempt int) {
	rt.logger.Warnf("Timeout during store jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func (rt *Handle) sendRetryUpdateStats(attempt int) {
	rt.logger.Warnf("Timeout during update job status in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func (rt *Handle) sendQueryRetryStats(attempt int) {
	rt.logger.Warnf("Timeout during query jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

// pipelineDelayStats reports the delay of the pipeline as a range:
//
// - max - time elapsed since the first job was created
//
// - min - time elapsed since the last job was created
func (rt *Handle) pipelineDelayStats(partition string, first, last *jobsdb.JobT) {
	var firstJobDelay float64
	var lastJobDelay float64
	if first != nil {
		firstJobDelay = time.Since(first.CreatedAt).Seconds()
	}
	if last != nil {
		lastJobDelay = time.Since(last.CreatedAt).Seconds()
	}
	stats.Default.NewTaggedStat("pipeline_delay_min_seconds", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition, "module": "router"}).Gauge(lastJobDelay)
	stats.Default.NewTaggedStat("pipeline_delay_max_seconds", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition, "module": "router"}).Gauge(firstJobDelay)
}

// eventOrderDebugInfo provides some debug information for the given orderKey in case of a panic.
// Top 100 job statuses for the given orderKey are returned.
func (rt *Handle) eventOrderDebugInfo(orderKey eventorder.BarrierKey) (res string) {
	defer func() {
		if r := recover(); r != nil {
			res = fmt.Sprintf("panic in EventOrderDebugInfo: %v", r)
		}
	}()
	userID, destinationID := orderKey.UserID, orderKey.DestinationID
	if err := rt.jobsDB.WithTx(func(tx *Tx) error {
		rows, err := tx.Query(`SELECT * FROM joborderlog($1, $2, 10) LIMIT 100`, destinationID, userID)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Err()
			_ = rows.Close()
		}()
		var out bytes.Buffer
		if err := sqlutil.PrintRowsToTable(rows, &out); err != nil {
			out.WriteString(fmt.Sprintf("error printing rows: %v", err))
		}
		res = out.String()
		return nil
	}); err != nil {
		res = fmt.Sprintf("error in EventOrderDebugInfo: %v", err)
	}
	return res
}
