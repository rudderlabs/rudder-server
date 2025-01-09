package router

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// cronTracker Track the status of the staging file whether it has reached the terminal state or not for every warehouse
// we pick the staging file which is oldest within the range NOW() - 2 * syncFrequency and NOW() - 3 * syncFrequency
// and checks if the corresponding upload has reached the terminal state or not.
// If the upload has not reached the terminal state, then we send a gauge metric with value 1 else 0
func (r *Router) cronTracker(ctx context.Context) error {
	for {
		execTime := r.now()
		r.stats.cronTrackerExecTimestamp.Gauge(execTime.Unix())

		for _, warehouse := range r.copyWarehouses() {
			if err := r.retryTrackSync(ctx, &warehouse); err != nil {
				if ctx.Err() != nil {
					return nil //nolint:nilerr
				}
				return fmt.Errorf("cron tracker: %w", err)
			}
		}

		nextExecTime := execTime.Add(r.config.uploadStatusTrackFrequency)
		select {
		case <-ctx.Done():
			r.logger.Infon("Context cancelled. Exiting cron tracker")
			return nil
		case <-time.After(time.Until(nextExecTime)):
		}
	}
}

func (r *Router) retryTrackSync(ctx context.Context, warehouse *model.Warehouse) error {
	o := func() error {
		return r.trackSync(ctx, warehouse)
	}
	b := backoff.WithContext(
		backoff.WithMaxRetries(
			backoff.NewExponentialBackOff(),
			uint64(r.config.cronTrackerRetries.Load()),
		),
		ctx,
	)
	return backoff.Retry(o, b)
}

func (r *Router) trackSync(ctx context.Context, warehouse *model.Warehouse) error {
	if !warehouse.IsEnabled() || r.isWithinExcludeWindow(warehouse) {
		return nil
	}

	createdAt, err := r.getOldestStagingFile(ctx, warehouse)
	if err != nil {
		return err
	}
	if createdAt.IsZero() {
		return nil
	}

	exists, err := r.checkUploadStatus(ctx, warehouse, createdAt)
	if err != nil {
		return err
	}

	r.recordUploadMissingMetric(warehouse, exists)
	return nil
}

func (r *Router) isWithinExcludeWindow(warehouse *model.Warehouse) bool {
	excludeWindow := warehouse.GetMapDestinationConfig(model.ExcludeWindowSetting)
	startTime, endTime := excludeWindowStartEndTimes(excludeWindow)
	return checkCurrentTimeExistsInExcludeWindow(r.now(), startTime, endTime)
}

func (r *Router) getOldestStagingFile(ctx context.Context, warehouse *model.Warehouse) (time.Time, error) {
	nowSQL := r.getNowSQL()
	timeWindow := r.calculateTimeWindow(warehouse)

	query := fmt.Sprintf(`
		SELECT created_at
		FROM `+whutils.WarehouseStagingFilesTable+`
		WHERE source_id = $1
		  AND destination_id = $2
		  AND created_at > %[1]s - $3 * INTERVAL '1 MIN'
		  AND created_at < %[1]s - $4 * INTERVAL '1 MIN'
		ORDER BY id DESC
		LIMIT 1;`,
		nowSQL,
	)
	queryArgs := []any{
		warehouse.Source.ID,
		warehouse.Destination.ID,
		2 * timeWindow / time.Minute,
		timeWindow / time.Minute,
	}

	var createdAt sql.NullTime
	err := r.db.QueryRowContext(ctx, query, queryArgs...).Scan(&createdAt)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("fetching oldest staging file for source %s and destination %s: %w",
			warehouse.Source.ID, warehouse.Destination.ID, err)
	}
	if !createdAt.Valid {
		return time.Time{}, fmt.Errorf("invalid created_at time for source %s and destination %s",
			warehouse.Source.ID, warehouse.Destination.ID)
	}
	return createdAt.Time, nil
}

func (r *Router) calculateTimeWindow(warehouse *model.Warehouse) time.Duration {
	timeWindow := r.config.uploadBufferTimeInMin.Load()
	syncFrequency := warehouse.GetStringDestinationConfig(r.conf, model.SyncFrequencySetting)
	if syncFrequency != "" {
		if value, err := strconv.Atoi(syncFrequency); err == nil {
			timeWindow += time.Duration(value) * time.Minute
		}
	}
	return timeWindow
}

func (r *Router) checkUploadStatus(ctx context.Context, warehouse *model.Warehouse, createdAt time.Time) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM ` + whutils.WarehouseUploadsTable + `
			WHERE source_id = $1 AND destination_id = $2 AND
				  (status = $3 OR status = $4 OR status LIKE $5) AND
				  updated_at > $6
		);`
	queryArgs := []any{
		warehouse.Source.ID,
		warehouse.Destination.ID,
		model.ExportedData,
		model.Aborted,
		"%_failed",
		createdAt.Format(misc.RFC3339Milli),
	}

	var exists bool
	err := r.db.QueryRowContext(ctx, query, queryArgs...).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("checking upload status for source %s and destination %s: %w",
			warehouse.Source.ID, warehouse.Destination.ID, err)
	}
	return exists, nil
}

func (r *Router) recordUploadMissingMetric(warehouse *model.Warehouse, exists bool) {
	metric := r.statsFactory.NewTaggedStat("warehouse_track_upload_missing", stats.GaugeType, stats.Tags{
		"module":        moduleName,
		"workspaceId":   warehouse.WorkspaceID,
		"destType":      r.destType,
		"sourceId":      warehouse.Source.ID,
		"destinationId": warehouse.Destination.ID,
	})
	if exists {
		metric.Gauge(0)
	} else {
		metric.Gauge(1)
	}
}
