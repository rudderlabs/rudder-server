package router

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// CronTracker Track the status of the staging file whether it has reached the terminal state or not for every warehouse
// we pick the staging file which is oldest within the range NOW() - 2 * syncFrequency and NOW() - 3 * syncFrequency
func (r *Router) CronTracker(ctx context.Context) error {
	cronTrackerExecTimestamp := r.statsFactory.NewTaggedStat("warehouse_cron_tracker_timestamp_seconds", stats.GaugeType, stats.Tags{
		"module":   moduleName,
		"destType": r.destType,
	})
	for {

		cronTrackerExecTimestamp.Gauge(time.Now().Unix())

		r.configSubscriberLock.RLock()
		warehouses := append([]model.Warehouse{}, r.warehouses...)
		r.configSubscriberLock.RUnlock()

		for _, warehouse := range warehouses {
			b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(r.config.cronTrackerRetries.Load())), ctx)
			err := backoff.RetryNotify(func() error {
				return r.Track(ctx, &warehouse, r.conf)
			}, b, func(err error, t time.Duration) {})
			if err != nil {
				r.logger.Errorn(
					"cron tracker failed for",
					obskit.SourceID(warehouse.Source.ID),
					obskit.DestinationID(warehouse.Destination.ID),
					obskit.Error(err),
				)
				break
			}
		}

		select {
		case <-ctx.Done():
			r.logger.Infon("context is cancelled, stopped running tracking")
			return nil
		case <-time.After(r.config.uploadStatusTrackFrequency):
		}
	}
}

// Track tracks the status of the warehouse uploads for the corresponding cases:
// 1. Staging files is not picked.
// 2. Upload job is struck
func (r *Router) Track(
	ctx context.Context,
	warehouse *model.Warehouse,
	config *config.Config,
) error {
	var (
		createdAt         sql.NullTime
		exists            bool
		syncFrequency     = "1440"
		now               = timeutil.Now
		nowSQL            = "NOW()"
		failedStatusRegex = "%_failed"
		timeWindow        = config.GetDuration("Warehouse.uploadBufferTimeInMin", 180, time.Minute)
		source            = warehouse.Source
		destination       = warehouse.Destination
	)

	if r.nowSQL != "" {
		nowSQL = r.nowSQL
	}
	if r.now != nil {
		now = r.now
	}

	trackUploadMissingStat := r.statsFactory.NewTaggedStat("warehouse_track_upload_missing", stats.GaugeType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"module":        moduleName,
		"destType":      r.destType,
		"sourceId":      source.ID,
		"destinationId": destination.ID,
		"warehouseID": misc.GetTagName(
			destination.ID,
			source.Name,
			destination.Name,
			misc.TailTruncateStr(source.ID, 6)),
	})
	trackUploadMissingStat.Gauge(0)

	if !source.Enabled || !destination.Enabled {
		return nil
	}

	excludeWindow := warehouse.GetMapDestinationConfig(model.ExcludeWindowSetting)
	excludeWindowStartTime, excludeWindowEndTime := excludeWindowStartEndTimes(excludeWindow)
	if checkCurrentTimeExistsInExcludeWindow(now(), excludeWindowStartTime, excludeWindowEndTime) {
		return nil
	}

	if sf := warehouse.GetStringDestinationConfig(r.conf, model.SyncFrequencySetting); sf != "" {
		syncFrequency = sf
	}
	if value, err := strconv.Atoi(syncFrequency); err == nil {
		timeWindow += time.Duration(value) * time.Minute
	}

	query := fmt.Sprintf(`
				SELECT
				  created_at
				FROM
				  %[1]s
				WHERE
				  source_id = $1 AND
				  destination_id = $2 AND
				  created_at > %[2]s - $3 * INTERVAL '1 MIN' AND
				  created_at < %[2]s - $4 * INTERVAL '1 MIN'
				ORDER BY
				  id DESC
				LIMIT
				  1;
				`,
		warehouseutils.WarehouseStagingFilesTable,
		nowSQL,
	)
	queryArgs := []interface{}{
		source.ID,
		destination.ID,
		2 * timeWindow / time.Minute,
		timeWindow / time.Minute,
	}

	err := r.db.QueryRowContext(ctx, query, queryArgs...).Scan(&createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("fetching last upload time for source: %s and destination: %s: %w", source.ID, destination.ID, err)
	}

	if !createdAt.Valid {
		return fmt.Errorf("invalid last upload time for source: %s and destination: %s", source.ID, destination.ID)
	}

	query = `
				SELECT
				  EXISTS (
					SELECT
					  1
					FROM
					  ` + warehouseutils.WarehouseUploadsTable + `
					WHERE
					  source_id = $1 AND
					  destination_id = $2 AND
					  (
						status = $3
						OR status = $4
						OR status LIKE $5
					  ) AND
					  updated_at > $6
				  );
	`
	queryArgs = []interface{}{
		source.ID,
		destination.ID,
		model.ExportedData,
		model.Aborted,
		failedStatusRegex,
		createdAt.Time.Format(misc.RFC3339Milli),
	}

	err = r.db.QueryRowContext(ctx, query, queryArgs...).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("fetching last upload status for source: %s and destination: %s: %w", source.ID, destination.ID, err)
	}

	if !exists {
		r.logger.Warnn("pending staging files not picked",
			obskit.SourceID(source.ID),
			obskit.SourceType(source.SourceDefinition.Name),
			obskit.DestinationID(destination.ID),
			obskit.DestinationType(destination.DestinationDefinition.Name),
			obskit.WorkspaceID(warehouse.WorkspaceID),
		)

		trackUploadMissingStat.Gauge(1)
	}

	return nil
}
