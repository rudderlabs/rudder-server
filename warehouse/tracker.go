package warehouse

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (wh *HandleT) CronTracker(ctx context.Context) error {
	for {
		var warehouses []model.Warehouse

		wh.configSubscriberLock.RLock()
		warehouses = append(warehouses, wh.warehouses...)
		wh.configSubscriberLock.RUnlock()

		for _, warehouse := range warehouses {
			if err := wh.Track(ctx, &warehouse, config.Default); err != nil {
				return fmt.Errorf(
					"cron tracker failed for source: %s, destination: %s with error: %w",
					warehouse.Source.ID,
					warehouse.Destination.ID,
					err,
				)
			}
		}

		select {
		case <-ctx.Done():
			wh.Logger.Infof("context is cancelled, stopped running tracking")
			return nil
		case <-time.After(uploadStatusTrackFrequency):
		}
	}
}

// Track tracks the status of the warehouse uploads for the corresponding cases:
// 1. Staging files is not picked.
// 2. Upload job is struck
func (wh *HandleT) Track(ctx context.Context, warehouse *model.Warehouse, config *config.Config) error {
	var (
		query             string
		queryArgs         []interface{}
		createdAt         sql.NullTime
		exists            bool
		syncFrequency     = "1440"
		Now               = timeutil.Now
		NowSQL            = "NOW()"
		failedStatusRegex = "%_failed"
		timeWindow        = config.GetDuration("Warehouse.uploadBufferTimeInMin", 180, time.Minute)
		source            = warehouse.Source
		destination       = warehouse.Destination
	)

	if wh.NowSQL != "" {
		NowSQL = wh.NowSQL
	}
	if wh.Now != nil {
		Now = wh.Now
	}

	tags := stats.Tags{
		"workspaceId": warehouse.WorkspaceID,
		"module":      moduleName,
		"destType":    wh.destType,
		"warehouseID": misc.GetTagName(
			destination.ID,
			source.Name,
			destination.Name,
			misc.TailTruncateStr(source.ID, 6)),
	}
	statKey := "warehouse_track_upload_missing"
	wh.stats.NewTaggedStat(statKey, stats.GaugeType, tags).Gauge(0)

	if !source.Enabled || !destination.Enabled {
		return nil
	}

	excludeWindow := warehouseutils.GetConfigValueAsMap(warehouseutils.ExcludeWindow, warehouse.Destination.Config)
	excludeWindowStartTime, excludeWindowEndTime := GetExcludeWindowStartEndTimes(excludeWindow)
	if CheckCurrentTimeExistsInExcludeWindow(Now(), excludeWindowStartTime, excludeWindowEndTime) {
		return nil
	}

	if sf := warehouseutils.GetConfigValue(warehouseutils.SyncFrequency, *warehouse); sf != "" {
		syncFrequency = sf
	}
	if value, err := strconv.Atoi(syncFrequency); err == nil {
		timeWindow += time.Duration(value) * time.Minute
	}

	query = fmt.Sprintf(`
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
				  created_at DESC
				LIMIT
				  1;
				`,
		warehouseutils.WarehouseStagingFilesTable,
		NowSQL,
	)
	queryArgs = []interface{}{
		source.ID,
		destination.ID,
		2 * timeWindow / time.Minute,
		timeWindow / time.Minute,
	}

	err := wh.dbHandle.QueryRowContext(ctx, query, queryArgs...).Scan(&createdAt)
	if err == sql.ErrNoRows {
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

	err = wh.dbHandle.QueryRowContext(ctx, query, queryArgs...).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("fetching last upload status for source: %s and destination: %s: %w", source.ID, destination.ID, err)
	}

	if !exists {
		wh.Logger.Warnw("pending staging files not picked",
			logfield.SourceID, source.ID,
			logfield.SourceType, source.SourceDefinition.Name,
			logfield.DestinationID, destination.ID,
			logfield.DestinationType, destination.DestinationDefinition.Name,
			logfield.WorkspaceID, warehouse.WorkspaceID,
		)
		wh.stats.NewTaggedStat(statKey, stats.GaugeType, tags).Gauge(1)
	}

	return nil
}
