package warehouse

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (wh *HandleT) CronTracker(ctx context.Context) error {
	for {
		wh.configSubscriberLock.RLock()

		ch := make(chan warehouseutils.Warehouse, 1)
		wg := sync.WaitGroup{}
		wg.Add(len(wh.warehouses))

		for _, warehouse := range wh.warehouses {
			warehouse := warehouse

			rruntime.GoForWarehouse(func() {
				defer wg.Done()
				ch <- warehouse
			})
		}
		wh.configSubscriberLock.RUnlock()

		rruntime.Go(func() {
			wg.Wait()
			close(ch)
		})

		for w := range ch {
			if err := wh.Track(ctx, &w, config.Default); err != nil {
				return fmt.Errorf("cron tracker failed for source: %s, destination: %s with error: %w", w.Source.ID, w.Destination.ID, err)
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

func (wh *HandleT) Track(ctx context.Context, warehouse *warehouseutils.Warehouse, config *config.Config) error {
	var (
		query             string
		queryArgs         []interface{}
		createdAt         sql.NullTime
		exists            bool
		uploaded          int
		syncFrequency     = "1440"
		Now               = timeutil.Now
		NowSQL            = "NOW()"
		failedStatusRegex = "%_failed"
		timeWindow        = config.GetInt("Warehouse.uploadBufferTimeInMin", 180)
		source            = warehouse.Source
		destination       = warehouse.Destination
	)

	if wh.NowSQL != "" {
		NowSQL = wh.NowSQL
	}
	if wh.Now != nil {
		Now = wh.Now
	}

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
		timeWindow += value
	}

	query = fmt.Sprintf(`
				SELECT
				  created_at
				FROM
				  %[1]s
				WHERE
				  source_id = '%[2]s' AND
				  destination_id = '%[3]s' AND
				  created_at > %[6]s - interval '%[4]d MIN' AND
				  created_at < %[6]s - interval '%[5]d MIN'
				ORDER BY
				  created_at DESC
				LIMIT
				  1;
				`,
		warehouseutils.WarehouseStagingFilesTable,
		source.ID,
		destination.ID,
		2*timeWindow,
		timeWindow,
		NowSQL,
	)

	err := wh.dbHandle.QueryRowContext(ctx, query).Scan(&createdAt)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
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

	if exists {
		uploaded = 1
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
	wh.stats.NewTaggedStat("warehouse_successful_upload_exists", stats.CountType, tags).Count(uploaded)
	return nil
}
