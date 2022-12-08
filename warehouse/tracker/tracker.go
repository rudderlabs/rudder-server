package tracker

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	uploadBufferTimeInMin      int
	uploadStatusTrackFrequency time.Duration
)

type Tracker struct {
	DB     *sql.DB
	Stats  stats.Stats
	Logger logger.Logger

	Warehouses []warehouseutils.Warehouse
}

func Init() {
	config.RegisterIntConfigVariable(180, &uploadBufferTimeInMin, false, 1, "Warehouse.uploadBufferTimeInMin")
	config.RegisterDurationConfigVariable(30, &uploadStatusTrackFrequency, false, time.Minute, []string{"Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin"}...)
}

func (t *Tracker) Start(ctx context.Context) {
	for {
		for _, warehouse := range t.Warehouses {
			source := warehouse.Source
			destination := warehouse.Destination

			if !source.Enabled || !destination.Enabled {
				continue
			}

			config := destination.Config
			// Default frequency
			syncFrequency := "1440"
			if config[warehouseutils.SyncFrequency] != nil {
				syncFrequency, _ = config[warehouseutils.SyncFrequency].(string)
			}

			timeWindow := uploadBufferTimeInMin
			if value, err := strconv.Atoi(syncFrequency); err == nil {
				timeWindow += value
			}

			sqlStatement := fmt.Sprintf(`
				select
				  created_at
				from
				  %[1]s
				where
				  source_id = '%[2]s'
				  and destination_id = '%[3]s'
				  and created_at > now() - interval '%[4]d MIN'
				  and created_at < now() - interval '%[5]d MIN'
				order by
				  created_at desc
				limit
				  1;
`,

				warehouseutils.WarehouseStagingFilesTable,
				source.ID,
				destination.ID,
				2*timeWindow,
				timeWindow,
			)

			var createdAt sql.NullTime
			err := t.DB.QueryRow(sqlStatement).Scan(&createdAt)
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil && err != sql.ErrNoRows {
				panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
			}

			if !createdAt.Valid {
				continue
			}

			sqlStatement = fmt.Sprintf(`
				SELECT
				  EXISTS (
					SELECT
					  1
					FROM
					  %s
					WHERE
					  source_id = $1
					  AND destination_id = $2
					  AND (
						status = $3
						OR status = $4
						OR status LIKE $5
					  )
					  AND updated_at > $6
				  );
`,
				warehouseutils.WarehouseUploadsTable,
			)
			sqlStatementArgs := []interface{}{
				source.ID,
				destination.ID,
				model.ExportedData,
				model.Aborted,
				"%_failed",
				createdAt.Time.Format(misc.RFC3339Milli),
			}
			var (
				exists   bool
				uploaded int
			)
			err = t.DB.QueryRow(sqlStatement, sqlStatementArgs...).Scan(&exists)
			if err != nil && err != sql.ErrNoRows {
				panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
			}
			if exists {
				uploaded = 1
			}

			tags := stats.Tags{
				"workspaceId": warehouse.WorkspaceID,
				"module":      "warehouse",
				"destType":    warehouse.Type,
				"warehouseID": misc.GetTagName(
					warehouse.Destination.ID,
					warehouse.Source.Name,
					warehouse.Destination.Name,
					misc.TailTruncateStr(warehouse.Source.ID, 6)),
			}
			t.Stats.NewTaggedStat("warehouse_successful_upload_exists", stats.CountType, tags).Count(uploaded)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(uploadStatusTrackFrequency):
		}
	}
}
