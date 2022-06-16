package warehouse

import (
	"fmt"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func getFirstStagedEventAt(stagingFileID int64) (time.Time, error) {
	sqlStatement := fmt.Sprintf(`SELECT first_event_at FROM %[1]s where id = %[2]v`, warehouseutils.WarehouseStagingFilesTable, stagingFileID)

	var firstEventAt time.Time
	err := dbHandle.QueryRow(sqlStatement).Scan(&firstEventAt)
	return firstEventAt, err
}

func getTotalEventsStaged(startFileID int64, endFileID int64) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`select sum(total_events) from %[1]s where id >= %[2]v and id <= %[3]v`, warehouseutils.WarehouseStagingFilesTable, startFileID, endFileID)

	err = dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total, err
}
