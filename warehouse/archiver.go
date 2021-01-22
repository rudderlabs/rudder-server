package warehouse

import (
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/archiver"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	archiveLoadFiles               bool
	archiveStagingFiles            bool
	stagingFilesArchivalTimeInDays int
	loadFilesArchivalTimeInDays    int
	archiverTickerTime             time.Duration
)

func init() {
	archiveLoadFiles = config.GetBool("Warehouse.archiveLoadFiles", true)
	archiveStagingFiles = config.GetBool("Warehouse.archiveStagingFiles", true)
	stagingFilesArchivalTimeInDays = config.GetInt("Warehouse.stagingFilesArchivalTimeInDays", 45)
	loadFilesArchivalTimeInDays = config.GetInt("Warehouse.loadFilesArchivalTimeInDays", 15)
	archiverTickerTime = config.GetDuration("Warehouse.archiverTickerTimeInMin", 1440) * time.Minute // default 1 day
}

func runArchiver(dbHandle *sql.DB) {
	for {
		if archiveLoadFiles {
			archiver.ArchiveOldRecords(warehouseutils.WarehouseLoadFilesTable, "created_at", loadFilesArchivalTimeInDays, dbHandle)
		}
		if archiveStagingFiles {
			archiver.ArchiveOldRecords(warehouseutils.WarehouseStagingFilesTable, "created_at", stagingFilesArchivalTimeInDays, dbHandle)
		}
		time.Sleep(archiverTickerTime)
	}
}
