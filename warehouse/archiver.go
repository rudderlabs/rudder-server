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
	loadConfigArchiver()
}

func loadConfigArchiver() {
	config.RegisterBoolConfigVariable("Warehouse.archiveLoadFiles", true, &archiveLoadFiles, true)
	config.RegisterBoolConfigVariable("Warehouse.archiveStagingFiles", true, &archiveStagingFiles, true)
	config.RegisterIntConfigVariable("Warehouse.stagingFilesArchivalTimeInDays", 45, &stagingFilesArchivalTimeInDays, true, 1)
	config.RegisterIntConfigVariable("Warehouse.loadFilesArchivalTimeInDays", 15, &loadFilesArchivalTimeInDays, true, 1)
	config.RegisterDurationConfigVariable("Warehouse.archiverTickerTimeInMin", time.Duration(1440), &archiverTickerTime, true, time.Minute)
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
