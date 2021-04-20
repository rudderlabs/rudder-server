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
	config.RegisterBoolConfigVariable(true, &archiveLoadFiles, true, "Warehouse.archiveLoadFiles")
	config.RegisterBoolConfigVariable(true, &archiveStagingFiles, true, "Warehouse.archiveStagingFiles")
	config.RegisterIntConfigVariable(45, &stagingFilesArchivalTimeInDays, true, 1, "Warehouse.stagingFilesArchivalTimeInDays")
	config.RegisterIntConfigVariable(15, &loadFilesArchivalTimeInDays, true, 1, "Warehouse.loadFilesArchivalTimeInDays")
	config.RegisterDurationConfigVariable(time.Duration(1440), &archiverTickerTime, true, time.Minute, "Warehouse.archiverTickerTimeInMin")
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
