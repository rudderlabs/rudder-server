package warehouse

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/tablearchiver"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	archiveLoadFiles    bool
	archiveStagingFiles bool
	archivalTimeInDays  int
	backupRowsBatchSize int
	archiverTickerTime  time.Duration
)

func init() {
	archiveLoadFiles = config.GetBool("Warehouse.archiveLoadFiles", true)
	archiveStagingFiles = config.GetBool("Warehouse.archiveStagingFiles", true)
	archivalTimeInDays = config.GetInt("Warehouse.archivalTimeInDays", 45)
	backupRowsBatchSize = config.GetInt("Warehouse.backupRowsBatchSize", 100)
	archiverTickerTime = config.GetDuration("Warehouse.archiverTickerTimeInMin", 1440) * time.Minute // default 1 day
}

func isArchiverObjectStorageConfigured() bool {
	provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "")
	bucket := config.GetEnv("JOBS_BACKUP_BUCKET", "")
	return provider != "" && bucket != ""
}

func archiveOldRecords(tableName string, dbHandle *sql.DB) {
	stmt := fmt.Sprintf(`SELECT count(*), COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM %s WHERE created_at < NOW() -INTERVAL '%d DAY'`, tableName, archivalTimeInDays)

	var filesCount, minID, maxID int64
	err := dbHandle.QueryRow(stmt).Scan(&filesCount, &minID, &maxID)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error in fetching %s records count for archival: %v`, tableName, err)
		return
	}
	if filesCount == 0 {
		logger.Infof(`[WH Archiver]: No %s records found to archive`, tableName)
		return
	}

	// TODO: Check if any wh_upload depends on these files

	// TODO: Should we skip deletion if object storage provider not configured?
	if !isArchiverObjectStorageConfigured() {
		stmt = fmt.Sprintf(`DELETE FROM  %s WHERE id >= %d and id <= %d`, tableName, minID, maxID)
		_, err = dbHandle.Exec(stmt)
		if err != nil {
			logger.Errorf(`[WH Archiver]: Error in deleting %s records: %v`, tableName, err)
			return
		}
		logger.Infof(`[WH Archiver]: Deleted %s records %d to %d. No objet storage was configured for archival`, tableName, minID, maxID)
		return
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	backupPathDirName := "/rudder-warehouse-file-locations-dump/"
	pathPrefix := strcase.ToKebab(tableName)

	path := fmt.Sprintf(`%v%v.%v.%v.%v.json.gz`,
		tmpDirPath+backupPathDirName,
		pathPrefix,
		minID,
		maxID,
		timeutil.Now().Unix(),
	)
	defer os.Remove(path)

	fManager, err := filemanager.New(&filemanager.SettingsT{
		Provider: config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanager.GetProviderConfigFromEnv(),
	})

	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      dbHandle,
		Pagination:    backupRowsBatchSize,
		QueryTemplate: fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s where id >= %[2]d and id <= %[3]d order by id asc limit %[4]s offset %[5]s) AS dump_table`, tableName, minID, maxID, tablearchiver.PaginationAction, tablearchiver.OffsetAction),
		OutputPath:    path,
		FileManager:   fManager,
	}

	storedLocation, err := tableJSONArchiver.Do()

	if err != nil {
		logger.Errorf(`[WH Archiver]: Error archiving table %s: %v`, tableName, err)
		return
	}

	stmt = fmt.Sprintf(`DELETE FROM  %s WHERE id >= %d and id <= %d`, tableName, minID, maxID)
	_, err = dbHandle.Exec(stmt)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error in deleting %s records after archival: %v`, tableName, err)
		return
	}

	logger.Infof(`[WH Archiver]: Archived %s records %d to %d at %s`, tableName, minID, maxID, storedLocation)
}

func runArchiver(dbHandle *sql.DB) {
	for {
		if archiveLoadFiles {
			archiveOldRecords(warehouseutils.WarehouseLoadFilesTable, dbHandle)
		}
		if archiveStagingFiles {
			archiveOldRecords(warehouseutils.WarehouseStagingFilesTable, dbHandle)
		}
		time.Sleep(archiverTickerTime)
	}
}
