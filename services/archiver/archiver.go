package archiver

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/archiver/tablearchiver"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

var (
	backupRowsBatchSize int
	pkgLogger           logger.LoggerI
)

func init() {
	backupRowsBatchSize = config.GetInt("Archiver.backupRowsBatchSize", 100)
	pkgLogger = logger.NewLogger().Child("archiver")
}

func isArchiverObjectStorageConfigured() bool {
	provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "")
	bucket := config.GetEnv("JOBS_BACKUP_BUCKET", "")
	return provider != "" && bucket != ""
}

//ArchiveOldRecords archives records in the table with the name`tableName` and `tsColumn` provided is used as the timestamp column.
func ArchiveOldRecords(tableName, tsColumn string, archivalTimeInDays int, dbHandle *sql.DB) {
	stmt := fmt.Sprintf(`SELECT count(*), COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM %s WHERE %s < NOW() -INTERVAL '%d DAY'`, tableName, tsColumn, archivalTimeInDays)
	pkgLogger.Info(stmt)

	var filesCount, minID, maxID int64
	err := dbHandle.QueryRow(stmt).Scan(&filesCount, &minID, &maxID)
	if err != nil {
		pkgLogger.Errorf(`[Archiver]: Error in fetching %s records count for archival: %v`, tableName, err)
		return
	}
	if filesCount == 0 {
		pkgLogger.Infof(`[Archiver]: No %s records found to archive`, tableName)
		return
	}

	// TODO: Should we skip deletion if object storage provider not configured?
	if !isArchiverObjectStorageConfigured() {
		stmt = fmt.Sprintf(`DELETE FROM  %s WHERE id >= %d and id <= %d`, tableName, minID, maxID)
		_, err = dbHandle.Exec(stmt)
		if err != nil {
			pkgLogger.Errorf(`[Archiver]: Error in deleting %s records: %v`, tableName, err)
			return
		}
		pkgLogger.Infof(`[Archiver]: Deleted %s records %d to %d. No object storage was configured for archival`, tableName, minID, maxID)
		return
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[Archiver]: Failed to create tmp DIR")
		return
	}
	backupPathDirName := "/rudder-archives/"
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
	if err != nil {
		pkgLogger.Errorf("[Archiver]: Error in creating a file manager for :%s: , %v", config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"), err)
	}

	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      dbHandle,
		Pagination:    backupRowsBatchSize,
		QueryTemplate: fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s where id >= %[2]d and id <= %[3]d order by id asc limit %[4]s offset %[5]s) AS dump_table`, tableName, minID, maxID, tablearchiver.PaginationAction, tablearchiver.OffsetAction),
		OutputPath:    path,
		FileManager:   fManager,
	}

	storedLocation, err := tableJSONArchiver.Do()

	if err != nil {
		pkgLogger.Errorf(`[Archiver]: Error archiving table %s: %v`, tableName, err)
		return
	}

	stmt = fmt.Sprintf(`DELETE FROM  %s WHERE id >= %d and id <= %d`, tableName, minID, maxID)
	_, err = dbHandle.Exec(stmt)
	if err != nil {
		pkgLogger.Errorf(`[Archiver]: Error in deleting %s records after archival: %v`, tableName, err)
		return
	}

	pkgLogger.Infof(`[Archiver]: Archived %s records %d to %d at %s`, tableName, minID, maxID, storedLocation)
}
