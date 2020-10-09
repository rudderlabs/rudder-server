package warehouse

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	archiveLoadFiles    bool
	archiveStagingFiles bool
	archivalTimeInDays  int
	backupRowsBatchSize int64
	archiverTickerTime  time.Duration
)

func init() {
	archiveLoadFiles = config.GetBool("Warehouse.archiveLoadFiles", true)
	archiveStagingFiles = config.GetBool("Warehouse.archiveStagingFiles", true)
	archivalTimeInDays = config.GetInt("Warehouse.archivalTimeInDays", 45)
	backupRowsBatchSize = config.GetInt64("Warehouse.backupRowsBatchSize", 1000)
	archiverTickerTime = config.GetDuration("Warehouse.archiverTickerTimeInMin", 120) * time.Minute
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
		logger.Debugf(`[WH Archiver]: No %s records found to archive`, tableName)
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

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error in creating local directory: %v`, err)
		return
	}

	gzWriter, err := misc.CreateGZ(path)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error in creating gzWriter: %v`, err)
		return
	}
	defer os.Remove(path)

	var offset int64
	for {
		stmt := fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s where id >= %[2]d and id <= %[3]d order by id asc limit %[4]d offset %[5]d) AS dump_table`, tableName, minID, maxID, backupRowsBatchSize, offset)
		var rawJSONRows json.RawMessage
		row := dbHandle.QueryRow(stmt)
		err = row.Scan(&rawJSONRows)
		if err != nil {
			logger.Errorf(`[WH Archiver]: Scanning row failed with error : %v`, err)
			return
		}

		rawJSONRows = bytes.Replace(rawJSONRows, []byte("}, \n {"), []byte("}\n{"), -1) //replacing ", \n " with "\n"
		rawJSONRows = rawJSONRows[1 : len(rawJSONRows)-1]                               //stripping starting '[' and ending ']'
		rawJSONRows = append(rawJSONRows, '\n')                                         //appending '\n'

		gzWriter.Write(rawJSONRows)
		offset += backupRowsBatchSize
		if offset >= filesCount {
			break
		}
	}

	gzWriter.CloseGZ()

	file, err := os.Open(path)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error opening local file dump: %v`, err)
		return
	}
	defer file.Close()

	fManager, err := filemanager.New(&filemanager.SettingsT{
		Provider: config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanager.GetProviderConfigFromEnv(),
	})

	output, err := fManager.Upload(file)

	if err != nil {
		logger.Errorf(`[WH Archiver]: Error uploading local file dump to object storage: %v`, err)
		return
	}

	stmt = fmt.Sprintf(`DELETE FROM  %s WHERE id >= %d and id <= %d`, tableName, minID, maxID)
	_, err = dbHandle.Exec(stmt)
	if err != nil {
		logger.Errorf(`[WH Archiver]: Error in deleting %s records after archival: %v`, tableName, err)
		return
	}

	logger.Infof(`[WH Archiver]: Archived %s records %d to %d at %s`, tableName, minID, maxID, output.Location)
}

func archiveFileLocationRecords(dbHandle *sql.DB) {
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
