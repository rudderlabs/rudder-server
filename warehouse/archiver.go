package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/archiver/tablearchiver"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/sjson"
)

var (
	archiveUploadRelatedRecords bool
	uploadsArchivalTimeInDays   int
	archiverTickerTime          time.Duration
)

func init() {
	archiveUploadRelatedRecords = config.GetBool("Warehouse.archiveUploadRelatedRecords", true)
	uploadsArchivalTimeInDays = config.GetInt("Warehouse.uploadsArchivalTimeInDays", 7)
	archiverTickerTime = config.GetDuration("Warehouse.archiverTickerTimeInMin", 1440) * time.Minute // default 1 day
}

type backupRecordsArgs struct {
	tableName      string
	tableFilterSQL string
	sourceID       string
	destID         string
	uploadID       int64
}

func backupRecords(args backupRecordsArgs) (backupLocation string, err error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[Archiver]: Failed to create tmp DIR")
		return
	}
	backupPathDirName := "/rudder-archives/"
	pathPrefix := strcase.ToKebab(warehouseutils.WarehouseStagingFilesTable)

	path := fmt.Sprintf(`%v%v.%v.%v.%v.%v.json.gz`,
		tmpDirPath+backupPathDirName,
		pathPrefix,
		args.sourceID,
		args.destID,
		args.uploadID,
		timeutil.Now().Unix(),
	)
	defer os.Remove(path)

	fManager, err := filemanager.New(&filemanager.SettingsT{
		Provider: config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanager.GetProviderConfigFromEnv(),
	})
	if err != nil {
		err = errors.New(fmt.Sprintf(`Error in creating a file manager for:%s. Error: %v`, config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"), err))
		return
	}

	tmpl := fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s WHERE %[2]s order by id asc limit %[3]s offset %[4]s) AS dump_table`, args.tableName, args.tableFilterSQL, tablearchiver.PaginationAction, tablearchiver.OffsetAction)
	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      dbHandle,
		Pagination:    config.GetInt("Archiver.backupRowsBatchSize", 100),
		QueryTemplate: tmpl,
		OutputPath:    path,
		FileManager:   fManager,
	}

	backupLocation, err = tableJSONArchiver.Do()
	return
}

func archiveUploads(dbHandle *sql.DB) {
	sqlStatement := fmt.Sprintf(`SELECT id,source_id, destination_id, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, metadata FROM %s WHERE ((metadata->>'archivedStagingAndLoadFiles')::bool IS DISTINCT FROM TRUE) AND created_at < NOW() -INTERVAL '%d DAY' AND status = '%s'`, warehouseutils.WarehouseUploadsTable, uploadsArchivalTimeInDays, ExportedData)

	rows, err := dbHandle.Query(sqlStatement)
	if err == sql.ErrNoRows {
		pkgLogger.Debugf(`No uploads found for acrhival. Query: %s`, sqlStatement)
		return
	}
	if err != nil {
		pkgLogger.Errorf(`Error querying wh_uploads for acrhival. Query: %s, Error: %v`, sqlStatement, err)
		return
	}
	defer rows.Close()
	var archivedUploads int
	for rows.Next() {
		var sourceID, destID string
		var uploadID, startStagingFileId, endStagingFileId, startLoadFileID, endLoadFileID int64
		var uploadMetdata json.RawMessage
		err = rows.Scan(&uploadID, &sourceID, &destID, &startStagingFileId, &endStagingFileId, &startLoadFileID, &endLoadFileID, &uploadMetdata)
		if err != nil {
			pkgLogger.Errorf(`Error scanning wh_upload for acrhival. Error: %v`, err)
			continue
		}

		txn, err := dbHandle.Begin()
		if err != nil {
			pkgLogger.Errorf(`Error creating txn in archiveUploadFiles. Error: %v`, err)
			continue
		}

		// archive staging files
		var storedStagingFilesLocation string
		if archiver.IsArchiverObjectStorageConfigured() {
			filterSQL := fmt.Sprintf(`source_id='%[1]s' AND destination_id='%[2]s' AND id >= %[3]d and id <= %[4]d`, sourceID, destID, startStagingFileId, endStagingFileId)
			storedStagingFilesLocation, err = backupRecords(backupRecordsArgs{
				tableName:      warehouseutils.WarehouseLoadFilesTable,
				sourceID:       sourceID,
				destID:         destID,
				tableFilterSQL: filterSQL,
				uploadID:       uploadID,
			})

			if err != nil {
				pkgLogger.Errorf(`Error backing up staging files for upload:%d : %v`, uploadID, err)
				txn.Rollback()
				continue
			}
		}

		// delete staging files
		stmt := fmt.Sprintf(`DELETE FROM %s WHERE source_id='%s' AND destination_id='%s' AND id >= %d and id <= %d RETURNING id`, warehouseutils.WarehouseStagingFilesTable, sourceID, destID, startStagingFileId, endStagingFileId)

		stagingFileRows, err := txn.Query(stmt)
		if err != nil {
			pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
			txn.Rollback()
			continue
		}
		defer stagingFileRows.Close()

		var stagingFileIDs []int64
		for stagingFileRows.Next() {
			var stagingFileID int64
			err = stagingFileRows.Scan(&stagingFileID)
			if err != nil {
				pkgLogger.Errorf(`Error scanning staging file id in archiveUploadFiles. Error: %v`, err)
				txn.Rollback()
				return
			}
			stagingFileIDs = append(stagingFileIDs, stagingFileID)
		}

		// archive load files
		var storedLoadFilesLocation string
		if archiver.IsArchiverObjectStorageConfigured() {
			filterSQL := fmt.Sprintf(`staging_file_id IN (%v)`, misc.IntArrayToString(stagingFileIDs, ","))
			storedLoadFilesLocation, err = backupRecords(backupRecordsArgs{
				tableName:      warehouseutils.WarehouseLoadFilesTable,
				sourceID:       sourceID,
				destID:         destID,
				tableFilterSQL: filterSQL,
				uploadID:       uploadID,
			})

			if err != nil {
				pkgLogger.Errorf(`Error backing up load files for upload:%d : %v`, uploadID, err)
				txn.Rollback()
				continue
			}
		}

		// delete load files
		stmt = fmt.Sprintf(`DELETE FROM %s WHERE staging_file_id = ANY($1)`, warehouseutils.WarehouseLoadFilesTable)
		_, err = txn.Exec(stmt, pq.Array(stagingFileIDs))
		if err != nil {
			pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
			txn.Rollback()
			continue
		}

		// update upload metadata
		uploadMetdata, _ = sjson.SetBytes(uploadMetdata, "archivedStagingAndLoadFiles", true)
		stmt = fmt.Sprintf(`UPDATE %s SET metadata = $1 WHERE id = %d`, warehouseutils.WarehouseUploadsTable, uploadID)
		_, err = txn.Exec(stmt, uploadMetdata)
		if err != nil {
			pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
			txn.Rollback()
			continue
		}

		err = txn.Commit()
		if err != nil {
			txn.Rollback()
			continue
		}
		archivedUploads++
		if storedStagingFilesLocation != "" {
			pkgLogger.Debugf(`[Archiver]: Archived upload: %d related staging files at: %s`, uploadID, storedStagingFilesLocation)
			pkgLogger.Debugf(`[Archiver]: Archived upload: %d related load files at: %s`, uploadID, storedLoadFilesLocation)
		}
	}
	pkgLogger.Infof(`Successfully archived %d uploads`, archivedUploads)
}

func runArchiver(dbHandle *sql.DB) {
	for {
		if archiveUploadRelatedRecords {
			archiveUploads(dbHandle)
		}
		time.Sleep(archiverTickerTime)
	}
}
