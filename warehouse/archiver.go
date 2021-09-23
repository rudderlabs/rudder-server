package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	archiveUploadRelatedRecords bool
	uploadsArchivalTimeInDays   int
	archiverTickerTime          time.Duration
)

func Init() {
	loadConfigArchiver()
}

func loadConfigArchiver() {
	config.RegisterBoolConfigVariable(true, &archiveUploadRelatedRecords, true, "Warehouse.archiveUploadRelatedRecords")
	config.RegisterIntConfigVariable(5, &uploadsArchivalTimeInDays, true, 1, "Warehouse.uploadsArchivalTimeInDays")
	config.RegisterDurationConfigVariable(time.Duration(1440), &archiverTickerTime, true, time.Minute, []string{"Warehouse.archiverTickerTime", "Warehouse.archiverTickerTimeInMin"}...) // default 1 day
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
	defer misc.RemoveFilePaths(path)

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

func deleteFilesInStorage(locations []string) error {
	fManager, err := filemanager.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   misc.GetRudderObjectStorageConfig(""),
	})
	if err != nil {
		err = errors.New(fmt.Sprintf(`Error in creating a file manager for Rudder Storage. Error: %v`, err))
		return err
	}

	err = fManager.DeleteObjects(locations)
	if err != nil {
		pkgLogger.Errorf("Error in deleting objects in Rudder S3: %v", err)
	}
	return err
}

func usedRudderStorage(uploadMetdata []byte) bool {
	return gjson.GetBytes(uploadMetdata, "use_rudder_storage").Bool()
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

		hasUsedRudderStorage := usedRudderStorage(uploadMetdata)

		// archive staging files
		stmt := fmt.Sprintf(`SELECT id, location FROM %s WHERE source_id='%s' AND destination_id='%s' AND id >= %d and id <= %d`, warehouseutils.WarehouseStagingFilesTable, sourceID, destID, startStagingFileId, endStagingFileId)

		stagingFileRows, err := txn.Query(stmt)
		if err != nil {
			pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
			txn.Rollback()
			continue
		}
		defer stagingFileRows.Close()

		var stagingFileIDs []int64
		var stagingFileLocations []string
		for stagingFileRows.Next() {
			var stagingFileID int64
			var stagingFileLocation string
			err = stagingFileRows.Scan(&stagingFileID, &stagingFileLocation)
			if err != nil {
				pkgLogger.Errorf(`Error scanning staging file id in archiveUploadFiles. Error: %v`, err)
				txn.Rollback()
				return
			}
			stagingFileIDs = append(stagingFileIDs, stagingFileID)
			stagingFileLocations = append(stagingFileLocations, stagingFileLocation)
		}
		stagingFileRows.Close()

		var storedStagingFilesLocation, storedLoadFilesLocation string
		if len(stagingFileIDs) > 0 {
			if archiver.IsArchiverObjectStorageConfigured() {
				filterSQL := fmt.Sprintf(`id IN (%v)`, misc.IntArrayToString(stagingFileIDs, ","))
				storedStagingFilesLocation, err = backupRecords(backupRecordsArgs{
					tableName:      warehouseutils.WarehouseStagingFilesTable,
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
			} else {
				pkgLogger.Debugf(`Object storage not configured to archive upload related staging file records. Deleting the ones that need to be archived for upload:%d`, uploadID)
			}

			if hasUsedRudderStorage {
				err = deleteFilesInStorage(stagingFileLocations)
				if err != nil {
					pkgLogger.Errorf(`Error deleting staging files from Rudder S3. Error: %v`, stmt, err)
					txn.Rollback()
					continue
				}
			}

			// delete staging files
			stmt = fmt.Sprintf(`DELETE FROM %s WHERE id IN (%v)`, warehouseutils.WarehouseStagingFilesTable, misc.IntArrayToString(stagingFileIDs, ","))
			_, err = txn.Query(stmt)
			if err != nil {
				pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
				txn.Rollback()
				continue
			}

			// archive load files
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
			} else {
				pkgLogger.Infof(`Object storage not configured to archive upload related load file records. Deleting the ones that need to be archived for upload:%d`, uploadID)
			}

			// delete load files
			stmt = fmt.Sprintf(`DELETE FROM %s WHERE staging_file_id = ANY($1) RETURNING location`, warehouseutils.WarehouseLoadFilesTable)
			loadLocationRows, err := txn.Query(stmt, pq.Array(stagingFileIDs))
			if err != nil {
				pkgLogger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
				txn.Rollback()
				continue
			}

			defer loadLocationRows.Close()

			if hasUsedRudderStorage {
				var loadLocations []string
				for loadLocationRows.Next() {
					var loc string
					err = loadLocationRows.Scan(&loc)
					if err != nil {
						pkgLogger.Errorf(`Error scanning location in archiveUploadFiles. Error: %v`, err)
						txn.Rollback()
						return
					}
					loadLocations = append(loadLocations, loc)
				}
				loadLocationRows.Close()
				var paths []string
				for _, loc := range loadLocations {
					u, err := url.Parse(loc)
					if err != nil {
						pkgLogger.Errorf(`Error deleting load files from Rudder S3. Error: %v`, stmt, err)
						txn.Rollback()
						continue
					}
					paths = append(paths, u.Path[1:])
				}
				err = deleteFilesInStorage(paths)
				if err != nil {
					pkgLogger.Errorf(`Error deleting load files from Rudder S3. Error: %v`, stmt, err)
					txn.Rollback()
					continue
				}
			}
			loadLocationRows.Close()
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
