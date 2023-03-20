package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/iancoleman/strcase"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/archiver/tablearchiver"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
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
	config.RegisterDurationConfigVariable(360, &archiverTickerTime, true, time.Minute, []string{"Warehouse.archiverTickerTime", "Warehouse.archiverTickerTimeInMin"}...) // default 6 hours
}

type backupRecordsArgs struct {
	tableName      string
	tableFilterSQL string
	sourceID       string
	destID         string
	uploadID       int64
}

type uploadRecord struct {
	sourceID           string
	destID             string
	uploadID           int64
	startStagingFileId int64
	endStagingFileId   int64
	startLoadFileID    int64
	endLoadFileID      int64
	uploadMetdata      json.RawMessage
	workspaceID        string
}

type Archiver struct {
	DB          *sql.DB
	Stats       stats.Stats
	Logger      logger.Logger
	FileManager filemanager.FileManagerFactory
	Multitenant *multitenant.Manager
}

func (a *Archiver) backupRecords(args backupRecordsArgs) (backupLocation string, err error) {
	a.Logger.Infof(`Starting backupRecords for uploadId: %s, sourceId: %s, destinationId: %s, tableName: %s,`, args.uploadID, args.sourceID, args.destID, args.tableName)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		a.Logger.Errorf("[Archiver]: Failed to create tmp DIR")
		return
	}
	backupPathDirName := fmt.Sprintf(`/%s/`, misc.RudderArchives)
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

	fManager, err := a.FileManager.New(&filemanager.SettingsT{
		Provider: config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanager.GetProviderConfigForBackupsFromEnv(context.TODO()),
	})
	if err != nil {
		err = fmt.Errorf("error in creating a file manager for:%s. Error: %w", config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"), err)
		return
	}

	tmpl := fmt.Sprintf(`
		SELECT
		  json_agg(dump_table)
		FROM
		  (
			SELECT
			  *
			FROM
			  %[1]s
			WHERE
			  %[2]s
			ORDER BY
			  id ASC
			LIMIT
			  %[3]s offset %[4]s
		  ) AS dump_table
`,
		args.tableName,
		args.tableFilterSQL,
		tablearchiver.PaginationAction,
		tablearchiver.OffsetAction,
	)
	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      a.DB,
		Pagination:    config.GetInt("Warehouse.Archiver.backupRowsBatchSize", 100),
		QueryTemplate: tmpl,
		OutputPath:    path,
		FileManager:   fManager,
	}

	backupLocation, err = tableJSONArchiver.Do()
	a.Logger.Infof(`Completed backupRecords for uploadId: %s, sourceId: %s, destinationId: %s, tableName: %s,`, args.uploadID, args.sourceID, args.destID, args.tableName)
	return
}

func (a *Archiver) deleteFilesInStorage(locations []string) error {
	fManager, err := a.FileManager.New(&filemanager.SettingsT{
		Provider: warehouseutils.S3,
		Config:   misc.GetRudderObjectStorageConfig(""),
	})
	if err != nil {
		err = fmt.Errorf("error in creating a file manager for Rudder Storage. Error: %w", err)
		return err
	}

	err = fManager.DeleteObjects(context.TODO(), locations)
	if err != nil {
		a.Logger.Errorf("Error in deleting objects in Rudder S3: %v", err)
	}
	return err
}

func (*Archiver) usedRudderStorage(metadata []byte) bool {
	return gjson.GetBytes(metadata, "use_rudder_storage").Bool()
}

func (a *Archiver) Do(ctx context.Context) error {
	a.Logger.Infof(`Started archiving for warehouse`)
	sqlStatement := fmt.Sprintf(`
		SELECT
		  id,
		  source_id,
		  destination_id,
		  start_staging_file_id,
		  end_staging_file_id,
		  start_load_file_id,
		  end_load_file_id,
		  metadata,
		  workspace_id
		FROM
		  %s
		WHERE
		  (
			(
			  metadata ->> 'archivedStagingAndLoadFiles'
			):: bool IS DISTINCT
			FROM
			  TRUE
		  )
		  AND created_at < NOW() - $1::interval
		  AND status = $2
		  AND NOT workspace_id = ANY ( $3 )
		LIMIT
		  10000;
`,
		pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
	)

	// empty workspace id should be excluded as a safety measure
	skipWorkspaceIDs := []string{""}
	skipWorkspaceIDs = append(skipWorkspaceIDs, a.Multitenant.DegradedWorkspaces()...)

	rows, err := a.DB.QueryContext(ctx, sqlStatement,
		fmt.Sprintf("%d DAY", uploadsArchivalTimeInDays),
		model.ExportedData,
		pq.Array(skipWorkspaceIDs),
	)
	defer func() {
		if err != nil {
			a.Logger.Errorf(`Error occurred while archiving for warehouse uploads with error: %v`, err)
			a.Stats.NewTaggedStat("warehouse.archiver.archiveFailed", stats.CountType, stats.Tags{}).Count(1)
		}
	}()
	if err == sql.ErrNoRows {
		a.Logger.Debugf(`No uploads found for archival. Query: %s`, sqlStatement)
		return nil
	}
	if err != nil {
		return fmt.Errorf("querying wh_uploads for archival: %w", err)
	}
	var uploadsToArchive []*uploadRecord
	for rows.Next() {
		var u uploadRecord
		err := rows.Scan(
			&u.uploadID,
			&u.sourceID,
			&u.destID,
			&u.startStagingFileId,
			&u.endStagingFileId,
			&u.startLoadFileID,
			&u.endLoadFileID,
			&u.uploadMetdata,
			&u.workspaceID,
		)
		if err != nil {
			a.Logger.Errorf(`Error scanning wh_upload for archival. Error: %v`, err)
			continue
		}
		uploadsToArchive = append(uploadsToArchive, &u)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scanning rows: %w", err)
	}

	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing rows after scanning wh_uploads for archival: %w", err)
	}

	var archivedUploads int
	for _, u := range uploadsToArchive {

		txn, err := a.DB.Begin()
		if err != nil {
			a.Logger.Errorf(`Error creating txn in archiveUploadFiles. Error: %v`, err)
			continue
		}

		hasUsedRudderStorage := a.usedRudderStorage(u.uploadMetdata)

		// archive staging files
		stmt := fmt.Sprintf(`
			SELECT
			  id,
			  location
			FROM
			  %s
			WHERE
			  source_id = '%s'
			  AND destination_id = '%s'
			  AND id >= %d
			  and id <= %d;
`,
			warehouseutils.WarehouseStagingFilesTable,
			u.sourceID,
			u.destID,
			u.startStagingFileId,
			u.endStagingFileId,
		)

		stagingFileRows, err := txn.Query(stmt)
		if err != nil {
			a.Logger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
			txn.Rollback()
			continue
		}
		defer stagingFileRows.Close()

		var stagingFileIDs []int64
		var stagingFileLocations []string
		for stagingFileRows.Next() {
			var stagingFileID int64
			var stagingFileLocation string
			err = stagingFileRows.Scan(
				&stagingFileID,
				&stagingFileLocation,
			)
			if err != nil {
				txn.Rollback()
				return fmt.Errorf("scanning staging file id: %w", err)
			}
			stagingFileIDs = append(stagingFileIDs, stagingFileID)
			stagingFileLocations = append(stagingFileLocations, stagingFileLocation)
		}
		stagingFileRows.Close()

		var storedStagingFilesLocation string
		if len(stagingFileIDs) > 0 {
			if !hasUsedRudderStorage {
				filterSQL := fmt.Sprintf(`id IN (%v)`, misc.IntArrayToString(stagingFileIDs, ","))
				storedStagingFilesLocation, err = a.backupRecords(backupRecordsArgs{
					tableName:      warehouseutils.WarehouseStagingFilesTable,
					sourceID:       u.sourceID,
					destID:         u.destID,
					tableFilterSQL: filterSQL,
					uploadID:       u.uploadID,
				})

				if err != nil {
					a.Logger.Errorf(`Error backing up staging files for upload:%d : %v`, u.uploadID, err)
					txn.Rollback()
					continue
				}
			} else {
				a.Logger.Debugf(`Object storage not configured to archive upload related staging file records. Deleting the ones that need to be archived for upload:%d`, u.uploadID)
			}

			if hasUsedRudderStorage {
				err = a.deleteFilesInStorage(stagingFileLocations)
				if err != nil {
					a.Logger.Errorf(`Error deleting staging files from Rudder S3. Error: %v`, stmt, err)
					txn.Rollback()
					continue
				}
			}

			// delete staging file records
			stmt = fmt.Sprintf(`
				DELETE FROM
				  %s
				WHERE
				  id IN (%v);
`,
				warehouseutils.WarehouseStagingFilesTable,
				misc.IntArrayToString(stagingFileIDs, ","),
			)
			_, err = txn.Query(stmt)
			if err != nil {
				a.Logger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
				txn.Rollback()
				continue
			}

			// delete load file records
			stmt = fmt.Sprintf(`
				DELETE FROM
				  %s
				WHERE
				  staging_file_id = ANY($1) RETURNING location;
`,
				warehouseutils.WarehouseLoadFilesTable,
			)
			loadLocationRows, err := txn.Query(stmt, pq.Array(stagingFileIDs))
			if err != nil {
				a.Logger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
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
						txn.Rollback()
						return fmt.Errorf("scanning load file location: %w", err)
					}
					loadLocations = append(loadLocations, loc)
				}
				loadLocationRows.Close()
				var paths []string
				for _, loc := range loadLocations {
					u, err := url.Parse(loc)
					if err != nil {
						a.Logger.Errorf(`Error deleting load files from Rudder S3. Error: %v`, stmt, err)
						txn.Rollback()
						continue
					}
					paths = append(paths, u.Path[1:])
				}
				err = a.deleteFilesInStorage(paths)
				if err != nil {
					a.Logger.Errorf(`Error deleting load files from Rudder S3. Error: %v`, stmt, err)
					txn.Rollback()
					continue
				}
			}
			loadLocationRows.Close()
		}

		// update upload metadata
		u.uploadMetdata, _ = sjson.SetBytes(u.uploadMetdata, "archivedStagingAndLoadFiles", true)
		stmt = fmt.Sprintf(`
			UPDATE
			  %s
			SET
			  metadata = $1
			WHERE
			  id = %d;
`,
			warehouseutils.WarehouseUploadsTable,
			u.uploadID,
		)
		_, err = txn.Exec(stmt, u.uploadMetdata)
		if err != nil {
			a.Logger.Errorf(`Error running txn in archiveUploadFiles. Query: %s Error: %v`, stmt, err)
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
			a.Logger.Debugf(`[Archiver]: Archived upload: %d related staging files at: %s`, u.uploadID, storedStagingFilesLocation)
		}

		a.Stats.NewTaggedStat("warehouse.archiver.numArchivedUploads", stats.CountType, stats.Tags{
			"destination": u.destID,
			"source":      u.sourceID,
		}).Count(1)
	}
	a.Logger.Infof(`Successfully archived %d uploads`, archivedUploads)
	return nil
}
