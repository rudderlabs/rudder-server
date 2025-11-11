package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/lib/pq"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/services/archiver/tablearchiver"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
	uploadMetadata     json.RawMessage
	workspaceID        string
}

type Archiver struct {
	db            *sqlmw.DB
	stats         stats.Stats
	log           logger.Logger
	conf          *config.Config
	fileManager   filemanager.Factory
	tenantManager *multitenant.Manager

	config struct {
		archiveUploadRelatedRecords config.ValueLoader[bool]
		canDeleteUploads            config.ValueLoader[bool]
		uploadsArchivalTimeInDays   config.ValueLoader[int]
		uploadRetentionTimeInDays   config.ValueLoader[int]
		archiverTickerTime          config.ValueLoader[time.Duration]
		backupRowsBatchSize         config.ValueLoader[int]
		maxLimit                    config.ValueLoader[int]
	}

	archiveFailedStat stats.Counter
}

func New(
	conf *config.Config,
	log logger.Logger,
	stat stats.Stats,
	db *sqlmw.DB,
	fileManager filemanager.Factory,
	tenantManager *multitenant.Manager,
) *Archiver {
	a := &Archiver{
		conf:          conf,
		log:           log.Child("archiver"),
		stats:         stat,
		db:            db,
		fileManager:   fileManager,
		tenantManager: tenantManager,
	}

	a.config.archiveUploadRelatedRecords = a.conf.GetReloadableBoolVar(true, "Warehouse.archiveUploadRelatedRecords")
	a.config.canDeleteUploads = a.conf.GetReloadableBoolVar(false, "Warehouse.canDeleteUploads")
	a.config.uploadsArchivalTimeInDays = a.conf.GetReloadableIntVar(5, 1, "Warehouse.uploadsArchivalTimeInDays")
	a.config.uploadRetentionTimeInDays = a.conf.GetReloadableIntVar(90, 1, "Warehouse.uploadRetentionTimeInDays")
	a.config.backupRowsBatchSize = a.conf.GetReloadableIntVar(100, 1, "Warehouse.Archiver.backupRowsBatchSize")
	a.config.archiverTickerTime = a.conf.GetReloadableDurationVar(360, time.Minute, "Warehouse.archiverTickerTime", "Warehouse.archiverTickerTimeInMin") // default 6 hours
	a.config.maxLimit = a.conf.GetReloadableIntVar(10000, 1, "Warehouse.Archiver.maxLimit")

	a.archiveFailedStat = a.stats.NewStat("warehouse.archiver.archiveFailed", stats.CountType)

	return a
}

func (a *Archiver) backupRecords(ctx context.Context, args backupRecordsArgs) (backupLocation string, err error) {
	a.log.Infon("[Archiver]: Starting backupRecords",
		obskit.UploadID(args.uploadID),
		logger.NewStringField(logfield.SourceID, args.sourceID),
		logger.NewStringField(logfield.DestinationID, args.destID),
		logger.NewStringField(logfield.TableName, args.tableName),
	)

	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		a.log.Errorn("[Archiver]: Failed to create tmp DIR", obskit.Error(err))
		return backupLocation, err
	}
	backupPathDirName := "/" + misc.RudderArchives + "/"
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

	fManager, err := a.fileManager(&filemanager.Settings{
		Provider: a.conf.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanagerutil.GetProviderConfigForBackupsFromEnv(ctx, a.conf),
		Conf:     a.conf,
	})
	if err != nil {
		err = fmt.Errorf("error in creating a file manager for:%s. Error: %w",
			a.conf.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"), err,
		)
		return backupLocation, err
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
		  ) AS dump_table`,
		args.tableName,
		args.tableFilterSQL,
		tablearchiver.PaginationAction,
		tablearchiver.OffsetAction,
	)
	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      a.db.DB,
		Pagination:    a.config.backupRowsBatchSize.Load(),
		QueryTemplate: tmpl,
		OutputPath:    path,
		FileManager:   fManager,
	}

	backupLocation, err = tableJSONArchiver.Do()
	a.log.Infon("[Archiver]: Completed backupRecords",
		obskit.UploadID(args.uploadID),
		logger.NewStringField(logfield.SourceID, args.sourceID),
		logger.NewStringField(logfield.DestinationID, args.destID),
		logger.NewStringField(logfield.TableName, args.tableName),
	)

	return backupLocation, err
}

func (a *Archiver) deleteFilesInStorage(ctx context.Context, locations []string) error {
	fManager, err := a.fileManager(&filemanager.Settings{
		Provider: warehouseutils.S3,
		Config:   misc.GetRudderObjectStorageConfig(""),
		Conf:     a.conf,
	})
	if err != nil {
		err = fmt.Errorf("error in creating a file manager for Rudder Storage. Error: %w", err)
		return err
	}

	err = fManager.Delete(ctx, locations)
	if err != nil {
		a.log.Errorn("[Archiver]: Error in deleting objects in Rudder S3", obskit.Error(err))
	}
	return err
}

func (*Archiver) usedRudderStorage(metadata []byte) bool {
	return gjson.GetBytes(metadata, "use_rudder_storage").Bool()
}

func (a *Archiver) Do(ctx context.Context) error {
	a.log.Infon("[Archiver]: Started archiving for warehouse")

	uploadsToArchive, err := a.countUploadsToArchive(ctx)
	if err != nil {
		return fmt.Errorf("counting uploads to archive: %w", err)
	}

	maxLimit := a.config.maxLimit.Load()

	for uploadsToArchive > 0 {
		if err := a.archiveUploads(ctx, maxLimit); err != nil {
			return fmt.Errorf("archiving uploads: %w", err)
		}
		uploadsToArchive -= maxLimit
	}
	return nil
}

func (a *Archiver) countUploadsToArchive(ctx context.Context) (int, error) {
	skipWorkspaceIDs := []string{""}
	skipWorkspaceIDs = append(skipWorkspaceIDs, a.tenantManager.DegradedWorkspaces()...)

	sqlStatement := fmt.Sprintf(`
		SELECT
		  count(*)
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
		  AND NOT workspace_id = ANY ( $3 );`,
		pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
	)

	var totalUploads int

	err := a.db.QueryRowContext(
		ctx,
		sqlStatement,
		fmt.Sprintf("%d DAY", a.config.uploadsArchivalTimeInDays.Load()),
		model.ExportedData,
		pq.Array(skipWorkspaceIDs),
	).Scan(&totalUploads)
	return totalUploads, err
}

func (a *Archiver) archiveUploads(ctx context.Context, maxArchiveLimit int) error {
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
		  $4;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
	)

	// empty workspace id should be excluded as a safety measure
	skipWorkspaceIDs := []string{""}
	skipWorkspaceIDs = append(skipWorkspaceIDs, a.tenantManager.DegradedWorkspaces()...)

	rows, err := a.db.QueryContext(ctx, sqlStatement,
		fmt.Sprintf("%d DAY", a.config.uploadsArchivalTimeInDays.Load()),
		model.ExportedData,
		pq.Array(skipWorkspaceIDs),
		maxArchiveLimit,
	)
	defer func() {
		if err != nil {
			a.log.Errorn("[Archiver]: Error occurred while archiving for warehouse uploads", obskit.Error(err))
			a.archiveFailedStat.Increment()
		}
	}()
	if errors.Is(err, sql.ErrNoRows) {
		a.log.Debugn("[Archiver]: No uploads found for archival", logger.NewStringField(logfield.Query, sqlStatement))
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
			&u.uploadMetadata,
			&u.workspaceID,
		)
		if err != nil {
			a.log.Errorn("[Archiver]: Error scanning wh_upload for archival", obskit.Error(err))
			continue
		}
		uploadsToArchive = append(uploadsToArchive, &u)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scanning wh_uploads rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing rows after scanning wh_uploads for archival: %w", err)
	}

	var archivedUploads int
	for _, u := range uploadsToArchive {
		txn, err := a.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			a.log.Errorn("[Archiver]: Error creating txn in archiveUploadFiles", obskit.Error(err))
			continue
		}

		// archive staging files
		stagingFileIDs, err := a.getStagingFilesData(ctx, txn, u)
		if err != nil {
			a.log.Errorn("[Archiver]: Error getting staging files data for upload",
				obskit.UploadID(u.uploadID),
				obskit.Error(err),
			)
			_ = txn.Rollback()
			continue
		}

		var storedStagingFilesLocation string
		if len(stagingFileIDs) > 0 {
			filterSQL := fmt.Sprintf(`id IN (%v)`, misc.IntArrayToString(stagingFileIDs, ","))
			storedStagingFilesLocation, err = a.backupRecords(ctx, backupRecordsArgs{
				tableName:      warehouseutils.WarehouseStagingFilesTable,
				sourceID:       u.sourceID,
				destID:         u.destID,
				tableFilterSQL: filterSQL,
				uploadID:       u.uploadID,
			})
			if err != nil {
				a.log.Errorn("[Archiver]: Error backing up staging files for upload",
					obskit.UploadID(u.uploadID),
					obskit.Error(err),
				)
				_ = txn.Rollback()
				continue
			}

			// delete staging file records
			stmt := fmt.Sprintf(`
				DELETE FROM %s
				WHERE id = ANY($1);`,
				pq.QuoteIdentifier(warehouseutils.WarehouseStagingFilesTable),
			)
			_, err = txn.ExecContext(ctx, stmt, pq.Array(stagingFileIDs))
			if err != nil {
				a.log.Errorn("[Archiver]: Error running txn in archiveUploadFiles",
					logger.NewStringField(logfield.Query, stmt),
					obskit.Error(err),
				)
				_ = txn.Rollback()
				continue
			}

			hasUsedRudderStorage := a.usedRudderStorage(u.uploadMetadata)

			// delete load file records
			if err := a.deleteLoadFileRecords(ctx, txn, u.uploadID, hasUsedRudderStorage); err != nil {
				a.log.Errorn("[Archiver]: Error while deleting load file records for upload",
					obskit.UploadID(u.uploadID),
					obskit.Error(err),
				)
				_ = txn.Rollback()
				continue
			}
		}

		// update upload metadata
		u.uploadMetadata, _ = sjson.SetBytes(u.uploadMetadata, "archivedStagingAndLoadFiles", true)
		stmt := fmt.Sprintf(`
			UPDATE %s
			SET metadata = $1
			WHERE id = $2;`,
			pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
		)
		_, err = txn.ExecContext(ctx, stmt, u.uploadMetadata, u.uploadID)
		if err != nil {
			a.log.Errorn("[Archiver]: Error running txn while archiving upload files",
				logger.NewStringField(logfield.Query, stmt),
				obskit.Error(err),
			)
			_ = txn.Rollback()
			continue
		}

		if err = txn.Commit(); err != nil {
			a.log.Errorn("[Archiver]: Error committing txn while archiving upload files", obskit.Error(err))
			_ = txn.Rollback()
			continue
		}

		archivedUploads++
		if storedStagingFilesLocation != "" {
			a.log.Debugn("[Archiver]: Archived upload related staging files",
				obskit.UploadID(u.uploadID),
				logger.NewStringField("location", storedStagingFilesLocation),
			)
		}

		a.stats.NewTaggedStat("warehouse.archiver.numArchivedUploads", stats.CountType, stats.Tags{
			"destination": u.destID,
			"source":      u.sourceID,
		}).Increment()
	}

	a.log.Infon("[Archiver]: Successfully archived uploads",
		logger.NewIntField("archivedUploads", int64(archivedUploads)),
	)
	return nil
}

func (a *Archiver) getStagingFilesData(
	ctx context.Context,
	txn *sqlmw.Tx,
	u *uploadRecord,
) ([]int64, error) {
	stmt := fmt.Sprintf(`
		SELECT
		  id
		FROM
		  %s
		WHERE
		  upload_id = $1;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseStagingFilesTable),
	)

	stagingFileRows, err := txn.QueryContext(ctx, stmt,
		u.uploadID,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot query staging files data: %w", err)
	}
	defer func() { _ = stagingFileRows.Close() }()

	var stagingFileIDs []int64
	for stagingFileRows.Next() {
		var stagingFileID int64
		err = stagingFileRows.Scan(&stagingFileID)
		if err != nil {
			return nil, fmt.Errorf("scanning staging file id: %w", err)
		}
		stagingFileIDs = append(stagingFileIDs, stagingFileID)
	}
	if err = stagingFileRows.Err(); err != nil {
		return nil, fmt.Errorf("iterating staging file ids: %w", err)
	}

	return stagingFileIDs, nil
}

func (a *Archiver) deleteLoadFileRecords(
	ctx context.Context,
	txn *sqlmw.Tx,
	uploadID int64,
	hasUsedRudderStorage bool,
) error {
	stmt := fmt.Sprintf(`
		DELETE FROM %s
		WHERE upload_id = $1
		RETURNING location;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseLoadFilesTable),
	)
	loadLocationRows, err := txn.QueryContext(ctx, stmt, uploadID)
	if err != nil {
		return fmt.Errorf("cannot delete load files with upload_id = %+v: %w", uploadID, err)
	}

	defer func() { _ = loadLocationRows.Close() }()

	if !hasUsedRudderStorage {
		return nil // no need to delete files in rudder storage
	}

	var loadLocations []string
	for loadLocationRows.Next() {
		var loc string
		if err = loadLocationRows.Scan(&loc); err != nil {
			return fmt.Errorf("cannot scan load file location: %w", err)
		}

		u, err := url.Parse(loc)
		if err != nil {
			return fmt.Errorf("cannot parse load file location %q: %w", loc, err)
		}

		loadLocations = append(loadLocations, u.Path[1:])
	}
	if err = loadLocationRows.Err(); err != nil {
		return fmt.Errorf("iterating load file locations: %w", err)
	}

	err = a.deleteFilesInStorage(ctx, loadLocations)
	if err != nil {
		return fmt.Errorf("error deleting files in storage: %w", err)
	}

	return nil
}

func (a *Archiver) Delete(ctx context.Context) error {
	a.log.Infon("Started deleting for warehouse")

	maxLimit := a.config.maxLimit.Load()

	for {
		count, err := a.deleteUploads(ctx, maxLimit)
		if err != nil {
			return fmt.Errorf("deleting uploads: %w", err)
		}
		if count == 0 {
			break
		}
	}
	return nil
}

func (a *Archiver) deleteUploads(ctx context.Context, limit int) (int64, error) {
	skipWorkspaceIDs := []string{""}
	skipWorkspaceIDs = append(skipWorkspaceIDs, a.tenantManager.DegradedWorkspaces()...)

	sqlStatement := fmt.Sprintf(`
		WITH rows_to_delete AS (
    		SELECT ctid
    			FROM %[1]s
    			WHERE created_at < NOW() - $1::interval
      			AND status = $2
      			AND NOT workspace_id = ANY ($3)
    			LIMIT $4
		)
		DELETE FROM %[1]s
		WHERE ctid IN (SELECT ctid FROM rows_to_delete);
`,
		pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable))

	result, err := a.db.ExecContext(
		ctx,
		sqlStatement,
		fmt.Sprintf("%d DAY", a.config.uploadRetentionTimeInDays.Load()),
		model.ExportedData,
		pq.Array(skipWorkspaceIDs),
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("error deleting uploads: %w", err)
	}
	return result.RowsAffected()
}
