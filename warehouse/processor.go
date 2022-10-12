//go:generate mockgen -source=processor.go -destination=../mocks/warehouse/mock_processor.go -package=warehouse github.com/rudderlabs/rudder-server/warehouse Processor

package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/thoas/go-funk"
	"github.com/tidwall/gjson"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Processor interface {
	uploadToProcess(availableWorkers int, skipIdentifiers []string) ([]UploadJob, error)
	stagingFiles(w warehouseutils.Warehouse, startID, endID int64) ([]*StagingFileT, error)
}

type ProcessorImpl struct {
	warehouses                        *[]warehouseutils.Warehouse
	configSubscriberLock              *sync.RWMutex
	workspaceBySourceIDs              map[string]string
	workspaceBySourceIDsLock          *sync.RWMutex
	destType                          string
	dbHandle                          *sql.DB
	notifier                          pgnotifier.PgNotifier
	allowMultipleSourcesForJobsPickup bool
}

func NewProcessor(
	warehouses *[]warehouseutils.Warehouse,
	configSubscriberLock *sync.RWMutex,
	workspaceBySourceIDs map[string]string,
	workspaceBySourceIDsLock *sync.RWMutex,
	destType string,
	dbHandle *sql.DB,
	notifier pgnotifier.PgNotifier,
	allowMultipleSourcesForJobsPickup bool,
) Processor {
	return &ProcessorImpl{
		warehouses:                        warehouses,
		configSubscriberLock:              configSubscriberLock,
		workspaceBySourceIDs:              workspaceBySourceIDs,
		workspaceBySourceIDsLock:          workspaceBySourceIDsLock,
		destType:                          destType,
		dbHandle:                          dbHandle,
		notifier:                          notifier,
		allowMultipleSourcesForJobsPickup: allowMultipleSourcesForJobsPickup,
	}
}

func (s *ProcessorImpl) uploadToProcess(
	availableWorkers int,
	skipIdentifiers []string,
) ([]UploadJob, error) {
	var skipIdentifiersSQL string
	partitionIdentifierSQL := `destination_id, namespace`

	if len(skipIdentifiers) > 0 {
		skipIdentifiersSQL = `and ((destination_id || '_' || namespace)) != ALL($1)`
	}

	if s.allowMultipleSourcesForJobsPickup {
		if len(skipIdentifiers) > 0 {
			skipIdentifiersSQL = `and ((source_id || '_' || destination_id || '_' || namespace)) != ALL($1)`
		}
		partitionIdentifierSQL = fmt.Sprintf(`%s, %s`, "source_id", partitionIdentifierSQL)
	}

	sqlStatement := fmt.Sprintf(`
			SELECT
					id,
					status,
					schema,
					mergedSchema,
					namespace,
					workspace_id,
					source_id,
					destination_id,
					destination_type,
					start_staging_file_id,
					end_staging_file_id,
					start_load_file_id,
					end_load_file_id,
					error,
					metadata,
					timings->0 as firstTiming,
					timings->-1 as lastTiming,
					timings,
					COALESCE(metadata->>'priority', '100')::int,
					first_event_at,
					last_event_at
				FROM (
					SELECT
						ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COALESCE(metadata->>'priority', '100')::int ASC, id ASC) AS row_number,
						t.*
					FROM
						%s t
					WHERE
						t.destination_type = '%s' and t.in_progress=%t and t.status != '%s' and t.status != '%s' %s and COALESCE(metadata->>'nextRetryTime', now()::text)::timestamptz <= now()
				) grouped_uploads
				WHERE
					grouped_uploads.row_number = 1
				ORDER BY
					COALESCE(metadata->>'priority', '100')::int ASC, id ASC
				LIMIT %d;

		`,
		partitionIdentifierSQL,
		warehouseutils.WarehouseUploadsTable,
		s.destType,
		false,
		ExportedData,
		Aborted,
		skipIdentifiersSQL,
		availableWorkers,
	)

	var rows *sql.Rows
	var err error
	if len(skipIdentifiers) > 0 {
		rows, err = s.dbHandle.Query(
			sqlStatement,
			pq.Array(skipIdentifiers),
		)
	} else {
		rows, err = s.dbHandle.Query(
			sqlStatement,
		)
	}

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return []UploadJob{}, err
	}

	if errors.Is(err, sql.ErrNoRows) {
		return []UploadJob{}, nil
	}
	defer rows.Close()

	var uploadJobs []UploadJob
	for rows.Next() {
		var upload Upload
		var schema json.RawMessage
		var mergedSchema json.RawMessage
		var firstTiming sql.NullString
		var lastTiming sql.NullString
		var firstEventAt, lastEventAt sql.NullTime
		err := rows.Scan(
			&upload.ID,
			&upload.Status,
			&schema,
			&mergedSchema,
			&upload.Namespace,
			&upload.WorkspaceID,
			&upload.SourceID,
			&upload.DestinationID,
			&upload.DestinationType,
			&upload.StartStagingFileID,
			&upload.EndStagingFileID,
			&upload.StartLoadFileID,
			&upload.EndLoadFileID,
			&upload.Error,
			&upload.Metadata,
			&firstTiming,
			&lastTiming,
			&upload.TimingsObj,
			&upload.Priority,
			&firstEventAt,
			&lastEventAt,
		)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		upload.FirstEventAt = firstEventAt.Time
		upload.LastEventAt = lastEventAt.Time
		upload.UploadSchema = warehouseutils.JSONSchemaToMap(schema)
		upload.MergedSchema = warehouseutils.JSONSchemaToMap(mergedSchema)

		// TODO: replace gjson with jsoniter
		// cloud sources info
		upload.SourceBatchID = gjson.GetBytes(upload.Metadata, "source_batch_id").String()
		upload.SourceTaskID = gjson.GetBytes(upload.Metadata, "source_task_id").String()
		upload.SourceTaskRunID = gjson.GetBytes(upload.Metadata, "source_task_run_id").String()
		upload.SourceJobID = gjson.GetBytes(upload.Metadata, "source_job_id").String()
		upload.SourceJobRunID = gjson.GetBytes(upload.Metadata, "source_job_run_id").String()
		// load file type
		upload.LoadFileType = gjson.GetBytes(upload.Metadata, "load_file_type").String()

		_, upload.FirstAttemptAt = warehouseutils.TimingFromJSONString(firstTiming)
		var lastStatus string
		lastStatus, upload.LastAttemptAt = warehouseutils.TimingFromJSONString(lastTiming)
		upload.Attempts = gjson.Get(string(upload.Error), fmt.Sprintf(`%s.attempt`, lastStatus)).Int()

		if upload.WorkspaceID == "" {
			var ok bool
			s.workspaceBySourceIDsLock.Lock()
			upload.WorkspaceID, ok = s.workspaceBySourceIDs[upload.SourceID]
			s.workspaceBySourceIDsLock.Unlock()

			if !ok {
				pkgLogger.Warnf("could not find workspace id for source id: %s", upload.SourceID)
			}
		}

		s.configSubscriberLock.RLock()
		warehouse, ok := funk.Find(s.warehouses, func(w warehouseutils.Warehouse) bool {
			return w.Source.ID == upload.SourceID && w.Destination.ID == upload.DestinationID
		}).(warehouseutils.Warehouse)
		s.configSubscriberLock.RUnlock()

		upload.UseRudderStorage = warehouse.GetBoolDestinationConfig("useRudderStorage")

		if !ok {
			uploadJob := UploadJobImpl{
				upload:   &upload,
				dbHandle: s.dbHandle,
			}
			err := fmt.Errorf("unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)
			_, _ = uploadJob.setUploadError(err, Aborted)
			pkgLogger.Errorf("%v", err)
			continue
		}

		upload.SourceType = warehouse.Source.SourceDefinition.Name
		upload.SourceCategory = warehouse.Source.SourceDefinition.Category

		stagingFilesList, err := s.stagingFiles(warehouse, upload.StartStagingFileID, upload.EndStagingFileID)
		if err != nil {
			return nil, err
		}
		var stagingFileIDs []int64
		for _, stagingFile := range stagingFilesList {
			stagingFileIDs = append(stagingFileIDs, stagingFile.ID)
		}

		whManager, err := manager.New(s.destType)
		if err != nil {
			return nil, err
		}

		uploadJob := NewUploadJob(
			&upload,
			s.dbHandle,
			warehouse,
			whManager,
			stagingFilesList,
			stagingFileIDs,
			s.notifier,
			validations.NewDestinationValidator(),
		)

		uploadJobs = append(uploadJobs, uploadJob)
	}

	return uploadJobs, nil
}

func (wh *ProcessorImpl) stagingFiles(warehouse warehouseutils.Warehouse, startID, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  id,
		  location,
		  status,
		  metadata ->> 'time_window_year',
		  metadata ->> 'time_window_month',
		  metadata ->> 'time_window_day',
		  metadata ->> 'time_window_hour',
		  metadata ->> 'use_rudder_storage',
		  metadata ->> 'destination_revision_id'
		FROM
		  %[1]s ST
		WHERE
		  ST.id >= %[2]v
		  AND ST.id <= %[3]v
		  AND ST.source_id = '%[4]s'
		  AND ST.destination_id = '%[5]s'
		ORDER BY
		  id ASC;
`,
		warehouseutils.WarehouseStagingFilesTable,
		startID,
		endID,
		warehouse.Source.ID,
		warehouse.Destination.ID,
	)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		var timeWindowYear, timeWindowMonth, timeWindowDay, timeWindowHour sql.NullInt64
		var destinationRevisionID sql.NullString
		var UseRudderStorage sql.NullBool
		err := rows.Scan(
			&jsonUpload.ID,
			&jsonUpload.Location,
			&jsonUpload.Status,
			&timeWindowYear,
			&timeWindowMonth,
			&timeWindowDay,
			&timeWindowHour,
			&UseRudderStorage,
			&destinationRevisionID,
		)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.TimeWindow = time.Date(int(timeWindowYear.Int64), time.Month(timeWindowMonth.Int64), int(timeWindowDay.Int64), int(timeWindowHour.Int64), 0, 0, 0, time.UTC)
		jsonUpload.UseRudderStorage = UseRudderStorage.Bool
		jsonUpload.DestinationRevisionID = destinationRevisionID.String
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}
