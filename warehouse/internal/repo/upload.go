package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const uploadsTableName = warehouseutils.WarehouseUploadsTable

type Uploads repo

func NewUploads(db *sql.DB) *Uploads {
	return &Uploads{
		db:  db,
		now: time.Now,
	}
}

func (uploads *Uploads) CreateWithStagingFiles(ctx context.Context, upload model.Upload, files []model.StagingFile) error {
	startJSONID := files[0].ID
	endJSONID := files[len(files)-1].ID

	var firstEventAt, lastEventAt time.Time
	if ok := files[0].FirstEventAt.IsZero(); !ok {
		firstEventAt = files[0].FirstEventAt
	}
	if ok := files[len(files)-1].LastEventAt.IsZero(); !ok {
		lastEventAt = files[len(files)-1].LastEventAt
	}

	now := timeutil.Now()
	metadataMap := map[string]interface{}{
		"use_rudder_storage": files[0].UseRudderStorage, // TODO: Since the use_rudder_storage is now being populated for both the staging and load files. Let's try to leverage it instead of hard coding it from the first staging file.
		"source_batch_id":    files[0].SourceBatchID,
		"source_task_id":     files[0].SourceTaskID,
		"source_task_run_id": files[0].SourceTaskRunID,
		"source_job_id":      files[0].SourceJobID,
		"source_job_run_id":  files[0].SourceJobRunID,
		"load_file_type":     warehouseutils.GetLoadFileType(upload.DestinationType),
		"nextRetryTime":      upload.NextRetryTime,
		"priority":           upload.Priority,
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		return err
	}
	row := uploads.db.QueryRowContext(
		ctx,

		`INSERT INTO `+uploadsTableName+` (
			source_id, namespace, workspace_id, destination_id,
			destination_type, start_staging_file_id,
			end_staging_file_id, start_load_file_id,
			end_load_file_id, status, schema,
			error, metadata, first_event_at,
			last_event_at, created_at, updated_at
		)
		VALUES
			(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17
			) RETURNING id;`,

		upload.SourceID,
		upload.Namespace,
		upload.WorkspaceID,
		upload.DestinationID,
		upload.DestinationType,
		startJSONID,
		endJSONID,
		0,
		0,
		upload.Status,
		"{}",
		"{}",
		metadata,
		firstEventAt,
		lastEventAt,
		now,
		now,
	)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		return err
	}

	return nil
}
