package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	sourceJobTableName = warehouseutils.WarehouseAsyncJobTable
	sourceJobColumns   = `
		id,
		source_id,
		destination_id,
		status,
		created_at,
		updated_at,
		tablename,
		error,
		async_job_type,
		metadata,
		attempt,
		workspace_id
	`
)

type Source repo

func NewSource(db *sqlmiddleware.DB, opts ...Opt) *Source {
	r := &Source{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (repo *Source) Insert(
	ctx context.Context,
	sourceJobs []model.SourceJob,
) ([]int64, error) {
	txn, err := repo.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf(`begin transaction: %w`, err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	stmt, err := txn.PrepareContext(
		ctx, `
			INSERT INTO `+sourceJobTableName+` (
			  source_id, destination_id, tablename,
			  status, created_at, updated_at, async_job_type,
			  workspace_id, metadata
			)
			VALUES
			  ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;
`,
	)
	if err != nil {
		return nil, fmt.Errorf(`preparing statement: %w`, err)
	}
	defer func() { _ = stmt.Close() }()

	var ids []int64
	for _, sourceJob := range sourceJobs {
		var id int64
		err = stmt.QueryRowContext(
			ctx,
			sourceJob.SourceID,
			sourceJob.DestinationID,
			sourceJob.TableName,
			model.SourceJobStatusWaiting,
			repo.now(),
			repo.now(),
			sourceJob.Status,
			sourceJob.WorkspaceID,
			sourceJob.Metadata,
		).Scan(&id)
		if err != nil {
			return nil, fmt.Errorf(`executing: %w`, err)
		}

		ids = append(ids, id)
	}

	if err = txn.Commit(); err != nil {
		return nil, fmt.Errorf(`commiting: %w`, err)
	}

	return ids, nil
}

func (repo *Source) Reset(
	ctx context.Context,
) error {
	_, err := repo.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
			status = $1
		WHERE
			status = $2 OR status = $3;
	`,
		model.SourceJobStatusWaiting,
		model.SourceJobStatusExecuting,
		model.SourceJobStatusFailed,
	)
	if err != nil {
		return fmt.Errorf("resetting: %w", err)
	}
	return nil
}

func (repo *Source) GetToProcess(
	ctx context.Context,
	limit int64,
) ([]model.SourceJob, error) {
	rows, err := repo.db.QueryContext(ctx, `
		SELECT
			`+sourceJobColumns+`
		FROM
			`+sourceJobTableName+`
		WHERE
			status = $1 OR status = $2
		LIMIT $3;
	`,
		model.SourceJobStatusWaiting,
		model.SourceJobStatusFailed,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var sourceJobs []model.SourceJob
	for rows.Next() {
		var sourceJob model.SourceJob
		err := repo.scanSourceJob(rows.Scan, &sourceJob)
		if err != nil {
			return nil, fmt.Errorf("scanning: %w", err)
		}

		sourceJobs = append(sourceJobs, sourceJob)
	}

	return sourceJobs, nil
}

func (repo *Source) scanSourceJob(
	scan scanFn,
	sourceJob *model.SourceJob,
) error {
	var errorRaw sql.NullString
	var jobTypeRaw sql.NullString

	if err := scan(
		&sourceJob.ID,
		&sourceJob.SourceID,
		&sourceJob.DestinationID,
		&sourceJob.Status,
		&sourceJob.CreatedAt,
		&sourceJob.UpdatedAt,
		&sourceJob.TableName,
		&errorRaw,
		&jobTypeRaw,
		&sourceJob.Metadata,
		&sourceJob.Attempts,
		&sourceJob.WorkspaceID,
	); err != nil {
		return fmt.Errorf("scanning: %w", err)
	}

	if errorRaw.Valid {
		sourceJob.Error = errors.New(errorRaw.String)
	}
	if jobTypeRaw.Valid {
		switch jobTypeRaw.String {
		case model.DeleteByJobRunID:
			sourceJob.JobType = jobTypeRaw.String
		default:
			return fmt.Errorf("scanning: unknown job type: %s", jobTypeRaw.String)
		}
	} else {
		return fmt.Errorf("scanning: job type is null")
	}

	sourceJob.CreatedAt = sourceJob.CreatedAt.UTC()
	sourceJob.UpdatedAt = sourceJob.UpdatedAt.UTC()

	return nil
}

func (repo *Source) GetByJobRunTaskRun(
	ctx context.Context,
	jobRunID string,
	taskRunID string,
) (*model.SourceJob, error) {
	row := repo.db.QueryRowContext(ctx, `
		SELECT
			`+sourceJobColumns+`
		FROM
			`+sourceJobTableName+`
		WHERE
			metadata->>'job_run_id' = $1 AND
			metadata->>'task_run_id' = $2
		LIMIT 1;
	`,
		jobRunID,
		taskRunID,
	)

	var sourceJob model.SourceJob
	err := repo.scanSourceJob(row.Scan, &sourceJob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrSourcesJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scanning: %w", err)
	}
	return &sourceJob, nil
}

func (repo *Source) OnUpdateSuccess(
	ctx context.Context,
	id int64,
) error {
	_, err := repo.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
			status = $1,
			updated_at = $2
		WHERE
			id = $3;
`,
		model.SourceJobStatusSucceeded,
		repo.now(),
		id,
	)
	if err != nil {
		return fmt.Errorf("on update success: %w", err)
	}
	return nil
}

func (repo *Source) OnUpdateFailure(
	ctx context.Context,
	id int64,
	error error,
	maxAttempt int,
) error {
	_, err := repo.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
 		  status =(
			CASE WHEN attempt > $1 THEN `+model.SourceJobStatusAborted+`
			ELSE `+model.SourceJobStatusFailed+`) END
		  ),
		  attempt = attempt + 1,
		  updated_at = $2,
		  error = $3
		WHERE
			id = $3;
`,
		maxAttempt,
		repo.now(),
		error.Error(),
		id,
	)
	if err != nil {
		return fmt.Errorf("on update success: %w", err)
	}
	return nil
}
