package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	sourceJobTableName = whutils.WarehouseAsyncJobTable
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

func NewSource(db *sqlmw.DB, opts ...Opt) *Source {
	r := &Source{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (s *Source) Insert(ctx context.Context, sourceJobs []model.SourceJob) ([]int64, error) {
	var ids []int64

	err := (*repo)(s).WithTx(ctx, func(tx *sqlmw.Tx) error {
		stmt, err := tx.PrepareContext(
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
			return fmt.Errorf(`preparing statement: %w`, err)
		}
		defer func() { _ = stmt.Close() }()

		for _, sourceJob := range sourceJobs {
			var id int64
			err = stmt.QueryRowContext(
				ctx,
				sourceJob.SourceID,
				sourceJob.DestinationID,
				sourceJob.TableName,
				model.SourceJobStatusWaiting.String(),
				s.now(),
				s.now(),
				sourceJob.JobType.String(),
				sourceJob.WorkspaceID,
				sourceJob.Metadata,
			).Scan(&id)
			if err != nil {
				return fmt.Errorf(`executing: %w`, err)
			}

			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *Source) Reset(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
			status = $1
		WHERE
			status = $2 OR status = $3;
	`,
		model.SourceJobStatusWaiting.String(),
		model.SourceJobStatusExecuting.String(),
		model.SourceJobStatusFailed.String(),
	)
	if err != nil {
		return fmt.Errorf("executing: %w", err)
	}
	return nil
}

func (s *Source) GetToProcess(ctx context.Context, limit int64) ([]model.SourceJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			`+sourceJobColumns+`
		FROM
			`+sourceJobTableName+`
		WHERE
			status = $1 OR status = $2
		LIMIT $3;
	`,
		model.SourceJobStatusWaiting.String(),
		model.SourceJobStatusFailed.String(),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying: %w", err)
	}
	defer func() { _ = rows.Close() }()

	sourceJobs, err := scanSourceJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning source jobs: %w", err)
	}
	return sourceJobs, nil
}

func scanSourceJobs(rows *sqlmw.Rows) ([]model.SourceJob, error) {
	var sourceJobs []model.SourceJob
	for rows.Next() {
		var sourceJob model.SourceJob
		err := scanSourceJob(rows.Scan, &sourceJob)
		if err != nil {
			return nil, fmt.Errorf("scanning source job: %w", err)
		}
		sourceJobs = append(sourceJobs, sourceJob)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sourceJobs, nil
}

func scanSourceJob(scan scanFn, sourceJob *model.SourceJob) error {
	var errorRaw sql.NullString
	var jobType, status string

	if err := scan(
		&sourceJob.ID,
		&sourceJob.SourceID,
		&sourceJob.DestinationID,
		&status,
		&sourceJob.CreatedAt,
		&sourceJob.UpdatedAt,
		&sourceJob.TableName,
		&errorRaw,
		&jobType,
		&sourceJob.Metadata,
		&sourceJob.Attempts,
		&sourceJob.WorkspaceID,
	); err != nil {
		return fmt.Errorf("scanning row: %w", err)
	}

	sourceJobStatus, err := model.FromSourceJobStatus(status)
	if err != nil {
		return fmt.Errorf("getting sourceJobStatus %w", err)
	}
	sourceJobType, err := model.FromSourceJobType(jobType)
	if err != nil {
		return fmt.Errorf("getting sourceJobType: %w", err)
	}
	if errorRaw.Valid && errorRaw.String != "" {
		sourceJob.Error = errors.New(errorRaw.String)
	}

	sourceJob.Status = sourceJobStatus
	sourceJob.JobType = sourceJobType
	sourceJob.CreatedAt = sourceJob.CreatedAt.UTC()
	sourceJob.UpdatedAt = sourceJob.UpdatedAt.UTC()
	return nil
}

func (s *Source) GetByJobRunTaskRun(ctx context.Context, jobRunID, taskRunID string) (*model.SourceJob, error) {
	row := s.db.QueryRowContext(ctx, `
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
	err := scanSourceJob(row.Scan, &sourceJob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrSourcesJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scanning source job: %w", err)
	}
	return &sourceJob, nil
}

func (s *Source) OnUpdateSuccess(ctx context.Context, id int64) error {
	r, err := s.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
			status = $1,
			updated_at = $2
		WHERE
			id = $3;
`,
		model.SourceJobStatusSucceeded.String(),
		s.now(),
		id,
	)
	if err != nil {
		return fmt.Errorf("executing: %w", err)
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return model.ErrSourcesJobNotFound
	}
	return nil
}

func (s *Source) OnUpdateFailure(ctx context.Context, id int64, error error, maxAttempt int) error {
	r, err := s.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
 		  status = (
			CASE WHEN attempt > $1 THEN $2
			ELSE $3 END
		  ),
		  attempt = attempt + 1,
		  updated_at = $4,
		  error = $5
		WHERE
			id = $6;
`,
		maxAttempt,
		model.SourceJobStatusAborted.String(),
		model.SourceJobStatusFailed.String(),
		s.now(),
		error.Error(),
		id,
	)
	if err != nil {
		return fmt.Errorf("executing: %w", err)
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return model.ErrSourcesJobNotFound
	}
	return nil
}

func (s *Source) MarkExecuting(ctx context.Context, ids []int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE
			`+sourceJobTableName+`
		SET
			status = $1,
			updated_at = $2
		WHERE
			id = ANY($3);
`,
		model.SourceJobStatusExecuting,
		s.now(),
		pq.Array(ids),
	)
	if err != nil {
		return fmt.Errorf("executing: %w", err)
	}
	return nil
}
