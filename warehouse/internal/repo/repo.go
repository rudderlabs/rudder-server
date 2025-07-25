package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

type repo struct {
	db           *sqlmiddleware.DB
	statsFactory stats.Stats

	now func() time.Time
}

func (r *repo) WithTx(ctx context.Context, f func(tx *sqlmiddleware.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := f(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("rollback transaction for %w: %w", err, rollbackErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *repo) DeferActionTimer(action string, extraTags stats.Tags) func() {
	// Define a single stat with a more descriptive name
	statName := "warehouse_repo_query_duration_seconds"

	// Use action as a tag
	tags := stats.Tags{"action": action, "repoType": r.getRepoType()}

	// Merge any additional tags
	for k, v := range extraTags {
		tags[k] = v
	}

	// Record the duration with the consolidated stat and action tag
	return r.statsFactory.NewTaggedStat(statName, stats.TimerType, tags).RecordDuration()
}

func (r *repo) DeferActionTimerSimple(action string) func() {
	// Define a single stat with a more descriptive name
	statName := "warehouse_repo_query_duration_seconds"

	// Use action and repoType as tags
	tags := stats.Tags{"action": action, "repoType": r.getRepoType()}

	// Record the duration with the consolidated stat and action tag
	return r.statsFactory.NewTaggedStat(statName, stats.TimerType, tags).RecordDuration()
}

func (r *repo) getRepoType() string {
	switch any(r).(type) {
	case *StagingFiles:
		return stagingTableName
	case *WHSchema:
		return whSchemaTableName
	case *Uploads:
		return uploadsTableName
	case *LoadFiles:
		return loadTableName
	case *TableUploads:
		return tableUploadTableName
	case *Source:
		return sourceJobTableName
	}
	return "repo"
}
