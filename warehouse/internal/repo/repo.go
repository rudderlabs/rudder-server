// Package repo provides database repository functionality for warehouse operations.
package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

// repo provides base repository functionality with database access and statistics tracking.
type repo struct {
	db           *sqlmiddleware.DB
	now          func() time.Time
	statsFactory stats.Stats
	repoType     string
}

// WithTx executes a function within a database transaction.
// Handles begin, commit, and rollback automatically.
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

// TimerStat returns a function that records the duration of a database action.
func (r *repo) TimerStat(action string, extraTags stats.Tags) func() {
	statName := "warehouse_repo_query_duration_seconds"
	tags := stats.Tags{"action": action, "repoType": r.getRepoType()}
	for k, v := range extraTags {
		tags[k] = v
	}
	return r.statsFactory.NewTaggedStat(statName, stats.TimerType, tags).RecordDuration()
}

func (r *repo) getRepoType() string {
	return r.repoType
}
