package repo

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
)

// OrphanJobIDs returns the IDs of the jobs that are in executing state for more than the given interval.
func (n *Notifier) OrphanJobIDs(
	ctx context.Context,
	intervalInSeconds int,
) ([]int64, error) {
	rows, err := n.db.QueryContext(ctx, `
		UPDATE
          `+notifierTableName+`
		SET
		  status = $1,
		  updated_at = $2
		WHERE
		  id IN (
			SELECT
			  id
			FROM
              `+notifierTableName+`
			WHERE
			  status = $3
			AND last_exec_time <= NOW() - $4 * INTERVAL '1 SECOND'
		    FOR
			UPDATE
		  	SKIP LOCKED
	  	) RETURNING id;
`,
		model.Waiting,
		n.now(),
		model.Executing,
		intervalInSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("orphan jobs ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("orphan jobs ids: scanning: %w", err)
		}

		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orphan jobs ids: iterating over rows: %w", err)
	}

	return ids, nil
}
