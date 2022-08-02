package warehouse

import (
	"context"
	"database/sql"
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func distinctDestinationRevisionIdsFromStagingFiles(ctx context.Context, d struct {
	sourceID           string
	destinationID      string
	startStagingFileID int64
	endStagingFileID   int64
},
) (revisionIDs []string, err error) {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  DISTINCT metadata ->> 'destination_revision_id' AS destination_revision_id
		FROM
		  %s
		WHERE
          id >= $1
		  AND id <= $2
		  AND source_id = $3
		  AND destination_id = $4
          AND metadata ->> 'destination_revision_id' <> '';
	`,
		warehouseutils.WarehouseStagingFilesTable,
	)
	rows, err := dbHandle.QueryContext(ctx, sqlStatement, []interface{}{
		d.startStagingFileID,
		d.endStagingFileID,
		d.sourceID,
		d.destinationID,
	}...)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if err != nil {
		err = fmt.Errorf("error occurred while executing destination revisionID query %+v with err: %w", d, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var revisionID string
		err = rows.Scan(&revisionID)
		if err != nil {
			err = fmt.Errorf("error occurred while processing destination revisionID query %+v with err: %w", d, err)
			return
		}
		revisionIDs = append(revisionIDs, revisionID)
	}
	return
}
