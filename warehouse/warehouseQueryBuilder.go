package warehouse

import (
	"database/sql"
	"fmt"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func destinationRevisionIDs(d struct {
	sourceID           string
	destinationID      string
	startStagingFileID int64
	endStagingFileID   int64
}) (revisionIDs []string, err error) {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  DISTINCT metadata ->> 'destination_revision_id' AS destination_revision_id
		FROM
		  %s
		WHERE
		  metadata ->> 'destination_revision_id' IS NOT NULL
		  AND source_id = $1
		  AND destination_id = $2
		  AND id >= $3
		  AND id <= $4;
	`,
		warehouseutils.WarehouseStagingFilesTable,
	)
	rows, err := dbHandle.Query(sqlStatement, []interface{}{
		d.sourceID,
		d.destinationID,
		d.startStagingFileID,
		d.endStagingFileID,
	}...)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if err != nil && err != sql.ErrNoRows {
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
