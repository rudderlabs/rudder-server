package warehouse

import (
	"fmt"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TableUploadT struct {
	uploadID  int64
	tableName string
}

func NewTableUpload(uploadID int64, tableName string) *TableUploadT {
	return &TableUploadT{uploadID: uploadID, tableName: tableName}
}

func getTotalEventsUploaded(uploadID int64) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`select sum(total_events) from wh_table_uploads where wh_upload_id=%d and status='%s'`, uploadID, ExportedData)
	err = dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total, err
}

func getNumEventsPerTableUpload(uploadID int64) (map[string]int, error) {
	eventsPerTableMap := make(map[string]int)

	sqlStatement := fmt.Sprintf(`select table_name, total_events from wh_table_uploads where wh_upload_id=%d and total_events > 0`, uploadID)
	rows, err := dbHandle.Query(sqlStatement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tName string
		var totalEvents int
		err := rows.Scan(&tName, &totalEvents)
		if err != nil {
			return nil, err
		}
		eventsPerTableMap[tName] = totalEvents
	}
	return eventsPerTableMap, nil
}

func areTableUploadsCreated(uploadID int64) bool {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE wh_upload_id=%d`, warehouseutils.WarehouseTableUploadsTable, uploadID)
	var count int
	err := dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	return count > 0
}

func createTableUploads(uploadID int64, tableNames []string) (err error) {

	//Using transactions for bulk copying
	txn, err := dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(warehouseutils.WarehouseTableUploadsTable, "wh_upload_id", "table_name", "status", "error", "created_at", "updated_at"))
	if err != nil {
		return
	}
	defer stmt.Close()

	now := timeutil.Now()
	for _, table := range tableNames {
		_, err = stmt.Exec(uploadID, table, "waiting", "{}", now, now)
		if err != nil {
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}
	return
}

func (tableUpload *TableUploadT) getStatus() (status string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT status from %s WHERE wh_upload_id=%d AND table_name='%s' ORDER BY id DESC`, warehouseutils.WarehouseTableUploadsTable, tableUpload.uploadID, tableUpload.tableName)
	err = dbHandle.QueryRow(sqlStatement).Scan(&status)
	return status, err
}

func (tableUpload *TableUploadT) setStatus(status string) (err error) {
	// set last_exec_time only if status is executing
	execValues := []interface{}{status, timeutil.Now(), tableUpload.uploadID, tableUpload.tableName}
	var lastExec string
	if status == TableUploadExecuting {
		// setting values using syntax $n since Exec can correctlt format time.Time strings
		lastExec = fmt.Sprintf(`, last_exec_time=$%d`, len(execValues)+1)
		execValues = append(execValues, timeutil.Now())
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 %s WHERE wh_upload_id=$3 AND table_name=$4`, warehouseutils.WarehouseTableUploadsTable, lastExec)
	pkgLogger.Debugf("[WH]: Setting table upload status: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, execValues...)
	return err
}

func (tableUpload *TableUploadT) setError(status string, statusError error) (err error) {
	tableName := tableUpload.tableName
	uploadID := tableUpload.uploadID
	pkgLogger.Errorf("[WH]: Failed uploading table-%s for upload-%v: %v", tableName, uploadID, statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2, error=$3 WHERE wh_upload_id=$4 AND table_name=$5`, warehouseutils.WarehouseTableUploadsTable)
	pkgLogger.Debugf("[WH]: Setting table upload error: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, status, timeutil.Now(), misc.QuoteLiteral(statusError.Error()), uploadID, tableName)
	return err
}

func (tableUpload *TableUploadT) hasBeenLoaded() (bool, error) {
	status, err := tableUpload.getStatus()
	if err != nil {
		return false, err
	}

	return (status == ExportedData), nil
}

func (tableUpload *TableUploadT) updateTableEventsCount(job *UploadJobT) (err error) {
	subQuery := fmt.Sprintf(`SELECT sum(total_events) as total from %[1]s right join (
		SELECT  staging_file_id, MAX(id) AS id FROM wh_load_files
		WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v)
		GROUP BY staging_file_id ) uniqueStagingFiles
		ON  wh_load_files.id = uniqueStagingFiles.id `,
		warehouseutils.WarehouseLoadFilesTable,
		job.warehouse.Source.ID,
		job.warehouse.Destination.ID,
		tableUpload.tableName,
		job.upload.StartLoadFileID,
		job.upload.EndLoadFileID,
		warehouseutils.WarehouseTableUploadsTable)

	sqlStatement := fmt.Sprintf(`update %[1]s set total_events = subquery.total FROM (%[2]s) AS subquery WHERE table_name = '%[3]s' AND wh_upload_id = %[4]d`,
		warehouseutils.WarehouseTableUploadsTable,
		subQuery,
		tableUpload.tableName,
		job.upload.ID)
	_, err = job.dbHandle.Exec(sqlStatement)
	return
}

func (tableUpload *TableUploadT) getNumEvents() (total int64, err error) {
	sqlStatement := fmt.Sprintf(`select total_events from wh_table_uploads where wh_upload_id=%d and table_name='%s'`, tableUpload.uploadID, tableUpload.tableName)
	err = dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total, err
}
