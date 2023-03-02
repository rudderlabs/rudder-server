package warehouse

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	tableUploadsUniqueConstraintName = "unique_table_upload_wh_upload"
	createTableUploadsBatchSize      = 500
)

type TableUploadT struct {
	uploadID  int64
	tableName string
}

func NewTableUpload(uploadID int64, tableName string) *TableUploadT {
	return &TableUploadT{uploadID: uploadID, tableName: tableName}
}

func (job *UploadJobT) getTotalEventsUploaded(includeDiscards bool) (int64, error) {
	var total sql.NullInt64
	var discardsStatement string
	if !includeDiscards {
		discardsStatement = fmt.Sprintf(`and table_name != '%s'`, warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.DiscardsTable))
	}
	sqlStatement := fmt.Sprintf(`
		SELECT 
		  sum(total_events) 
		FROM 
		  wh_table_uploads 
		WHERE 
		  wh_upload_id = %d 
		  AND status = '%s' %s;
`,
		job.upload.ID,
		model.ExportedData,
		discardsStatement,
	)
	err := dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total.Int64, err
}

func areTableUploadsCreated(uploadID int64) bool {
	sqlStatement := fmt.Sprintf(`
		SELECT 
		  COUNT(*) 
		FROM 
		  %s 
		WHERE 
		  wh_upload_id = %d;
`,
		warehouseutils.WarehouseTableUploadsTable,
		uploadID,
	)
	var count int
	err := dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	return count > 0
}

func createTableUploadsForBatch(uploadID int64, tableNames []string) (err error) {
	columnsInInsert := []string{"wh_upload_id", "table_name", "status", "error", "created_at", "updated_at"}
	currentTime := timeutil.Now()
	valueReferences := make([]string, 0, len(tableNames))
	valueArgs := make([]interface{}, 0, len(tableNames)*len(columnsInInsert))
	for idx, tName := range tableNames {
		var valueRefsArr []string
		for index := idx*len(columnsInInsert) + 1; index <= (idx+1)*len(columnsInInsert); index++ {
			valueRefsArr = append(valueRefsArr, fmt.Sprintf(`$%d`, index))
		}
		valueReferences = append(valueReferences, fmt.Sprintf("(%s)", strings.Join(valueRefsArr, ",")))
		valueArgs = append(valueArgs, uploadID, tName, "waiting", "{}", currentTime, currentTime)
	}

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s (
		  wh_upload_id, table_name, status, 
		  error, created_at, updated_at
		) 
		VALUES 
		  %s ON CONFLICT ON CONSTRAINT %s DO NOTHING;
`,
		warehouseutils.WarehouseTableUploadsTable,
		strings.Join(valueReferences, ","),
		tableUploadsUniqueConstraintName,
	)

	_, err = dbHandle.Exec(sqlStatement, valueArgs...)
	if err != nil {
		pkgLogger.Errorf(`Failed created entries in wh_table_uploads for upload:%d : %v`, uploadID, err)
	}
	return err
}

func createTableUploads(uploadID int64, tableNames []string) (err error) {
	// we add table uploads to db in batches to avoid hitting postgres row insert limits
	for i := 0; i < len(tableNames); i += createTableUploadsBatchSize {
		j := i + createTableUploadsBatchSize
		if j > len(tableNames) {
			j = len(tableNames)
		}
		err = createTableUploadsForBatch(uploadID, tableNames[i:j])
		if err != nil {
			return
		}
	}
	return
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
	sqlStatement := fmt.Sprintf(`
		UPDATE 
		  %s 
		SET 
		  status = $1, 
		  updated_at = $2 %s 
		WHERE 
		  wh_upload_id = $3 
		  AND table_name = $4;
`,
		warehouseutils.WarehouseTableUploadsTable,
		lastExec,
	)
	pkgLogger.Debugf("[WH]: Setting table upload status: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, execValues...)
	return err
}

func (tableUpload *TableUploadT) getTotalEvents() (int64, error) {
	sqlStatement := fmt.Sprintf(`
		SELECT 
		  total_events 
		FROM 
		  %s 
		WHERE 
		  wh_upload_id = %d 
		  AND table_name = '%s';
`,
		warehouseutils.WarehouseTableUploadsTable,
		tableUpload.uploadID,
		tableUpload.tableName,
	)
	var total sql.NullInt64
	err := dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total.Int64, err
}

func (tableUpload *TableUploadT) setError(status string, statusError error) (err error) {
	tableName := tableUpload.tableName
	uploadID := tableUpload.uploadID
	pkgLogger.Errorf("[WH]: Failed uploading table-%s for upload-%v: %v", tableName, uploadID, statusError.Error())
	sqlStatement := fmt.Sprintf(`
		UPDATE 
		  %s 
		SET 
		  status = $1, 
		  updated_at = $2, 
		  error = $3 
		WHERE 
		  wh_upload_id = $4 
		  AND table_name = $5;
`,
		warehouseutils.WarehouseTableUploadsTable,
	)
	pkgLogger.Debugf("[WH]: Setting table upload error: %v", sqlStatement)
	_, err = dbHandle.Exec(
		sqlStatement,
		status,
		timeutil.Now(),
		misc.QuoteLiteral(statusError.Error()),
		uploadID,
		tableName,
	)
	return err
}

func (tableUpload *TableUploadT) updateTableEventsCount(job *UploadJobT) (err error) {
	subQuery := fmt.Sprintf(`
		WITH row_numbered_load_files as (
		  SELECT 
			total_events, 
			row_number() OVER (
			  PARTITION BY staging_file_id, 
			  table_name 
			  ORDER BY 
				id DESC
			) AS row_number 
		  FROM 
			%[1]s 
		  WHERE 
			staging_file_id IN (%[2]v) 
			AND table_name = '%[3]s'
		) 
		SELECT 
		  sum(total_events) as total 
		FROM 
		  row_numbered_load_files 
		WHERE 
		  row_number = 1
`,
		warehouseutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		tableUpload.tableName,
	)

	sqlStatement := fmt.Sprintf(`
		UPDATE 
		  %[1]s 
		SET 
		  total_events = subquery.total 
		FROM 
		  (%[2]s) AS subquery 
		WHERE 
		  table_name = '%[3]s' 
		  AND wh_upload_id = %[4]d;
`,
		warehouseutils.WarehouseTableUploadsTable,
		subQuery,
		tableUpload.tableName,
		job.upload.ID,
	)
	pkgLogger.Debugf(`Updating table event counts for table:%s in upload:%d, sqlStatement: %v`, tableUpload.tableName, job.upload.ID, sqlStatement)
	_, err = job.dbHandle.Exec(sqlStatement)
	return
}

func (tableUpload *TableUploadT) getNumEvents() (total int64, err error) {
	sqlStatement := fmt.Sprintf(`
		SELECT 
		  total_events 
		FROM 
		  wh_table_uploads 
		WHERE 
		  wh_upload_id = %d 
		  AND table_name = '%s';
`,
		tableUpload.uploadID,
		tableUpload.tableName,
	)
	err = dbHandle.QueryRow(sqlStatement).Scan(&total)
	return total, err
}
