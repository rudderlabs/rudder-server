//go:generate mockgen -source=table_upload.go -destination=../mocks/warehouse/mock_table_upload.go -package=mock_warehouse github.com/rudderlabs/rudder-server/warehouse TableUpload

package warehouse

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	tableUploadsUniqueConstraintName = "unique_table_upload_wh_upload"
	createTableUploadsBatchSize      = 500
)

type TableUpload interface {
	setStatus(status string) (err error)
	getTotalEvents() (int64, error)
	setError(status string, statusError error) (err error)
	getNumEvents() (total int64, err error)
}

type TableUploadImpl struct {
	uploadID  int64
	tableName string
}

type TableUploadFactory interface {
	New(uploadID int64, tableName string) TableUpload
}

type TableUploadFactoryImpl struct{}

func (tableUpload *TableUploadImpl) setStatus(status string) (err error) {
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

func (tableUpload *TableUploadImpl) getTotalEvents() (int64, error) {
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

func (tableUpload *TableUploadImpl) setError(status string, statusError error) (err error) {
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

func (tableUpload *TableUploadImpl) getNumEvents() (total int64, err error) {
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

func (*TableUploadFactoryImpl) New(uploadID int64, tableName string) TableUpload {
	return &TableUploadImpl{uploadID: uploadID, tableName: tableName}
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
