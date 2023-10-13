package identity

import (
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("identity")
}

type WarehouseManager interface {
	DownloadIdentityRules(context.Context, *misc.GZipWriter) error
}

type Identity struct {
	warehouse        model.Warehouse
	db               *sqlmiddleware.DB
	uploader         warehouseutils.Uploader
	uploadID         int64
	warehouseManager WarehouseManager
	downloader       downloader.Downloader
	encodingFactory  *encoding.Factory
}

func New(warehouse model.Warehouse, db *sqlmiddleware.DB, uploader warehouseutils.Uploader, uploadID int64, warehouseManager WarehouseManager, loadFileDownloader downloader.Downloader, encodingFactory *encoding.Factory) *Identity {
	return &Identity{
		warehouse:        warehouse,
		db:               db,
		uploader:         uploader,
		uploadID:         uploadID,
		warehouseManager: warehouseManager,
		downloader:       loadFileDownloader,
		encodingFactory:  encodingFactory,
	}
}

func (idr *Identity) mergeRulesTable() string {
	return warehouseutils.IdentityMergeRulesTableName(idr.warehouse)
}

func (idr *Identity) mappingsTable() string {
	return warehouseutils.IdentityMappingsTableName(idr.warehouse)
}

func (idr *Identity) whMergeRulesTable() string {
	return warehouseutils.ToProviderCase(idr.warehouse.Destination.DestinationDefinition.Name, warehouseutils.IdentityMergeRulesTable)
}

func (idr *Identity) whMappingsTable() string {
	return warehouseutils.ToProviderCase(idr.warehouse.Destination.DestinationDefinition.Name, warehouseutils.IdentityMappingsTable)
}

func (idr *Identity) applyRule(txn *sqlmiddleware.Tx, ruleID int64, gzWriter *misc.GZipWriter) (totalRowsModified int, err error) {
	sqlStatement := fmt.Sprintf(`SELECT merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value FROM %s WHERE id=%v`, idr.mergeRulesTable(), ruleID)

	var prop1Val, prop2Val, prop1Type, prop2Type sql.NullString
	err = txn.QueryRow(sqlStatement).Scan(&prop1Type, &prop1Val, &prop2Type, &prop2Val)
	if err != nil {
		return
	}

	var rudderIDs []string
	var additionalClause string
	if prop2Val.Valid && prop2Type.Valid {
		additionalClause = fmt.Sprintf(`OR (merge_property_type='%s' AND merge_property_value=%s)`, prop2Type.String, misc.QuoteLiteral(prop2Val.String))
	}
	sqlStatement = fmt.Sprintf(`SELECT ARRAY_AGG(DISTINCT(rudder_id)) FROM %s WHERE (merge_property_type='%s' AND merge_property_value=%s) %s`, idr.mappingsTable(), prop1Type.String, misc.QuoteLiteral(prop1Val.String), additionalClause)
	pkgLogger.Debugf(`IDR: Fetching all rudder_id's corresponding to the merge_rule: %v`, sqlStatement)
	err = txn.QueryRow(sqlStatement).Scan(pq.Array(&rudderIDs))
	if err != nil {
		pkgLogger.Errorf("IDR: Error fetching all rudder_id's corresponding to the merge_rule: %v\nwith Error: %v", sqlStatement, err)
		return
	}

	currentTimeString := time.Now().Format(misc.RFC3339Milli)
	var rows [][]string

	// if no rudder_id is found with properties in merge_rule, create a new one
	// else if only one rudder_id is found with properties in merge_rule, use that rudder_id
	// else create a new rudder_id and assign it to all properties found with properties in the merge_rule
	if len(rudderIDs) <= 1 {
		// generate new one and assign to these two
		var rudderID string
		if len(rudderIDs) == 0 {
			rudderID = misc.FastUUID().String()
		} else {
			rudderID = rudderIDs[0]
		}
		row1 := []string{prop1Type.String, prop1Val.String, rudderID, currentTimeString}
		rows = append(rows, row1)
		row1Values := misc.SingleQuoteLiteralJoin(row1)

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, rudderID, currentTimeString}
			rows = append(rows, row2)
			row2Values = fmt.Sprintf(`, (%s)`, misc.SingleQuoteLiteralJoin(row2))
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, idr.mappingsTable(), row1Values, row2Values, warehouseutils.IdentityMappingsUniqueMappingConstraintName(idr.warehouse))
		pkgLogger.Debugf(`IDR: Inserting properties from merge_rule into mappings table: %v`, sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error inserting properties from merge_rule into mappings table: %v`, err)
			return
		}
	} else {
		// generate new one and update all
		newID := rudderIDs[0]
		row1 := []string{prop1Type.String, prop1Val.String, newID, currentTimeString}
		rows = append(rows, row1)
		row1Values := misc.SingleQuoteLiteralJoin(row1)

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, newID, currentTimeString}
			rows = append(rows, row2)
			row2Values = fmt.Sprintf(`, (%s)`, misc.SingleQuoteLiteralJoin(row2))
		}

		quotedRudderIDs := misc.SingleQuoteLiteralJoin(rudderIDs)
		sqlStatement := fmt.Sprintf(`SELECT merge_property_type, merge_property_value FROM %s WHERE rudder_id IN (%v)`, idr.mappingsTable(), quotedRudderIDs)
		pkgLogger.Debugf(`IDR: Get all merge properties from mapping table with rudder_id's %v: %v`, quotedRudderIDs, sqlStatement)
		var tableRows *sqlmiddleware.Rows
		tableRows, err = txn.Query(sqlStatement)
		if err != nil {
			return
		}
		defer func() { _ = tableRows.Close() }()

		for tableRows.Next() {
			var mergePropType, mergePropVal string
			err = tableRows.Scan(&mergePropType, &mergePropVal)
			if err != nil {
				return
			}
			row := []string{mergePropType, mergePropVal, newID, currentTimeString}
			rows = append(rows, row)
		}
		if err = tableRows.Err(); err != nil {
			return
		}

		sqlStatement = fmt.Sprintf(`UPDATE %s SET rudder_id='%s', updated_at='%s' WHERE rudder_id IN (%v)`, idr.mappingsTable(), newID, currentTimeString, misc.SingleQuoteLiteralJoin(rudderIDs[1:]))
		var res sql.Result
		res, err = txn.Exec(sqlStatement)
		if err != nil {
			return
		}
		affectedRowCount, _ := res.RowsAffected()
		pkgLogger.Debugf(`IDR: Updated rudder_id for all properties in mapping table. Updated %v rows: %v `, affectedRowCount, sqlStatement)

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, idr.mappingsTable(), row1Values, row2Values, warehouseutils.IdentityMappingsUniqueMappingConstraintName(idr.warehouse))
		pkgLogger.Debugf(`IDR: Insert new mappings into %s: %v`, idr.mappingsTable(), sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			return
		}
	}
	columnNames := []string{"merge_property_type", "merge_property_value", "rudder_id", "updated_at"}
	for _, row := range rows {
		eventLoader := idr.encodingFactory.NewEventLoader(gzWriter, idr.uploader.GetLoadFileType(), idr.warehouse.Type)
		// TODO : support add row for parquet loader
		eventLoader.AddRow(columnNames, row)
		data, _ := eventLoader.WriteToString()
		_ = gzWriter.WriteGZ(data)
	}

	return len(rows), err
}

func (idr *Identity) addRules(txn *sqlmiddleware.Tx, loadFileNames []string, gzWriter *misc.GZipWriter) (ids []int64, err error) {
	// add rules from load files into temp table
	// use original table to delete redundant ones from temp table
	// insert from temp table into original table
	mergeRulesStagingTable := fmt.Sprintf(`rudder_identity_merge_rules_staging_%s`, warehouseutils.RandHex())
	sqlStatement := fmt.Sprintf(`CREATE TEMP TABLE %s
						ON COMMIT DROP
						AS SELECT * FROM %s
						WITH NO DATA;`, mergeRulesStagingTable, idr.mergeRulesTable())

	pkgLogger.Infof(`IDR: Creating temp table %s in postgres for loading %s: %v`, mergeRulesStagingTable, idr.mergeRulesTable(), sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error creating temp table %s in postgres: %v`, mergeRulesStagingTable, err)
		return
	}

	sortedColumnNames := []string{"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value", "id"}
	stmt, err := txn.Prepare(pq.CopyIn(mergeRulesStagingTable, sortedColumnNames...))
	if err != nil {
		pkgLogger.Errorf(`IDR: Error starting bulk copy using CopyIn: %v`, err)
		return
	}

	var rowID int

	for _, loadFileName := range loadFileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(loadFileName)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error opening downloaded load file at %s: %v`, loadFileName, err)
			return
		}
		defer gzipFile.Close()

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error reading downloaded load file at %s: %v`, loadFileName, err)
			return
		}
		defer gzipReader.Close()

		eventReader := idr.encodingFactory.NewEventReader(gzipReader, idr.warehouse.Type)
		columnNames := []string{"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value"}
		for {
			var record []string
			record, err = eventReader.Read(columnNames)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				pkgLogger.Errorf("IDR: Error while reading merge rule file %s for loading in staging table locally:%s: %v", loadFileName, mergeRulesStagingTable, err)
				return
			}
			var recordInterface [5]interface{}
			for idx, value := range record {
				if strings.TrimSpace(value) != "" {
					recordInterface[idx] = value
				}
			}
			// add rowID which allows us to insert in same order from staging to original merge _rules table
			rowID++
			recordInterface[4] = rowID
			_, err = stmt.Exec(recordInterface[:]...)
			if err != nil {
				pkgLogger.Errorf("IDR: Error while adding rowID to merge_rules table: %v", err)
				return
			}
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		pkgLogger.Errorf(`IDR: Error bulk copy using CopyIn: %v for uploadID: %v`, err, idr.uploadID)
		return
	}

	sqlStatement = fmt.Sprintf(`DELETE FROM %s AS staging
					USING %s original
					WHERE
					(original.merge_property_1_type = staging.merge_property_1_type)
					AND
					(original.merge_property_1_value = staging.merge_property_1_value)
					AND
					(original.merge_property_2_type = staging.merge_property_2_type)
					AND
					(original.merge_property_2_value = staging.merge_property_2_value)`,
		mergeRulesStagingTable, idr.mergeRulesTable())
	pkgLogger.Infof(`IDR: Deleting from staging table %s using %s: %v`, mergeRulesStagingTable, idr.mergeRulesTable(), sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error deleting from staging table %s using %s: %v`, mergeRulesStagingTable, idr.mergeRulesTable(), err)
		return
	}

	// write merge rules to file to be uploaded to warehouse in later steps
	err = idr.writeTableToFile(mergeRulesStagingTable, txn, gzWriter)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error writing staging table %s to file: %v`, mergeRulesStagingTable, err)
		return
	}

	// select and insert distinct combination of merge rules and sort them by order in which they were added
	sqlStatement = fmt.Sprintf(`INSERT INTO %s
						(merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value)
						SELECT merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value FROM
						(
							SELECT DISTINCT ON (
								merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value
							) id, merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value
							FROM %s
						) t
		 				ORDER BY id ASC RETURNING id`, idr.mergeRulesTable(), mergeRulesStagingTable)
	pkgLogger.Infof(`IDR: Inserting into %s from %s: %v`, idr.mergeRulesTable(), mergeRulesStagingTable, sqlStatement)
	rows, err := txn.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error inserting into %s from %s: %v`, idr.mergeRulesTable(), mergeRulesStagingTable, err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error reading id from inserted column in %s from %s: %v`, idr.mergeRulesTable(), mergeRulesStagingTable, err)
			return
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		pkgLogger.Errorf(`IDR: Error reading rows from %s from %s: %v`, idr.mergeRulesTable(), mergeRulesStagingTable, err)
		return
	}
	pkgLogger.Debugf(`IDR: Number of merge rules inserted for uploadID %v : %v`, idr.uploadID, len(ids))
	return ids, nil
}

func (idr *Identity) writeTableToFile(tableName string, txn *sqlmiddleware.Tx, gzWriter *misc.GZipWriter) (err error) {
	batchSize := int64(500)
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
	var totalRows int64
	err = txn.QueryRow(sqlStatement).Scan(&totalRows)
	if err != nil {
		return
	}

	var offset int64
	for {
		sqlStatement = fmt.Sprintf(`SELECT merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value FROM %s LIMIT %d OFFSET %d`, tableName, batchSize, offset)

		var rows *sqlmiddleware.Rows
		rows, err = txn.Query(sqlStatement)
		if err != nil {
			return
		}
		defer func() { _ = rows.Close() }()
		columnNames := []string{"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value"}
		for rows.Next() {
			var rowData []string
			eventLoader := idr.encodingFactory.NewEventLoader(gzWriter, idr.uploader.GetLoadFileType(), idr.warehouse.Type)
			var prop1Val, prop2Val, prop1Type, prop2Type sql.NullString
			err = rows.Scan(
				&prop1Type,
				&prop1Val,
				&prop2Type,
				&prop2Val,
			)
			if err != nil {
				return
			}
			rowData = append(rowData, prop1Type.String, prop1Val.String, prop2Type.String, prop2Val.String)
			for i, columnName := range columnNames {
				// TODO : use proper column type here
				eventLoader.AddColumn(columnName, "", rowData[i])
			}
			rowString, _ := eventLoader.WriteToString()
			_ = gzWriter.WriteGZ(rowString)
		}
		if err = rows.Err(); err != nil {
			return
		}

		offset += batchSize
		if offset >= totalRows {
			break
		}
	}
	return
}

func (idr *Identity) uploadFile(ctx context.Context, filePath string, txn *sqlmiddleware.Tx, tableName string, totalRecords int) (err error) {
	outputFile, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	storageProvider := warehouseutils.ObjectStorageType(idr.warehouse.Destination.DestinationDefinition.Name, idr.warehouse.Destination.Config, idr.uploader.UseRudderStorage())
	uploader, err := filemanager.New(&filemanager.Settings{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           idr.warehouse.Destination.Config,
			UseRudderStorage: idr.uploader.UseRudderStorage(),
		}),
	})
	if err != nil {
		pkgLogger.Errorf("IDR: Error in creating a file manager for :%s: , %v", idr.warehouse.Destination.DestinationDefinition.Name, err)
		return err
	}
	output, err := uploader.Upload(ctx, outputFile, config.GetString("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, idr.warehouse.Source.ID, tableName)
	if err != nil {
		return
	}

	sqlStatement := fmt.Sprintf(`UPDATE %s SET location='%s', total_events=%d WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, output.Location, totalRecords, idr.uploadID, warehouseutils.ToProviderCase(idr.warehouse.Destination.DestinationDefinition.Name, tableName))
	pkgLogger.Infof(`IDR: Updating load file location for table: %s: %s `, tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error updating load file location for table: %s: %v`, tableName, err)
	}
	return err
}

func (idr *Identity) createTempGzFile(dirName string) (gzWriter misc.GZipWriter, path string) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	fileExtension := warehouseutils.GetTempFileExtension(idr.warehouse.Type)
	path = tmpDirPath + dirName + fmt.Sprintf(`%s_%s/%v/`, idr.warehouse.Destination.DestinationDefinition.Name, idr.warehouse.Destination.ID, idr.uploadID) + misc.FastUUID().String() + "." + fileExtension
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err = misc.CreateGZ(path)
	if err != nil {
		panic(err)
	}
	return
}

func (idr *Identity) processMergeRules(ctx context.Context, fileNames []string) (err error) {
	txn, err := idr.db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}

	// START: Add new merge rules to local pg table and also to file
	mergeRulesFileGzWriter, mergeRulesFilePath := idr.createTempGzFile(fmt.Sprintf(`/%s/`, misc.RudderIdentityMergeRulesTmp))
	defer misc.RemoveFilePaths(mergeRulesFilePath)

	ruleIDs, err := idr.addRules(txn, fileNames, &mergeRulesFileGzWriter)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error adding rules to %s: %v`, idr.mergeRulesTable(), err)
		return
	}
	_ = mergeRulesFileGzWriter.CloseGZ()
	pkgLogger.Infof(`IDR: Added %d unique rules to %s and file`, len(ruleIDs), idr.mergeRulesTable())
	// END: Add new merge rules to local pg table and also to file

	// START: Add new/changed identity mappings to local pg table and also to file
	mappingsFileGzWriter, mappingsFilePath := idr.createTempGzFile(fmt.Sprintf(`/%s/`, misc.RudderIdentityMappingsTmp))
	defer misc.RemoveFilePaths(mappingsFilePath)
	var totalMappingRecords int
	for idx, ruleID := range ruleIDs {
		var count int
		count, err = idr.applyRule(txn, ruleID, &mappingsFileGzWriter)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error applying rule %d in %s: %v`, ruleID, idr.mergeRulesTable(), err)
			return
		}
		totalMappingRecords += count
		if idx%1000 == 0 {
			pkgLogger.Infof(
				`IDR: Applied %d rules out of %d. Total Mapping records added: %d. Namespace: %s, Destination: %s:%s`,
				idx+1,
				len(ruleIDs),
				totalMappingRecords,
				idr.warehouse.Namespace,
				idr.warehouse.Type,
				idr.warehouse.Destination.ID,
			)
		}
	}
	_ = mappingsFileGzWriter.CloseGZ()
	// END: Add new/changed identity mappings to local pg table and also to file

	// upload new merge rules to object storage
	err = idr.uploadFile(ctx, mergeRulesFilePath, txn, idr.whMergeRulesTable(), len(ruleIDs))
	if err != nil {
		pkgLogger.Errorf(`IDR: Error uploading load file for %s at %s to object storage: %v`, idr.mergeRulesTable(), mergeRulesFilePath, err)
		return
	}

	// upload new/changed identity mappings to object storage
	err = idr.uploadFile(ctx, mappingsFilePath, txn, idr.whMappingsTable(), totalMappingRecords)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error uploading load file for %s at %s to object storage: %v`, mappingsFilePath, mergeRulesFilePath, err)
		return
	}

	err = txn.Commit()
	if err != nil {
		pkgLogger.Errorf(`IDR: Error committing transaction: %v`, err)
		return
	}
	return
}

// Resolve does the below things in a single pg txn
// 1. Fetch all new merge rules added in the upload
// 2. Append to local identity merge rules table
// 3. Apply each merge rule and update local identity mapping table
// 4. Upload the diff of each table to load files for both tables
func (idr *Identity) Resolve(ctx context.Context) (err error) {
	var loadFileNames []string
	defer misc.RemoveFilePaths(loadFileNames...)
	loadFileNames, err = idr.downloader.Download(ctx, idr.whMergeRulesTable())
	if err != nil {
		pkgLogger.Errorf(`IDR: Failed to download load files for %s with error: %v`, idr.mergeRulesTable(), err)
		return
	}

	return idr.processMergeRules(ctx, loadFileNames)
}

func (idr *Identity) ResolveHistoricIdentities(ctx context.Context) (err error) {
	var loadFileNames []string
	defer misc.RemoveFilePaths(loadFileNames...)
	gzWriter, path := idr.createTempGzFile(fmt.Sprintf(`/%s/`, misc.RudderIdentityMergeRulesTmp))
	err = idr.warehouseManager.DownloadIdentityRules(ctx, &gzWriter)
	_ = gzWriter.CloseGZ()
	if err != nil {
		pkgLogger.Errorf(`IDR: Failed to download identity information from warehouse with error: %v`, err)
		return
	}
	loadFileNames = append(loadFileNames, path)

	return idr.processMergeRules(ctx, loadFileNames)
}
