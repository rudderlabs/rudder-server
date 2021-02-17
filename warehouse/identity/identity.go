package identity

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("identity")
}

type WarehouseManager interface {
	DownloadIdentityRules(*misc.GZipWriter) error
}

type HandleT struct {
	Warehouse        warehouseutils.WarehouseT
	DbHandle         *sql.DB
	Uploader         warehouseutils.UploaderI
	UploadID         int64
	WarehouseManager WarehouseManager
}

func (idr *HandleT) mergeRulesTable() string {
	return warehouseutils.IdentityMergeRulesTableName(idr.Warehouse)
}

func (idr *HandleT) mappingsTable() string {
	return warehouseutils.IdentityMappingsTableName(idr.Warehouse)
}

func (idr *HandleT) whMergeRulesTable() string {
	return warehouseutils.ToProviderCase(idr.Warehouse.Destination.DestinationDefinition.Name, warehouseutils.IdentityMergeRulesTable)
}

func (idr *HandleT) whMappingsTable() string {
	return warehouseutils.ToProviderCase(idr.Warehouse.Destination.DestinationDefinition.Name, warehouseutils.IdentityMappingsTable)
}

func (idr *HandleT) applyRule(txn *sql.Tx, ruleID int64, gzWriter *misc.GZipWriter) (totalRowsModified int, err error) {
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
	var buff bytes.Buffer
	csvWriter := csv.NewWriter(&buff)
	var csvRows [][]string

	// if no rudder_id is found with properties in merge_rule, create a new one
	// else if only one rudder_id is found with properties in merge_rule, use that rudder_id
	// else create a new rudder_id and assign it to all properties found with properties in the merge_rule
	if len(rudderIDs) <= 1 {
		// generate new one and assign to these two
		var rudderID string
		if len(rudderIDs) == 0 {
			rudderID = uuid.NewV4().String()
		} else {
			rudderID = rudderIDs[0]
		}
		row1 := []string{prop1Type.String, prop1Val.String, rudderID, currentTimeString}
		csvRows = append(csvRows, row1)
		row1Values := misc.SingleQuoteLiteralJoin(row1)

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, rudderID, currentTimeString}
			csvRows = append(csvRows, row2)
			row2Values = fmt.Sprintf(`, (%s)`, misc.SingleQuoteLiteralJoin(row2))
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, idr.mappingsTable(), row1Values, row2Values, warehouseutils.IdentityMappingsUniqueMappingConstraintName(idr.Warehouse))
		pkgLogger.Debugf(`IDR: Inserting properties from merge_rule into mappings table: %v`, sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error inserting properties from merge_rule into mappings table: %v`, err)
			return
		}
	} else {
		// generate new one and update all
		newID := uuid.NewV4().String()
		row1 := []string{prop1Type.String, prop1Val.String, newID, currentTimeString}
		csvRows = append(csvRows, row1)
		row1Values := misc.SingleQuoteLiteralJoin(row1)

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, newID, currentTimeString}
			csvRows = append(csvRows, row2)
			row2Values = fmt.Sprintf(`, (%s)`, misc.SingleQuoteLiteralJoin(row2))
		}

		quotedRudderIDs := misc.SingleQuoteLiteralJoin(rudderIDs)
		sqlStatement := fmt.Sprintf(`SELECT merge_property_type, merge_property_value FROM %s WHERE rudder_id IN (%v)`, idr.mappingsTable(), quotedRudderIDs)
		pkgLogger.Debugf(`IDR: Get all merge properties from mapping table with rudder_id's %v: %v`, quotedRudderIDs, sqlStatement)
		var rows *sql.Rows
		rows, err = txn.Query(sqlStatement)
		if err != nil {
			return
		}

		for rows.Next() {
			var mergePropType, mergePropVal string
			err = rows.Scan(&mergePropType, &mergePropVal)
			if err != nil {
				return
			}
			csvRow := []string{mergePropType, mergePropVal, newID, currentTimeString}
			csvRows = append(csvRows, csvRow)
		}

		sqlStatement = fmt.Sprintf(`UPDATE %s SET rudder_id='%s', updated_at='%s' WHERE rudder_id IN (%v)`, idr.mappingsTable(), newID, currentTimeString, quotedRudderIDs)
		pkgLogger.Debugf(`IDR: Update rudder_id for all properties in mapping table with rudder_id's %v: %v`, quotedRudderIDs, sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			return
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, idr.mappingsTable(), row1Values, row2Values, warehouseutils.IdentityMappingsUniqueMappingConstraintName(idr.Warehouse))
		pkgLogger.Debugf(`IDR: Insert new mappings into %s: %v`, idr.mappingsTable(), sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			return
		}
	}
	for _, csvRow := range csvRows {
		csvWriter.Write(csvRow)
	}
	csvWriter.Flush()
	gzWriter.WriteGZ(buff.String())
	return len(csvRows), err
}

func (idr *HandleT) addRules(txn *sql.Tx, loadFileNames []string, gzWriter *misc.GZipWriter) (ids []int64, err error) {
	// add rules from load files into temp table
	// use original table to delete redundant ones from temp table
	// insert from temp table into original table
	mergeRulesStagingTable := fmt.Sprintf(`rudder_identity_merge_rules_staging_%s`, strings.Replace(uuid.NewV4().String(), "-", "", -1))
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
			pkgLogger.Errorf(`IDR: Error opeining downloaded load file at %s: %v`, loadFileName, err)
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

		csvReader := csv.NewReader(gzipReader)
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					pkgLogger.Errorf("IDR: Error while reading csv file for loading in staging table locally:%s: %v", mergeRulesStagingTable, err)
					return
				}
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
		pkgLogger.Errorf(`IDR: Error bulk copy using CopyIn: %v`, err)
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
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			pkgLogger.Errorf(`IDR: Error reading id from inserted column in %s from %s: %v`, idr.mergeRulesTable(), mergeRulesStagingTable, err)
			return
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (idr *HandleT) writeTableToFile(tableName string, txn *sql.Tx, gzWriter *misc.GZipWriter) (err error) {
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

		var rows *sql.Rows
		rows, err = txn.Query(sqlStatement)
		if err != nil {
			return
		}

		for rows.Next() {
			var buff bytes.Buffer
			csvWriter := csv.NewWriter(&buff)
			var csvRow []string

			var prop1Val, prop2Val, prop1Type, prop2Type sql.NullString
			err = rows.Scan(&prop1Type, &prop1Val, &prop2Type, &prop2Val)
			if err != nil {
				return
			}
			csvRow = append(csvRow, prop1Type.String, prop1Val.String, prop2Type.String, prop2Val.String)
			csvWriter.Write(csvRow)
			csvWriter.Flush()
			gzWriter.WriteGZ(buff.String())
		}

		offset += batchSize
		if offset >= totalRows {
			break
		}
	}
	return
}

func (idr *HandleT) downloadLoadFiles(tableName string) ([]string, error) {
	objectLocations := idr.Uploader.GetLoadFileLocations(warehouseutils.GetLoadFileLocationsOptionsT{Table: tableName})
	var fileNames []string
	for _, objectLocation := range objectLocations {
		objectName, err := warehouseutils.GetObjectName(objectLocation, idr.Warehouse.Destination.Config, warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config))
		if err != nil {
			pkgLogger.Errorf("IDR: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			pkgLogger.Errorf("IDR: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		objectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.ID, time.Now().Unix()) + objectName
		err = os.MkdirAll(filepath.Dir(objectPath), os.ModePerm)
		if err != nil {
			pkgLogger.Errorf("IDR: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
			return nil, err
		}
		objectFile, err := os.Create(objectPath)
		if err != nil {
			pkgLogger.Errorf("IDR: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config),
			Config:   idr.Warehouse.Destination.Config,
		})
		if err != nil {
			pkgLogger.Errorf("IDR: Error in creating a file manager for :%s: , %v", idr.Warehouse.Destination.DestinationDefinition.Name, err)
			return nil, err
		}
		err = downloader.Download(objectFile, objectName)
		if err != nil {
			pkgLogger.Errorf("IDR: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			pkgLogger.Errorf("IDR: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil
}

func (idr *HandleT) uploadFile(filePath string, txn *sql.Tx, tableName string, totalRecords int) (err error) {
	outputFile, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config),
		Config:   idr.Warehouse.Destination.Config,
	})
	if err != nil {
		pkgLogger.Errorf("IDR: Error in creating a file manager for :%s: , %v", idr.Warehouse.Destination.DestinationDefinition.Name, err)
		return err
	}
	output, err := uploader.Upload(outputFile, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, idr.Warehouse.Source.ID, tableName)
	if err != nil {
		return
	}

	sqlStatement := fmt.Sprintf(`UPDATE %s SET location='%s', total_events=%d WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, output.Location, totalRecords, idr.UploadID, warehouseutils.ToProviderCase(idr.Warehouse.Destination.DestinationDefinition.Name, tableName))
	pkgLogger.Infof(`IDR: Updating load file location for table: %s: %s `, tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error updating load file location for table: %s: %v`, tableName, err)
	}
	return err
}

func (idr *HandleT) createTempGzFile(dirName string) (gzWriter misc.GZipWriter, path string) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path = tmpDirPath + dirName + fmt.Sprintf(`%s_%s/%v/`, idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.ID, idr.UploadID) + uuid.NewV4().String() + ".csv.gz"
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

func (idr *HandleT) processMergeRules(fileNames []string) (err error) {

	txn, err := idr.DbHandle.Begin()
	if err != nil {
		panic(err)
	}

	// START: Add new merge rules to local pg table and also to file
	mergeRulesFileGzWriter, mergeRulesFilePath := idr.createTempGzFile(`/rudder-identity-merge-rules-tmp/`)
	defer os.Remove(mergeRulesFilePath)

	ruleIDs, err := idr.addRules(txn, fileNames, &mergeRulesFileGzWriter)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error adding rules to %s: %v`, idr.mergeRulesTable(), err)
		return
	}
	mergeRulesFileGzWriter.CloseGZ()
	pkgLogger.Infof(`IDR: Added %d unique rules to %s and file`, len(ruleIDs), idr.mergeRulesTable())
	// END: Add new merge rules to local pg table and also to file

	// START: Add new/changed identity mappings to local pg table and also to file
	mappingsFileGzWriter, mappingsFilePath := idr.createTempGzFile(`/rudder-identity-mappings-tmp/`)
	defer os.Remove(mappingsFilePath)
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
			pkgLogger.Infof(`IDR: Applied %d rules out of %d. Total Mapping records added: %d. Namepsace: %s, Destination: %s:%s`, idx+1, len(ruleIDs), totalMappingRecords, idr.Warehouse.Namespace, idr.Warehouse.Type, idr.Warehouse.Destination.ID)
		}
	}
	mappingsFileGzWriter.CloseGZ()
	// END: Add new/changed identity mappings to local pg table and also to file

	// upload new merge rules to object storage
	err = idr.uploadFile(mergeRulesFilePath, txn, idr.whMergeRulesTable(), len(ruleIDs))
	if err != nil {
		pkgLogger.Errorf(`IDR: Error uploading load file for %s at %s to object storage: %v`, idr.mergeRulesTable(), mergeRulesFilePath, err)
		return
	}

	// upload new/changed identity mappings to object storage
	err = idr.uploadFile(mappingsFilePath, txn, idr.whMappingsTable(), totalMappingRecords)
	if err != nil {
		pkgLogger.Errorf(`IDR: Error uploading load file for %s at %s to object storage: %v`, mappingsFilePath, mergeRulesFilePath, err)
		return
	}

	err = txn.Commit()
	if err != nil {
		pkgLogger.Errorf(`IDR: Error commiting transaction: %v`, err)
		return
	}
	return
}

// Resolve does the below things in a single pg txn
// 1. Fetch all new merge rules added in the upload
// 2. Append to local identity merge rules table
// 3. Apply each merge rule and update local identity mapping table
// 4. Upload the diff of each table to load files for both tables
func (idr *HandleT) Resolve() (err error) {

	var loadFileNames []string
	defer misc.RemoveFilePaths(loadFileNames...)
	loadFileNames, err = idr.downloadLoadFiles(idr.whMergeRulesTable())
	if err != nil {
		pkgLogger.Errorf(`IDR: Failed to download load files for %s with error: %v`, idr.mergeRulesTable(), err)
		return
	}

	return idr.processMergeRules(loadFileNames)
}

func (idr *HandleT) ResolveHistoricIdentities() (err error) {
	var loadFileNames []string
	defer misc.RemoveFilePaths(loadFileNames...)
	csvGzWriter, csvPath := idr.createTempGzFile(`/rudder-identity-merge-rules-tmp/`)
	err = idr.WarehouseManager.DownloadIdentityRules(&csvGzWriter)
	csvGzWriter.CloseGZ()
	if err != nil {
		pkgLogger.Errorf(`IDR: Failed to download identity information from warehouse with error: %v`, err)
		return
	}
	loadFileNames = append(loadFileNames, csvPath)

	return idr.processMergeRules(loadFileNames)
}
