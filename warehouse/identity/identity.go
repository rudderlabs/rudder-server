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

type HandleT struct {
	Warehouse warehouseutils.WarehouseT
	DbHandle  *sql.DB
	Upload    warehouseutils.UploadT
}

const (
	mergeRulesTable = "rudder_identity_merge_rules"
	mappingsTable   = "rudder_identity_mappings"
)

func (idr *HandleT) mergeRulesTable() string {
	// return "rudder_identity_merge_rules"
	return warehouseutils.ToProviderCase(idr.Warehouse.Destination.DestinationDefinition.Name, "rudder_identity_merge_rules")
}

func (idr *HandleT) Resolve() {

	txn, err := idr.DbHandle.Begin()
	if err != nil {
		// TODO: change this
		panic(err)
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}

	dirName := "/rudder-identity-merge-rules-tmp/"
	mergeRulesFilePath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/%v/`, idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.ID, idr.Upload.ID) + uuid.NewV4().String() + ".csv.gz"
	err = os.MkdirAll(filepath.Dir(mergeRulesFilePath), os.ModePerm)
	if err != nil {
		// TODO: change this
		panic(err)
	}
	mergeRulesFileGzWriter, err := misc.CreateGZ(mergeRulesFilePath)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	ids, err := idr.AddRules(txn, &mergeRulesFileGzWriter)
	if err != nil {
		panic(err)
	}
	mergeRulesFileGzWriter.CloseGZ()
	fmt.Printf("%+v\n", ids)

	dirName = "/rudder-identity-mappings-tmp/"
	mappingFilePath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/%v/`, idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.ID, idr.Upload.ID) + uuid.NewV4().String() + ".csv.gz"
	err = os.MkdirAll(filepath.Dir(mappingFilePath), os.ModePerm)
	if err != nil {
		// TODO: change this
		panic(err)
	}
	mappingsFileGzWriter, err := misc.CreateGZ(mappingFilePath)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	for _, id := range ids {
		idr.ApplyRule(txn, id, &mappingsFileGzWriter)
	}
	mappingsFileGzWriter.CloseGZ()

	fmt.Printf("%+v\n", mappingFilePath)

	// load mergeRules file to warehouse
	// - load to s3
	// - create load file
	// - create record in wh_table_uploads
	// - upload to warehouse
	// load mappings file to warehouse

	err = idr.CreateLoadFile(mergeRulesTable, txn, mergeRulesFilePath)
	if err != nil {
		// TODO: change this
		panic(err)
	}
	err = idr.CreateLoadFile(mappingsTable, txn, mappingFilePath)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	err = txn.Commit()
	if err != nil {
		// TODO: change this
		panic(err)
	}

}

func (idr *HandleT) ApplyRule(txn *sql.Tx, ruleID int64, gzWriter *misc.GZipWriter) {
	// insert missing merge props into mappings
	// check if all rows with either of the merge prop's have same rudder_id
	// if not generate rudder_id and change for all
	// write changed rows to temp table?
	sqlStatement := fmt.Sprintf(`SELECT merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value FROM %s WHERE id=%v`, mergeRulesTable, ruleID)

	var prop1Val, prop2Val, prop1Type, prop2Type sql.NullString

	err := txn.QueryRow(sqlStatement).Scan(&prop1Type, &prop1Val, &prop2Type, &prop2Val)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	var rudderIDs []string
	var x string
	if prop2Val.Valid && prop2Type.Valid {
		x = fmt.Sprintf(`OR (merge_property_type='%s' AND merge_property_value='%s')`, prop2Type.String, prop2Val.String)
	}
	sqlStatement = fmt.Sprintf(`SELECT ARRAY_AGG(DISTINCT(rudder_id)) FROM %s WHERE (merge_property_type='%s' AND merge_property_value='%s') %s`, mappingsTable, prop1Type.String, prop1Val.String, x)
	fmt.Printf("%+v\n", sqlStatement)
	err = txn.QueryRow(sqlStatement).Scan(pq.Array(&rudderIDs))

	if err != nil {
		panic(err)
	}

	currentTimeString := time.Now().Format(misc.RFC3339Milli)
	var buff bytes.Buffer
	csvWriter := csv.NewWriter(&buff)
	var csvRows [][]string
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

		var row1Values string
		for index, key := range row1 {
			if index > 0 {
				row1Values += fmt.Sprintf(`, `)
			}
			row1Values += fmt.Sprintf(`'%s'`, key)
		}

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, rudderID, currentTimeString}
			csvRows = append(csvRows, row2)

			for index, key := range row2 {
				if index > 0 {
					row2Values += fmt.Sprintf(`, `)
				}
				row2Values += fmt.Sprintf(`'%s'`, key)
			}
			row2Values = fmt.Sprintf(`, (%s)`, row2Values)
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, mappingsTable, row1Values, row2Values, "unique_merge_property")
		fmt.Printf("%+v\n", sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			// TODO: change this
			panic(err)
		}
	} else {
		// generate new one and update all
		newID := uuid.NewV4().String()
		row1 := []string{prop1Type.String, prop1Val.String, newID, currentTimeString}
		csvRows = append(csvRows, row1)

		var row1Values string
		for index, key := range row1 {
			if index > 0 {
				row1Values += fmt.Sprintf(`, `)
			}
			row1Values += fmt.Sprintf(`'%s'`, key)
		}

		var row2Values string
		if prop2Val.Valid && prop2Type.Valid {
			row2 := []string{prop2Type.String, prop2Val.String, newID, currentTimeString}
			csvRows = append(csvRows, row2)

			for index, key := range row2 {
				if index > 0 {
					row2Values += fmt.Sprintf(`, `)
				}
				row2Values += fmt.Sprintf(`'%s'`, key)
			}
			row2Values = fmt.Sprintf(`, (%s)`, row2Values)
		}

		var x string
		for index, rudderID := range rudderIDs {
			if index > 0 {
				x += fmt.Sprintf(`, `)
			}
			x += fmt.Sprintf(`'%s'`, rudderID)
		}

		sqlStatement := fmt.Sprintf(`SELECT merge_property_type, merge_property_value FROM %s WHERE rudder_id IN (%v)`, mappingsTable, x)
		fmt.Printf("%+v\n", sqlStatement)
		rows, err := txn.Query(sqlStatement)
		if err != nil {
			// TODO: change this
			panic(err)
		}

		for rows.Next() {
			var mergePropType, mergePropVal string
			err = rows.Scan(&mergePropType, &mergePropVal)
			if err != nil {
				// TODO: change this
				panic(err)
			}
			csvRow := []string{mergePropType, mergePropVal, newID, currentTimeString}
			csvRows = append(csvRows, csvRow)
		}

		sqlStatement = fmt.Sprintf(`UPDATE %s SET rudder_id='%s', updated_at='%s' WHERE rudder_id IN (%v)`, mappingsTable, newID, currentTimeString, x)
		fmt.Printf("%+v\n", sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			// TODO: change this
			panic(err)
		}

		sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_type, merge_property_value, rudder_id, updated_at) VALUES (%s) %s ON CONFLICT ON CONSTRAINT %s DO NOTHING`, mappingsTable, row1Values, row2Values, "unique_merge_property")
		fmt.Printf("%+v\n", sqlStatement)
		_, err = txn.Exec(sqlStatement)
		if err != nil {
			// TODO: change this
			panic(err)
		}
	}
	for _, csvRow := range csvRows {
		csvWriter.Write(csvRow)
	}
	csvWriter.Flush()
	gzWriter.WriteGZ(buff.String())

}

func (idr *HandleT) AddRules(txn *sql.Tx, gzWriter *misc.GZipWriter) ([]int64, error) {
	// do everything in a pg txn
	loadFileNames, err := idr.DownloadLoadFiles(idr.mergeRulesTable())
	if err != nil {
		// TODO: change this
		panic(err)
	}

	sqlStatement := fmt.Sprintf(`CREATE TEMP TABLE %s
	ON COMMIT DROP
	AS
	SELECT *
	FROM %s
	WITH NO DATA;`, "temp_merge_rules_table", mergeRulesTable)

	_, err = txn.Exec(sqlStatement)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	// TODO: change this
	sortedColumnKeys := []string{"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value"}
	stmt, err := txn.Prepare(pq.CopyIn("temp_merge_rules_table", sortedColumnKeys...))
	if err != nil {
		// TODO: change this
		panic(err)
	}

	for _, loadFileName := range loadFileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(loadFileName)
		if err != nil {
			// TODO: change this
			panic(err)
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			// TODO: change this
			panic(err)
		}

		csvReader := csv.NewReader(gzipReader)
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					// logger.Infof("PG: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				} else {
					gzipReader.Close()
					gzipFile.Close()
					// TODO: change this
					panic(err)
					// return
				}
			}
			var recordInterface []interface{}
			for _, value := range record {
				if strings.TrimSpace(value) == "" {
					recordInterface = append(recordInterface, nil)
				} else {
					recordInterface = append(recordInterface, value)
				}
			}
			// fmt.Printf("%+v\n", recordInterface...)
			_, err = stmt.Exec(recordInterface...)
		}
		gzipReader.Close()
		gzipFile.Close()
	}

	_, err = stmt.Exec()
	if err != nil {
		// TODO: change this
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`DELETE FROM %s AS staging USING %s _m WHERE (_m.merge_property_1_type = staging.merge_property_1_type) AND (_m.merge_property_1_value = staging.merge_property_1_value) AND (_m.merge_property_2_type = staging.merge_property_2_type) AND (_m.merge_property_2_value = staging.merge_property_2_value)`, "temp_merge_rules_table", mergeRulesTable)

	_, err = txn.Exec(sqlStatement)
	if err != nil {
		// TODO: change this
		panic(err)
	}

	idr.TableToFile("temp_merge_rules_table", txn, gzWriter)

	sqlStatement = fmt.Sprintf(`INSERT INTO %s (merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value)
		SELECT DISTINCT ON (merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value) merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value
		FROM %s RETURNING id`, mergeRulesTable, "temp_merge_rules_table")

	fmt.Printf("%+v\n", sqlStatement)
	rows, err := txn.Query(sqlStatement)
	if err != nil {
		// TODO: change this
		panic(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			// TODO: change this
			panic(err)
		}
		ids = append(ids, id)
	}

	fmt.Printf("%+v\n", ids)

	return ids, nil
}

func (idr *HandleT) TableToFile(tableName string, txn *sql.Tx, gzWriter *misc.GZipWriter) {
	batchSize := int64(500)
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
	var totalRows int64
	err := txn.QueryRow(sqlStatement).Scan(&totalRows)
	if err != nil {
		panic(err)
	}

	var offset int64
	for {
		sqlStatement = fmt.Sprintf(`SELECT merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value FROM %s LIMIT %d OFFSET %d`, tableName, batchSize, offset)

		rows, err := txn.Query(sqlStatement)
		if err != nil {
			// TODO: change this
			panic(err)
		}

		for rows.Next() {
			var buff bytes.Buffer
			csvWriter := csv.NewWriter(&buff)
			var csvRow []string

			var prop1Val, prop2Val, prop1Type, prop2Type sql.NullString
			err = rows.Scan(&prop1Type, &prop1Val, &prop2Type, &prop2Val)
			if err != nil {
				// TODO: change this
				panic(err)
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
}

func (idr *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objectLocations, _ := warehouseutils.GetLoadFileLocations(idr.DbHandle, idr.Warehouse.Source.ID, idr.Warehouse.Destination.ID, tableName, idr.Upload.StartLoadFileID, idr.Upload.EndLoadFileID)
	var filesName []string
	for _, objectLocation := range objectLocations {
		object, err := warehouseutils.GetObjectName(idr.Warehouse.Destination.Config, objectLocation, warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config))
		if err != nil {
			logger.Errorf("PG: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			logger.Errorf("PG: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.ID, time.Now().Unix()) + object
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			logger.Errorf("PG: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			logger.Errorf("PG: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config),
			Config:   idr.Warehouse.Destination.Config,
		})
		err = downloader.Download(objectFile, object)
		if err != nil {
			logger.Errorf("PG: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			logger.Errorf("PG: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		filesName = append(filesName, fileName)
	}
	return filesName, nil
}

func (idr *HandleT) CreateLoadFile(tableName string, txn *sql.Tx, filePath string) (err error) {
	outputFile, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageType(idr.Warehouse.Destination.DestinationDefinition.Name, idr.Warehouse.Destination.Config),
		Config:   idr.Warehouse.Destination.Config,
	})
	output, err := uploader.Upload(outputFile, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, idr.Warehouse.Source.ID, tableName)

	if err != nil {
		return
	}

	sqlStatement := fmt.Sprintf(`UPDATE %s SET location='%s' WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, output.Location, idr.Upload.ID, strings.ToUpper(tableName))
	fmt.Printf("%+v\n", sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
	return
}
