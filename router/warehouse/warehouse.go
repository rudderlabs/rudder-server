package warehouse

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/warehouse/redshift"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	jobQueryBatchSize         int
	noOfWorkers               int
	warehouseUploadSleepInMin int
	warehouses                []warehouseutils.WarehouseT
	configSubscriberLock      sync.RWMutex
	availableWarehouses       []string
	inProgressMap             map[string]bool
	warehouseCSVUploadsTable  string
	warehouseJSONUploadsTable string
	warehouseUploadsTable     string
	warehouseSchemasTable     string
)

// HandleT ...
type HandleT struct {
	dbHandle *sql.DB
	processQ chan ProcessJSONsJobT
	uploadQ  chan JSONToCSVsJobT
}

// ProcessJSONsJobT ...
type ProcessJSONsJobT struct {
	UploadID   int64
	List       []*JSONUploadT
	Schema     map[string]map[string]string
	Warehouse  warehouseutils.WarehouseT
	StartCSVID int64
}

// JSONToCSVsJobT ...
type JSONToCSVsJobT struct {
	UploadID  int64
	JSON      *JSONUploadT
	Schema    map[string]map[string]string
	Warehouse warehouseutils.WarehouseT
	Wg        *sync.WaitGroup
}

// JSONUploadT ...
type JSONUploadT struct {
	ID        string
	Location  string
	SourceID  string
	Schema    json.RawMessage
	Status    string // enum
	CreatedAt time.Time
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	warehouseUploadSleepInMin = config.GetInt("BatchRouter.warehouseUploadSleepInMin", 60)
	warehouseJSONUploadsTable = config.GetString("BatchRouter.warehouseJSONUploadsTable", "wh_json_uploads")
	warehouseCSVUploadsTable = config.GetString("BatchRouter.warehouseCSVUploadsTable", "wh_csv_uploads")
	warehouseUploadsTable = config.GetString("BatchRouter.warehouseUploadsTable", "wh_uploads")
	warehouseSchemasTable = config.GetString("BatchRouter.warehouseSchemasTable", "wh_schemas")
	availableWarehouses = []string{"S3"}
	inProgressMap = map[string]bool{}
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Eb.Subscribe("backendconfig", ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if source.Enabled && len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if misc.Contains(availableWarehouses, destination.DestinationDefinition.Name) {
						warehouses = append(warehouses, warehouseutils.WarehouseT{Source: source, Destination: destination})
						break
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (wh *HandleT) getPendingJSONss(sourceID string) ([]*JSONUploadT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                 FROM
                                  %[1]s,
                                   WHERE %[1]s.status='waiting' AND %[1]s.source_id='%[2]s'`,
		warehouseJSONUploadsTable, sourceID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	defer rows.Close()
	misc.AssertError(err)

	var jsonUploadList []*JSONUploadT
	for rows.Next() {
		var jsonUpload JSONUploadT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		misc.AssertError(err)
		jsonUploadList = append(jsonUploadList, &jsonUpload)
	}

	return jsonUploadList, nil
}

func (wh *HandleT) getPendingJSONs(warehouse warehouseutils.WarehouseT) ([]*JSONUploadT, error) {
	var lastJSONID int
	sqlStatement := fmt.Sprintf(`SELECT end_json_id FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY %[1]s.id`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID)

	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastJSONID)
	if err != nil && err != sql.ErrNoRows {
		// log the error
		misc.AssertError(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s'
								ORDER BY id ASC`,
		warehouseJSONUploadsTable, lastJSONID, warehouse.Source.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		// log the error
		misc.AssertError(err)
	}
	defer rows.Close()

	var jsonUploadList []*JSONUploadT
	for rows.Next() {
		var jsonUpload JSONUploadT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		misc.AssertError(err)
		jsonUploadList = append(jsonUploadList, &jsonUpload)
	}

	return jsonUploadList, nil
}

func consolidateSchema(jsonUploadsList []*JSONUploadT) map[string]map[string]string {
	schemaMap := make(map[string]map[string]string)
	for _, upload := range jsonUploadsList {
		var schema map[string]map[string]string
		err := json.Unmarshal(upload.Schema, &schema)
		misc.AssertError(err)
		for key, val := range schema {
			if schemaMap[key] != nil {
				for columnName, columnType := range val {
					schemaMap[key][columnName] = columnType
				}
			} else {
				schemaMap[key] = val
			}
		}
	}
	return schemaMap
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*JSONUploadT, schema map[string]map[string]string) (uploadID, startCSVID int64, err error) {

	startCSVIDSql := fmt.Sprintf(`SELECT end_csv_id FROM %[1]s WHERE (%[1]s.source_id = '%[2]s' AND %[1]s.destination_id = '%[3]s' AND %[1]s.end_csv_id IS NOT NULL) ORDER BY id DESC LIMIT 1`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID)
	err = wh.dbHandle.QueryRow(startCSVIDSql).Scan(&startCSVID)
	if err != nil && err != sql.ErrNoRows {
		misc.AssertError(err)
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, source_schema_name, destination_id, destination_type, start_json_id, end_json_id, start_csv_id, status, schema, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11) RETURNING id`, warehouseUploadsTable)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	misc.AssertError(err)
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	currentSchema, err := json.Marshal(schema)
	row := stmt.QueryRow(warehouse.Source.ID, strcase.ToSnake(warehouse.Source.Name), warehouse.Destination.ID, warehouse.Destination.DestinationDefinition.Name, startJSONID, endJSONID, startCSVID, warehouseutils.GeneratingCsvState, currentSchema, time.Now(), time.Now())
	err = row.Scan(&uploadID)
	misc.AssertError(err)
	return
}

func (wh *HandleT) mainLoop() {
	for {
		time.Sleep(time.Duration(2) * time.Second)
		// time.Sleep(time.Duration(warehouseUploadSleepInMin) * time.Minute)
		for _, warehouse := range warehouses {
			if inProgressMap[warehouse.Destination.ID] {
				continue
			}
			jsonUploadsList, err := wh.getPendingJSONs(warehouse)
			if len(jsonUploadsList) == 0 {
				delete(inProgressMap, warehouse.Destination.ID)
				continue
			}
			misc.AssertError(err)
			consolidatedSchema := consolidateSchema(jsonUploadsList)
			id, startCSVID, err := wh.initUpload(warehouse, jsonUploadsList, consolidatedSchema)
			inProgressMap[warehouse.Destination.ID] = true
			wh.processQ <- ProcessJSONsJobT{List: jsonUploadsList, Schema: consolidatedSchema, Warehouse: warehouse, UploadID: id, StartCSVID: startCSVID}
			// get all wh_json_uploads rows
			// consolidate schema
			// create record on wh_uploads table
			// send to worker channel
		}
	}
}

func (wh *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				processJSONsJob := <-wh.processQ
				var wg sync.WaitGroup
				wg.Add(len(processJSONsJob.List))
				for _, toProcessJSON := range processJSONsJob.List {
					wh.uploadQ <- JSONToCSVsJobT{UploadID: processJSONsJob.UploadID, JSON: toProcessJSON, Schema: processJSONsJob.Schema, Warehouse: processJSONsJob.Warehouse, Wg: &wg}
				}
				wg.Wait()

				var endCSVID int64
				// TODO: add where clause
				lastCSVIDSql := fmt.Sprintf(`SELECT id FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY id DESC LIMIT 1`, warehouseCSVUploadsTable, processJSONsJob.Warehouse.Source.ID, processJSONsJob.Warehouse.Destination.ID)
				err := wh.dbHandle.QueryRow(lastCSVIDSql).Scan(&endCSVID)
				misc.AssertError(err)

				sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, end_csv_id=$2 WHERE id=$3`, warehouseUploadsTable)
				_, err = wh.dbHandle.Exec(sqlStatement, warehouseutils.GeneratedCsvState, endCSVID, processJSONsJob.UploadID)
				misc.AssertError(err)
				// delete(inProgressMap, processJSONsJob.Warehouse.Destination.ID)
				var rs redshift.HandleT
				rs.Setup(warehouseutils.ConfigT{
					DbHandle:   wh.dbHandle,
					UploadID:   processJSONsJob.UploadID,
					Schema:     processJSONsJob.Schema,
					Warehouse:  processJSONsJob.Warehouse,
					StartCSVID: processJSONsJob.StartCSVID,
					EndCSVID:   endCSVID,
				})
			}
		}()
	}
}

func (wh *HandleT) processJSON(job JSONToCSVsJobT) (err error) {
	// TODO: change this
	jsonPath := config.GetEnv("TMPDIR2", "/Users/srikanth/warehousedata/")
	jsonPath += job.Warehouse.Destination.DestinationDefinition.Name + "/" + job.JSON.Location
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	jsonFile, err := os.Create(jsonPath)
	misc.AssertError(err)

	downloader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: "rl-redshift-json-dump",
	})

	err = downloader.Download(jsonFile, job.JSON.Location)
	misc.AssertError(err)
	jsonFile.Close()
	defer os.Remove(jsonPath)

	sortedTableColumnMap := make(map[string][]string)
	for tableName, columnMap := range job.Schema {
		sortedTableColumnMap[tableName] = []string{}
		for k := range columnMap {
			sortedTableColumnMap[tableName] = append(sortedTableColumnMap[tableName], k)
		}
		sort.Strings(sortedTableColumnMap[tableName])
	}

	rawf, err := os.Open(jsonPath)
	reader, _ := gzip.NewReader(rawf)

	tableContentMap := make(map[string]string)
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		lineBytes := sc.Bytes()
		var jsonLine map[string]interface{}
		json.Unmarshal(lineBytes, &jsonLine)
		metadata, _ := jsonLine["metadata"]
		tableName, _ := metadata.(map[string]interface{})["table"].(string)
		var csvFile string
		if _, ok := tableContentMap[tableName]; ok {
			csvFile = tableContentMap[tableName]
		} else {
			tableContentMap[tableName] = csvFile
		}
		csvRow := []string{}
		for _, columnName := range sortedTableColumnMap[tableName] {
			columnVal, _ := jsonLine[columnName]
			csvRow = append(csvRow, fmt.Sprintf("%v", columnVal))
		}
		tableContentMap[tableName] += strings.Join(csvRow, ",") + "\n"
	}
	csvFileMap := make(map[string]*os.File)
	for tableName, content := range tableContentMap {
		csvPath := strings.TrimSuffix(jsonPath, "json.gz") + tableName + ".csv.gz"
		cfile, err := os.Create(csvPath)
		csvFileMap[tableName] = cfile
		gzipWriter := gzip.NewWriter(cfile)
		_, err = gzipWriter.Write([]byte(content))
		misc.AssertError(err)
		gzipWriter.Close()
	}
	uploader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: "rl-redshift-csv-dump",
	})
	misc.AssertError(err)
	for tableName, csvFile := range csvFileMap {
		file, err := os.Open(csvFile.Name())
		defer os.Remove(csvFile.Name())
		uploadLocation, err := uploader.Upload(file, job.Warehouse.Source.ID, strconv.FormatInt(job.UploadID, 10), tableName)
		misc.AssertError(err)
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (json_id, location, source_id, destination_id, destination_type, table_name, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, warehouseCSVUploadsTable)
		stmt, err := wh.dbHandle.Prepare(sqlStatement)
		misc.AssertError(err)
		defer stmt.Close()

		_, err = stmt.Exec(job.JSON.ID, uploadLocation, job.JSON.SourceID, job.Warehouse.Destination.ID, job.Warehouse.Destination.DestinationDefinition.Name, tableName, time.Now())
		misc.AssertError(err)
	}
	return err
}

func (wh *HandleT) initUploaders() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				jsonToCSVsJob := <-wh.uploadQ
				wh.processJSON(jsonToCSVsJob)
				jsonToCSVsJob.Wg.Done()
			}
		}()
	}
}

func (wh *HandleT) setupTables() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  json_id BIGINT,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  table_name VARCHAR(64) NOT NULL,
									  created_at TIMESTAMP NOT NULL);`, warehouseCSVUploadsTable)

	_, err := wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_upload_state_type
                                     AS ENUM(
											  'generating_csv',
											  'generating_csv_failed',
											  'generated_csv',
											  'updating_schema',
											  'updating_schema_failed',
											  'updated_schema',
											  'exporting_data',
											  'exporting_data_failed',
											  'exported_data');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  source_id VARCHAR(64) NOT NULL,
									  source_schema_name VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  start_json_id BIGINT,
									  end_json_id BIGINT,
									  start_csv_id BIGINT,
									  end_csv_id BIGINT,
									  status wh_upload_state_type NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseUploadsTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL,
									  source_id VARCHAR(64) NOT NULL,
									  source_schema_name VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  created_at TIMESTAMP NOT NULL);`, warehouseSchemasTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)
}

// Setup ...
func (wh *HandleT) Setup() {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	wh.dbHandle, err = sql.Open("postgres", psqlInfo)
	misc.AssertError(err)
	wh.setupTables()
	wh.processQ = make(chan ProcessJSONsJobT)
	wh.uploadQ = make(chan JSONToCSVsJobT)
	go backendConfigSubscriber()
	go wh.initUploaders()
	go wh.initWorkers()
	go wh.mainLoop()
}
