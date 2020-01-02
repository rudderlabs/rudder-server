package warehouse

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/rudderlabs/rudder-server/router/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/router/warehouse/redshift"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	jobQueryBatchSize          int
	noOfWorkers                int
	warehouseUploadSleepInS    int
	mainLoopSleepInS           int
	configSubscriberLock       sync.RWMutex
	availableWarehouses        []string
	inProgressMap              map[string]bool
	inProgressMapLock          sync.RWMutex
	warehouseLoadFilesTable    string
	warehouseStagingFilesTable string
	warehouseUploadsTable      string
	warehouseSchemasTable      string
)

type HandleT struct {
	destType   string
	warehouses []warehouseutils.WarehouseT
	dbHandle   *sql.DB
	processQ   chan ProcessStagingFilesJobT
	uploadQ    chan LoadFileJobT
	isEnabled  bool
}

type ProcessStagingFilesJobT struct {
	Upload    warehouseutils.UploadT
	List      []*StagingFileT
	Warehouse warehouseutils.WarehouseT
}

type LoadFileJobT struct {
	UploadID    int64
	StagingFile *StagingFileT
	Schema      map[string]map[string]string
	Warehouse   warehouseutils.WarehouseT
	Wg          *misc.WaitGroup
}

type StagingFileT struct {
	ID        int64
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
	noOfWorkers = config.GetInt("Warehouse.noOfWorkers", 8)
	warehouseUploadSleepInS = config.GetInt("Warehouse.uploadSleepInS", 1800)
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	mainLoopSleepInS = config.GetInt("Warehouse.mainLoopSleepInS", 5)
	availableWarehouses = []string{"RS", "BQ"}
	inProgressMap = map[string]bool{}
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if source.Enabled && len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.Enabled && destination.DestinationDefinition.Name == wh.destType {
						wh.warehouses = append(wh.warehouses, warehouseutils.WarehouseT{Source: source, Destination: destination})
						break
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (wh *HandleT) getPendingJSONs(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastJSONID int
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status= '%[4]s' OR %[1]s.status = '%[5]s') ORDER BY %[1]s.id DESC`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)

	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastJSONID)
	if err != nil && err != sql.ErrNoRows {
		misc.AssertError(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC LIMIT 20`,
		warehouseStagingFilesTable, lastJSONID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		misc.AssertError(err)
	}
	defer rows.Close()

	var jsonUploadList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		misc.AssertError(err)
		jsonUploadList = append(jsonUploadList, &jsonUpload)
	}

	return jsonUploadList, nil
}

func consolidateSchema(jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaMap := make(map[string]map[string]string)
	for _, upload := range jsonUploadsList {
		var schema map[string]map[string]string
		err := json.Unmarshal(upload.Schema, &schema)
		misc.AssertError(err)
		for tableName, columnMap := range schema {
			if schemaMap[tableName] == nil {
				schemaMap[tableName] = columnMap
			} else {
				for columnName, columnType := range columnMap {
					schemaMap[tableName][columnName] = columnType
				}
			}
		}
	}
	return schemaMap
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, schema map[string]map[string]string) warehouseutils.UploadT {
	var startLoadFileID int64
	startLoadFileIDSql := fmt.Sprintf(`SELECT end_load_file_id FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status='%[4]s' OR %[1]s.status='%[5]s')) ORDER BY id DESC LIMIT 1`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)
	err := wh.dbHandle.QueryRow(startLoadFileIDSql).Scan(&startLoadFileID)
	if err != nil && err != sql.ErrNoRows {
		misc.AssertError(err)
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, status, schema, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11) RETURNING id`, warehouseUploadsTable)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	misc.AssertError(err)
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	currentSchema, err := json.Marshal(schema)
	namespace := strings.ToLower(strcase.ToSnake(warehouse.Source.Name))
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, startLoadFileID, warehouseutils.GeneratingLoadFileState, currentSchema, time.Now(), time.Now())
	var uploadID int64
	err = row.Scan(&uploadID)
	misc.AssertError(err)
	return warehouseutils.UploadT{
		ID:                 uploadID,
		Namespace:          namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		StartLoadFileID:    startLoadFileID,
		Status:             warehouseutils.GeneratingLoadFileState,
		Schema:             schema,
	}
}

func (wh *HandleT) getPendingUpload(warehouse warehouseutils.WarehouseT) (warehouseutils.UploadT, bool) {
	var upload warehouseutils.UploadT
	var schema json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.status!='%[4]s' AND %[1]s.status!='%[5]s' AND %[1]s.status!='%[6]s')`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState, warehouseutils.GeneratingLoadFileState)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&upload.ID, &upload.Status, &schema, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error)
	if err != nil && err != sql.ErrNoRows {
		misc.AssertError(err)
	}
	if err == sql.ErrNoRows {
		return warehouseutils.UploadT{}, false
	}
	upload.Schema = warehouseutils.JSONSchemaToMap(schema)
	return upload, true
}

func setDestInProgress(destID string, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[destID] = true
	} else {
		delete(inProgressMap, destID)
	}
	inProgressMapLock.Unlock()
}

func isDestInProgress(destID string) bool {
	inProgressMapLock.RLock()
	if inProgressMap[destID] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

type WarehouseManager interface {
	Process(config warehouseutils.ConfigT)
}

func NewWhManager(destType string) (WarehouseManager, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT
		return &rs, nil
	case "BQ":
		var bq bigquery.HandleT
		return &bq, nil
	}
	return nil, errors.New("No provider configured for WarehouseManager")
}

func (wh *HandleT) mainLoop() {
	for {
		if !wh.isEnabled {
			time.Sleep(time.Duration(mainLoopSleepInS) * time.Second)
			continue
		}
		for _, warehouse := range wh.warehouses {
			if isDestInProgress(warehouse.Destination.ID) {
				continue
			}
			setDestInProgress(warehouse.Destination.ID, true)

			pendingUpload, ok := wh.getPendingUpload(warehouse)
			if ok {
				whManager, err := NewWhManager(wh.destType)
				misc.AssertError(err)
				switch pendingUpload.Status {
				case warehouseutils.GeneratedLoadFileState, warehouseutils.UpdatingSchemaState, warehouseutils.UpdatingSchemaFailedState:
					go func() {
						whManager.Process(warehouseutils.ConfigT{
							DbHandle:  wh.dbHandle,
							Upload:    pendingUpload,
							Warehouse: warehouse,
							Stage:     "UpdateSchema",
						})
						setDestInProgress(warehouse.Destination.ID, false)
					}()
					continue
				case warehouseutils.UpdatedSchemaState, warehouseutils.ExportingDataState, warehouseutils.ExportingDataFailedState:
					go func() {
						whManager.Process(warehouseutils.ConfigT{
							DbHandle:  wh.dbHandle,
							Upload:    pendingUpload,
							Warehouse: warehouse,
							Stage:     "ExportData",
						})
						setDestInProgress(warehouse.Destination.ID, false)
					}()
					continue
				}
			}
			jsonUploadsList, err := wh.getPendingJSONs(warehouse)
			if len(jsonUploadsList) == 0 {
				setDestInProgress(warehouse.Destination.ID, false)
				continue
			}
			misc.AssertError(err)
			consolidatedSchema := consolidateSchema(jsonUploadsList)
			upload := wh.initUpload(warehouse, jsonUploadsList, consolidatedSchema)
			wh.processQ <- ProcessStagingFilesJobT{
				List:      jsonUploadsList,
				Warehouse: warehouse,
				Upload:    upload,
			}
		}
		time.Sleep(time.Duration(warehouseUploadSleepInS) * time.Second)
	}
}

func (wh *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				processStagingFilesJob := <-wh.processQ
				var jsonIDs []int64
				for _, job := range processStagingFilesJob.List {
					jsonIDs = append(jsonIDs, job.ID)
				}
				warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileExecutingState, wh.dbHandle)

				wg := misc.NewWaitGroup()
				wg.Add(len(processStagingFilesJob.List))
				for _, toProcessJSON := range processStagingFilesJob.List {
					wh.uploadQ <- LoadFileJobT{
						UploadID:    processStagingFilesJob.Upload.ID,
						StagingFile: toProcessJSON,
						Schema:      processStagingFilesJob.Upload.Schema,
						Warehouse:   processStagingFilesJob.Warehouse,
						Wg:          wg,
					}
				}
				err := wg.Wait()
				if err != nil {
					warehouseutils.SetStagingFilesError(jsonIDs, warehouseutils.StagingFileFailedState, wh.dbHandle, err)
					setDestInProgress(processStagingFilesJob.Warehouse.Destination.ID, false)
					continue
				}
				warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileSucceededState, wh.dbHandle)

				var endLoadFileID int64
				lastLoadFileIDSql := fmt.Sprintf(`SELECT id FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY id DESC LIMIT 1`, warehouseLoadFilesTable, processStagingFilesJob.Warehouse.Source.ID, processStagingFilesJob.Warehouse.Destination.ID)
				err = wh.dbHandle.QueryRow(lastLoadFileIDSql).Scan(&endLoadFileID)
				misc.AssertError(err)

				sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, end_load_file_id=$2, updated_at=$3 WHERE id=$4`, warehouseUploadsTable)
				_, err = wh.dbHandle.Exec(sqlStatement, warehouseutils.GeneratedLoadFileState, endLoadFileID, time.Now(), processStagingFilesJob.Upload.ID)
				misc.AssertError(err)
				whManager, err := NewWhManager(wh.destType)
				misc.AssertError(err)

				processStagingFilesJob.Upload.EndLoadFileID = endLoadFileID
				processStagingFilesJob.Upload.Status = warehouseutils.GeneratedLoadFileState
				whManager.Process(warehouseutils.ConfigT{
					DbHandle:  wh.dbHandle,
					Upload:    processStagingFilesJob.Upload,
					Warehouse: processStagingFilesJob.Warehouse,
				})
				setDestInProgress(processStagingFilesJob.Warehouse.Destination.ID, false)
			}
		}()
	}
}

func (wh *HandleT) processJSON(job LoadFileJobT) (err error) {
	dirName := "/rudder-warehouse-json-uploads-tmp/"
	tmpDirPath := misc.CreateTMPDIR()
	jsonPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, wh.destType, job.Warehouse.Destination.ID) + job.StagingFile.Location
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	jsonFile, err := os.Create(jsonPath)
	misc.AssertError(err)

	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[wh.destType],
		Config:   job.Warehouse.Destination.Config.(map[string]interface{}),
	})
	if err != nil {
		return err
	}

	err = downloader.Download(jsonFile, job.StagingFile.Location)
	if err != nil {
		return err
	}
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
	misc.AssertError(err)
	reader, err := gzip.NewReader(rawf)
	misc.AssertError(err)

	tableContentMap := make(map[string]string)
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		lineBytes := sc.Bytes()
		var jsonLine map[string]interface{}
		json.Unmarshal(lineBytes, &jsonLine)
		metadata, _ := jsonLine["metadata"]
		columnData := jsonLine["data"].(map[string]interface{})
		tableName, _ := metadata.(map[string]interface{})["table"].(string)
		columns, _ := metadata.(map[string]interface{})["columns"].(map[string]interface{})
		if _, ok := tableContentMap[tableName]; !ok {
			tableContentMap[tableName] = ""
		}
		if wh.destType == "BQ" {
			line, err := json.Marshal(columnData)
			misc.AssertError(err)
			tableContentMap[tableName] += string(line) + "\n"
		} else {
			csvRow := []string{}
			for _, columnName := range sortedTableColumnMap[tableName] {
				columnVal, _ := columnData[columnName]
				if stringVal, ok := columnVal.(string); ok {
					// handle commas in column values for csv
					if strings.Contains(stringVal, ",") {
						columnVal = strings.ReplaceAll(stringVal, "\"", "\"\"")
						columnVal = fmt.Sprintf(`"%s"`, columnVal)
					}
				}
				// avoid printing integers like 5000000 as 5e+06
				columnType := columns[columnName].(string)
				if columnType == "int" {
					columnVal = int(columnVal.(float64))
				}
				csvRow = append(csvRow, fmt.Sprintf("%v", columnVal))
			}
			tableContentMap[tableName] += strings.Join(csvRow, ",") + "\n"
		}
	}
	outputFileMap := make(map[string]*os.File)
	for tableName, content := range tableContentMap {
		outputFilePath := strings.TrimSuffix(jsonPath, "json.gz") + tableName + ".csv.gz"
		outputFile, err := os.Create(outputFilePath)
		outputFileMap[tableName] = outputFile
		gzipWriter := gzip.NewWriter(outputFile)
		_, err = gzipWriter.Write([]byte(content))
		misc.AssertError(err)
		gzipWriter.Close()
	}

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[wh.destType],
		Config:   job.Warehouse.Destination.Config.(map[string]interface{}),
	})
	misc.AssertError(err)

	for tableName, outputFile := range outputFileMap {
		file, err := os.Open(outputFile.Name())
		defer os.Remove(outputFile.Name())
		uploadLocation, err := uploader.Upload(file, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.Warehouse.Source.ID, strconv.FormatInt(job.UploadID, 10))
		if err != nil {
			return err
		}
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (staging_file_id, location, source_id, destination_id, destination_type, table_name, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, warehouseLoadFilesTable)
		stmt, err := wh.dbHandle.Prepare(sqlStatement)
		misc.AssertError(err)
		defer stmt.Close()

		_, err = stmt.Exec(job.StagingFile.ID, uploadLocation.Location, job.StagingFile.SourceID, job.Warehouse.Destination.ID, wh.destType, tableName, time.Now())
		misc.AssertError(err)
	}
	return err
}

func (wh *HandleT) initUploaders() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				makeLoadFilesJob := <-wh.uploadQ
				err := wh.processJSON(makeLoadFilesJob)
				if err != nil {
					makeLoadFilesJob.Wg.Err(err)
				} else {
					makeLoadFilesJob.Wg.Done()
				}
			}
		}()
	}
}

func (wh *HandleT) setupTables() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  staging_file_id BIGINT,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  table_name VARCHAR(64) NOT NULL,
									  created_at TIMESTAMP NOT NULL);`, warehouseLoadFilesTable)

	_, err := wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_upload_state_type
                                     AS ENUM(
											  'generationg_load_file',
											  'generationg_load_file_failed',
											  'generated_load_file',
											  'updating_schema',
											  'updating_schema_failed',
											  'updated_schema',
											  'exporting_data',
											  'exporting_data_failed',
											  'exported_data',
											  'aborted');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  start_staging_file_id BIGINT,
									  end_staging_file_id BIGINT,
									  start_load_file_id BIGINT,
									  end_load_file_id BIGINT,
									  status wh_upload_state_type NOT NULL,
									  schema JSONB NOT NULL,
									  error JSONB,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseUploadsTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error VARCHAR(512),
									  created_at TIMESTAMP NOT NULL);`, warehouseSchemasTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	misc.AssertError(err)
}

//Enable enables a router :)
func (wh *HandleT) Enable() {
	wh.isEnabled = true
}

//Disable disables a router:)
func (wh *HandleT) Disable() {
	wh.isEnabled = false
}

func (wh *HandleT) Setup(whType string) {
	logger.Infof("WH: Warehouse Router started: %s\n", whType)
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	wh.dbHandle, err = sql.Open("postgres", psqlInfo)
	misc.AssertError(err)
	wh.setupTables()
	wh.destType = whType
	wh.isEnabled = true
	wh.processQ = make(chan ProcessStagingFilesJobT)
	wh.uploadQ = make(chan LoadFileJobT)
	go wh.backendConfigSubscriber()
	go wh.initUploaders()
	go wh.initWorkers()
	go wh.mainLoop()
}
