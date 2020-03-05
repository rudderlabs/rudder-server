package warehouse

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
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
	"github.com/rudderlabs/rudder-server/router/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	jobQueryBatchSize          int
	noOfWorkers                int
	uploadFreqInS              int64
	mainLoopSleep              time.Duration
	stagingFilesBatchSize      int
	configSubscriberLock       sync.RWMutex
	availableWarehouses        []string
	crashRecoverWarehouses     []string
	inProgressMap              map[string]bool
	inRecoveryMap              map[string]bool
	inProgressMapLock          sync.RWMutex
	lastExecMap                map[string]int64
	lastExecMapLock            sync.RWMutex
	warehouseLoadFilesTable    string
	warehouseStagingFilesTable string
	warehouseUploadsTable      string
	warehouseSchemasTable      string
)

type HandleT struct {
	destType           string
	warehouses         []warehouseutils.WarehouseT
	dbHandle           *sql.DB
	uploadToWarehouseQ chan []ProcessStagingFilesJobT
	createLoadFilesQ   chan LoadFileJobT
	isEnabled          bool
}

type ProcessStagingFilesJobT struct {
	Upload    warehouseutils.UploadT
	List      []*StagingFileT
	Warehouse warehouseutils.WarehouseT
}

type LoadFileJobT struct {
	Upload                     warehouseutils.UploadT
	StagingFile                *StagingFileT
	Schema                     map[string]map[string]string
	Warehouse                  warehouseutils.WarehouseT
	Wg                         *misc.WaitGroup
	LoadFileIDsChan            chan []int64
	TableToBucketFolderMap     map[string]string
	TableToBucketFolderMapLock *sync.RWMutex
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
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	uploadFreqInS = config.GetInt64("Warehouse.uploadFreqInS", 1800)
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	mainLoopSleep = config.GetDuration("Warehouse.mainLoopSleepInS", 60) * time.Second
	availableWarehouses = []string{"RS", "BQ"}
	crashRecoverWarehouses = []string{"RS"}
	inProgressMap = map[string]bool{}
	inRecoveryMap = map[string]bool{}
	lastExecMap = map[string]int64{}
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendconfigFull")
	for {
		config := <-ch
		configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
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

func (wh *HandleT) getStagingFiles(warehouse warehouseutils.WarehouseT, startID int64, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'
								ORDER BY id ASC`,
		warehouseStagingFilesTable, startID, endID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status= '%[4]s' OR %[1]s.status = '%[5]s') ORDER BY %[1]s.id DESC`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) consolidateSchema(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, warehouse)
	if err != nil {
		panic(err)
	}
	schemaMap := make(map[string]map[string]string)
	for _, upload := range jsonUploadsList {
		var schema map[string]map[string]string
		err := json.Unmarshal(upload.Schema, &schema)
		if err != nil {
			panic(err)
		}
		for tableName, columnMap := range schema {
			if schemaMap[tableName] == nil {
				schemaMap[tableName] = make(map[string]string)
			}
			for columnName, columnType := range columnMap {
				// if column already has a type in db, use that
				if len(schemaInDB.Schema) > 0 {
					if _, ok := schemaInDB.Schema[tableName]; ok {
						if columnTypeInDB, ok := schemaInDB.Schema[tableName][columnName]; ok {
							schemaMap[tableName][columnName] = columnTypeInDB
							continue
						}
					}
				}
				// check if we already set the columnType in schemaMap
				if _, ok := schemaMap[tableName][columnName]; !ok {
					schemaMap[tableName][columnName] = columnType
				}
			}
		}
	}
	return schemaMap
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, schema map[string]map[string]string) warehouseutils.UploadT {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13) RETURNING id`, warehouseUploadsTable)
	logger.Infof("WH: %s: Creating record in wh_load_files id: %v", wh.destType, sqlStatement)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	currentSchema, err := json.Marshal(schema)
	namespace := misc.TruncateStr(strings.ToLower(strcase.ToSnake(warehouse.Source.Name)), 127)
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, warehouseutils.WaitingState, currentSchema, "{}", time.Now(), time.Now())

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	return warehouseutils.UploadT{
		ID:                 uploadID,
		Namespace:          namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             warehouseutils.WaitingState,
		Schema:             schema,
	}
}

func (wh *HandleT) getPendingUploads(warehouse warehouseutils.WarehouseT) ([]warehouseutils.UploadT, bool) {

	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.status!='%[4]s' AND %[1]s.status!='%[5]s') ORDER BY id asc`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)

	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	if err == sql.ErrNoRows {
		return []warehouseutils.UploadT{}, false
	}
	defer rows.Close()

	var uploads []warehouseutils.UploadT
	for rows.Next() {
		var upload warehouseutils.UploadT
		var schema json.RawMessage
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error)
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
		if err != nil {
			panic(err)
		}
		uploads = append(uploads, upload)
	}

	var anyPending bool
	if len(uploads) > 0 {
		anyPending = true
	}
	return uploads, anyPending
}

func connectionString(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`source:%s:destination:%s`, warehouse.Source.ID, warehouse.Destination.ID)
}

func setDestInProgress(warehouse warehouseutils.WarehouseT, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[connectionString(warehouse)] = true
	} else {
		delete(inProgressMap, connectionString(warehouse))
	}
	inProgressMapLock.Unlock()
}

func isDestInProgress(warehouse warehouseutils.WarehouseT) bool {
	inProgressMapLock.RLock()
	if inProgressMap[connectionString(warehouse)] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT) bool {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[connectionString(warehouse)]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	lastExecMap[connectionString(warehouse)] = time.Now().Unix()
	return false
}

type WarehouseManager interface {
	Process(config warehouseutils.ConfigT) error
	CrashRecover(config warehouseutils.ConfigT) (err error)
}

func NewWhManager(destType string) (WarehouseManager, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT
		return &rs, nil
	case "BQ":
		var bq bigquery.HandleT
		return &bq, nil
	case "SNOWFLAKE":
		var sf snowflake.HandleT
		return &sf, nil
	}
	return nil, errors.New("No provider configured for WarehouseManager")
}

func (wh *HandleT) mainLoop() {
	for {
		if !wh.isEnabled {
			time.Sleep(mainLoopSleep)
			continue
		}
		for _, warehouse := range wh.warehouses {
			if isDestInProgress(warehouse) {
				logger.Debugf("WH: Skipping upload loop since %s:%s upload in progess", wh.destType, warehouse.Destination.ID)
				continue
			}
			if uploadFrequencyExceeded(warehouse) {
				logger.Debugf("WH: Skipping upload loop since %s:%s upload freq not exceeded", wh.destType, warehouse.Destination.ID)
				continue
			}
			setDestInProgress(warehouse, true)

			_, ok := inRecoveryMap[warehouse.Destination.ID]
			if ok {
				whManager, err := NewWhManager(wh.destType)
				if err != nil {
					panic(err)
				}
				logger.Infof("WH: Crash recovering for %s:%s", wh.destType, warehouse.Destination.ID)
				err = whManager.CrashRecover(warehouseutils.ConfigT{
					DbHandle:  wh.dbHandle,
					Warehouse: warehouse,
				})
				if err != nil {
					setDestInProgress(warehouse, false)
					continue
				}
				delete(inRecoveryMap, warehouse.Destination.ID)
			}

			// fetch any pending wh_uploads records (query for not successful/aborted uploads)
			pendingUploads, ok := wh.getPendingUploads(warehouse)
			if ok {
				logger.Infof("WH: Found pending uploads: %v for %s:%s", len(pendingUploads), wh.destType, warehouse.Destination.ID)
				jobs := []ProcessStagingFilesJobT{}
				for _, pendingUpload := range pendingUploads {
					stagingFilesList, err := wh.getStagingFiles(warehouse, pendingUpload.StartStagingFileID, pendingUpload.EndStagingFileID)
					if err != nil {
						panic(err)
					}
					jobs = append(jobs, ProcessStagingFilesJobT{
						List:      stagingFilesList,
						Warehouse: warehouse,
						Upload:    pendingUpload,
					})
				}
				wh.uploadToWarehouseQ <- jobs
			} else {
				// fetch staging files that are not processed yet
				stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
				if err != nil {
					panic(err)
				}
				if len(stagingFilesList) == 0 {
					logger.Debugf("WH: Found no pending staging files for %s:%s", wh.destType, warehouse.Destination.ID)
					setDestInProgress(warehouse, false)
					continue
				}
				logger.Infof("WH: Found %v pending staging files for %s:%s", len(stagingFilesList), wh.destType, warehouse.Destination.ID)

				count := 0
				jobs := []ProcessStagingFilesJobT{}
				// process staging files in batches of stagingFilesBatchSize
				for {

					lastIndex := count + stagingFilesBatchSize
					if lastIndex >= len(stagingFilesList) {
						lastIndex = len(stagingFilesList)
					}
					// merge schemas over all staging files in this batch
					consolidatedSchema := wh.consolidateSchema(warehouse, stagingFilesList[count:lastIndex])
					// create record in wh_uploads to mark start of upload to warehouse flow
					upload := wh.initUpload(warehouse, stagingFilesList[count:lastIndex], consolidatedSchema)
					jobs = append(jobs, ProcessStagingFilesJobT{
						List:      stagingFilesList[count:lastIndex],
						Warehouse: warehouse,
						Upload:    upload,
					})
					count += stagingFilesBatchSize
					if count >= len(stagingFilesList) {
						break
					}
				}
				wh.uploadToWarehouseQ <- jobs
			}
		}
		time.Sleep(mainLoopSleep)
	}
}

func (wh *HandleT) createLoadFiles(job *ProcessStagingFilesJobT) (err error) {
	// stat for time taken to process staging files in a single job
	timer := warehouseutils.DestStat(stats.TimerType, "process_staging_files_batch_time", job.Warehouse.Destination.ID)
	timer.Start()

	var jsonIDs []int64
	for _, job := range job.List {
		jsonIDs = append(jsonIDs, job.ID)
	}
	warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileExecutingState, wh.dbHandle)

	wg := misc.NewWaitGroup()
	wg.Add(len(job.List))
	ch := make(chan []int64)
	// queue the staging files in a go routine so that job.List can be higher than number of workers in createLoadFilesQ and not be blocked
	logger.Debugf("WH: Starting batch processing %v stage files with %v workers for %s:%s", len(job.List), noOfWorkers, wh.destType, job.Warehouse.Destination.ID)
	tableToBucketFolderMap := map[string]string{}
	var tableToBucketFolderMapLock sync.RWMutex
	rruntime.Go(func() {
		func() {
			for _, stagingFile := range job.List {
				wh.createLoadFilesQ <- LoadFileJobT{
					Upload:                     job.Upload,
					StagingFile:                stagingFile,
					Schema:                     job.Upload.Schema,
					Warehouse:                  job.Warehouse,
					Wg:                         wg,
					LoadFileIDsChan:            ch,
					TableToBucketFolderMap:     tableToBucketFolderMap,
					TableToBucketFolderMapLock: &tableToBucketFolderMapLock,
				}
			}
		}()
	})

	var loadFileIDs []int64
	waitChan := make(chan error)
	rruntime.Go(func() {
		func() {
			err = wg.Wait()
			waitChan <- err
		}()
	})
	count := 0
waitForLoadFiles:
	for {
		select {
		case ids := <-ch:
			loadFileIDs = append(loadFileIDs, ids...)
			count++
			logger.Debugf("WH: Processed %v staging files in batch of %v for %s:%s", count, len(job.List), wh.destType, job.Warehouse.Destination.ID)
			logger.Debugf("WH: Received load files with ids: %v for %s:%s", loadFileIDs, wh.destType, job.Warehouse.Destination.ID)
			if count == len(job.List) {
				break waitForLoadFiles
			}
		case err = <-waitChan:
			if err != nil {
				logger.Errorf("WH: Discontinuing processing of staging files for %s:%s due to error: %v", wh.destType, job.Warehouse.Destination.ID, err)
				break waitForLoadFiles
			}
		}
	}
	close(ch)
	timer.End()
	if err != nil {
		warehouseutils.SetStagingFilesError(jsonIDs, warehouseutils.StagingFileFailedState, wh.dbHandle, err)
		warehouseutils.DestStat(stats.CountType, "process_staging_files_failures", job.Warehouse.Destination.ID).Count(len(job.List))
		return err
	}
	warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileSucceededState, wh.dbHandle)
	warehouseutils.DestStat(stats.CountType, "process_staging_files_success", job.Warehouse.Destination.ID).Count(len(job.List))
	warehouseutils.DestStat(stats.CountType, "generate_load_files", job.Warehouse.Destination.ID).Count(len(loadFileIDs))

	sort.Slice(loadFileIDs, func(i, j int) bool { return loadFileIDs[i] < loadFileIDs[j] })
	startLoadFileID := loadFileIDs[0]
	endLoadFileID := loadFileIDs[len(loadFileIDs)-1]

	// update wh_uploads records with end_load_file_id
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, start_load_file_id=$2, end_load_file_id=$3, updated_at=$4 WHERE id=$5`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement, warehouseutils.GeneratedLoadFileState, startLoadFileID, endLoadFileID, time.Now(), job.Upload.ID)
	if err != nil {
		panic(err)
	}

	job.Upload.StartLoadFileID = startLoadFileID
	job.Upload.EndLoadFileID = endLoadFileID
	job.Upload.Status = warehouseutils.GeneratedLoadFileState
	return
}

func (wh *HandleT) SyncLoadFilesToWarehouse(job *ProcessStagingFilesJobT) (err error) {
	logger.Infof("WH: Starting load flow for %s:%s", wh.destType, job.Warehouse.Destination.ID)
	whManager, err := NewWhManager(wh.destType)
	if err != nil {
		panic(err)
	}
	err = whManager.Process(warehouseutils.ConfigT{
		DbHandle:  wh.dbHandle,
		Upload:    job.Upload,
		Warehouse: job.Warehouse,
	})
	return
}

func (wh *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					// handle job to process staging files and convert them into load files
					processStagingFilesJobList := <-wh.uploadToWarehouseQ

					for _, job := range processStagingFilesJobList {
						// generate load files only if not done before
						// upload records have start_load_file_id and end_load_file_id set to 0 on creation
						// and are updated on creation of load files
						logger.Infof("WH: Processing staging files in upload job:%v with staging files from %v to %v for %s:%s", len(job.List), job.List[0].ID, job.List[len(job.List)-1].ID, wh.destType, job.Warehouse.Destination.ID)
						if job.Upload.StartLoadFileID == 0 {
							warehouseutils.SetUploadStatus(job.Upload, warehouseutils.GeneratingLoadFileState, wh.dbHandle)
							err := wh.createLoadFiles(&job)
							if err != nil {
								warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
								warehouseutils.SetUploadError(job.Upload, err, warehouseutils.GeneratingLoadFileFailedState, wh.dbHandle)
								break
							}
						}
						err := wh.SyncLoadFilesToWarehouse(&job)
						if err != nil {
							warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
							break
						}
						warehouseutils.DestStat(stats.CountType, "load_staging_files_into_warehouse", job.Warehouse.Destination.ID).Count(len(job.List))
					}
					setDestInProgress(processStagingFilesJobList[0].Warehouse, false)
				}
			}()
		})
	}
}

// Each Staging File has data for multiple tables in warehouse
// Create separate Load File out of Staging File for each table
func (wh *HandleT) processStagingFile(job LoadFileJobT) (loadFileIDs []int64, err error) {
	logger.Debugf("WH: Starting processing staging file: %v at %v for %s:%s", job.StagingFile.ID, job.StagingFile.Location, wh.destType, job.Warehouse.Destination.ID)
	// download staging file into a temp dir
	dirName := "/rudder-warehouse-json-uploads-tmp/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	jsonPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, wh.destType, job.Warehouse.Destination.ID) + job.StagingFile.Location
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		panic(err)
	}

	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[wh.destType],
		Config:   job.Warehouse.Destination.Config.(map[string]interface{}),
	})
	if err != nil {
		return loadFileIDs, err
	}

	timer := warehouseutils.DestStat(stats.TimerType, "download_staging_file_time", job.Warehouse.Destination.ID)
	timer.Start()

	err = downloader.Download(jsonFile, job.StagingFile.Location)
	if err != nil {
		return loadFileIDs, err
	}
	jsonFile.Close()
	defer os.Remove(jsonPath)
	timer.End()

	fi, err := os.Stat(jsonPath)
	if err != nil {
		logger.Errorf("WH: Error getting file size of downloaded staging file: ", err)
	}
	fileSize := fi.Size()
	logger.Debugf("WH: Downloaded staging file %s size:%v", job.StagingFile.Location, fileSize)
	warehouseutils.DestStat(stats.CountType, "downloaded_staging_file_size", job.Warehouse.Destination.ID).Count(int(fileSize))

	sortedTableColumnMap := make(map[string][]string)
	// sort columns per table so as to maintaing same order in load file (needed in case of csv load file)
	for tableName, columnMap := range job.Schema {
		sortedTableColumnMap[tableName] = []string{}
		for k := range columnMap {
			sortedTableColumnMap[tableName] = append(sortedTableColumnMap[tableName], k)
		}
		sort.Strings(sortedTableColumnMap[tableName])
	}

	logger.Debugf("Starting read from downloaded staging file: %s", job.StagingFile.Location)
	rawf, err := os.Open(jsonPath)
	if err != nil {
		logger.Errorf("WH: Error opening file using os.Open at path:%s downloaded from %s", jsonPath, job.StagingFile.Location)
		panic(err)
	}
	reader, err := gzip.NewReader(rawf)
	if err != nil {
		if err.Error() == "EOF" {
			return loadFileIDs, nil
		}
		logger.Errorf("WH: Error reading file using gzip.NewReader at path:%s downloaded from %s", jsonPath, job.StagingFile.Location)
		panic(err)
	}

	// read from staging file and write a separate load file for each table in warehouse
	// tableContentMap := make(map[string]string)
	outputFileMap := make(map[string]misc.GZipWriter)
	uuidTS := time.Now()
	sc := bufio.NewScanner(reader)
	misc.PrintMemUsage()

	timer = warehouseutils.DestStat(stats.TimerType, "process_staging_file_to_csv_time", job.Warehouse.Destination.ID)
	timer.Start()

	lineBytesCounter := 0
	for sc.Scan() {
		lineBytes := sc.Bytes()
		lineBytesCounter += len(lineBytes)
		var jsonLine map[string]interface{}
		json.Unmarshal(lineBytes, &jsonLine)
		metadata, ok := jsonLine["metadata"]
		if !ok {
			continue
		}
		columnData, ok := jsonLine["data"].(map[string]interface{})
		if !ok {
			continue
		}
		tableName, ok := metadata.(map[string]interface{})["table"].(string)
		if !ok {
			continue
		}
		columns, ok := metadata.(map[string]interface{})["columns"].(map[string]interface{})
		if !ok {
			continue
		}
		if _, ok := outputFileMap[tableName]; !ok {
			outputFilePath := strings.TrimSuffix(jsonPath, "json.gz") + tableName + ".csv.gz"
			gzWriter, err := misc.CreateGZ(outputFilePath)
			defer gzWriter.CloseGZ()
			if err != nil {
				return nil, err
			}
			outputFileMap[tableName] = gzWriter
		}
		if wh.destType == "BQ" {
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == "uuid_ts" {
					// add uuid_ts to track when event was processed into load_file
					columnData["uuid_ts"] = uuidTS.Format("2006-01-02 15:04:05 Z")
					continue
				}
				columnVal, ok := columnData[columnName]
				if !ok {
					continue
				}
				columnType, ok := columns[columnName].(string)
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					columnData[columnName] = int(columnVal.(float64))
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						columnData[columnName] = int(columnVal.(float64))
					} else {
						columnData[columnName] = nil
						continue
					}
				}

			}
			line, err := json.Marshal(columnData)
			if err != nil {
				return loadFileIDs, err
			}
			outputFileMap[tableName].WriteGZ(string(line) + "\n")
		} else {
			csvRow := []string{}
			var buff bytes.Buffer
			csvWriter := csv.NewWriter(&buff)
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == "uuid_ts" {
					// add uuid_ts to track when event was processed into load_file
					csvRow = append(csvRow, fmt.Sprintf("%v", uuidTS.Format(misc.RFC3339Milli)))
					continue
				}
				columnVal, ok := columnData[columnName]
				if !ok {
					csvRow = append(csvRow, "")
					continue
				}

				columnType, ok := columns[columnName].(string)
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					columnVal = int(columnVal.(float64))
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						columnVal = int(columnVal.(float64))
					} else {
						csvRow = append(csvRow, "")
						continue
					}
				}
				csvRow = append(csvRow, fmt.Sprintf("%v", columnVal))
			}
			csvWriter.Write(csvRow)
			csvWriter.Flush()
			outputFileMap[tableName].WriteGZ(buff.String())
		}
	}
	reader.Close()
	timer.End()
	misc.PrintMemUsage()

	logger.Debugf("Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFile.Location)
	warehouseutils.DestStat(stats.CountType, "bytes_processed_in_staging_file", job.Warehouse.Destination.ID).Count(lineBytesCounter)

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[wh.destType],
		Config:   job.Warehouse.Destination.Config.(map[string]interface{}),
	})
	if err != nil {
		panic(err)
	}

	timer = warehouseutils.DestStat(stats.TimerType, "upload_load_files_per_staging_file_time", job.Warehouse.Destination.ID)
	timer.Start()
	for tableName, outputFile := range outputFileMap {
		outputFile.CloseGZ()
		file, err := os.Open(outputFile.File.Name())
		defer os.Remove(outputFile.File.Name())
		logger.Debugf("WH: %s: Uploading load_file to %s for table: %s in staging_file id: %v", wh.destType, warehouseutils.ObjectStorageMap[wh.destType], tableName, job.StagingFile.ID)
		uploadLocation, err := uploader.Upload(file, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.Warehouse.Source.ID, getBucketFolder(job, tableName))
		// tableName, job.Warehouse.Source.ID, fmt.Sprintf(`%v-%v`, strconv.FormatInt(job.Upload.ID, 10), uuid.NewV4().String()))
		if err != nil {
			return loadFileIDs, err
		}
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (staging_file_id, location, source_id, destination_id, destination_type, table_name, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`, warehouseLoadFilesTable)
		stmt, err := wh.dbHandle.Prepare(sqlStatement)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		var fileID int64
		err = stmt.QueryRow(job.StagingFile.ID, uploadLocation.Location, job.StagingFile.SourceID, job.Warehouse.Destination.ID, wh.destType, tableName, time.Now()).Scan(&fileID)
		if err != nil {
			panic(err)
		}
		loadFileIDs = append(loadFileIDs, fileID)
	}
	timer.End()
	return
}

func getBucketFolder(job LoadFileJobT, tableName string) string {
	job.TableToBucketFolderMapLock.Lock()
	defer job.TableToBucketFolderMapLock.Unlock()

	folderName, ok := job.TableToBucketFolderMap[tableName]
	if !ok {
		folderName = fmt.Sprintf(`%v-%v`, strconv.FormatInt(job.Upload.ID, 10), uuid.NewV4().String())
		job.TableToBucketFolderMap[tableName] = folderName
		return folderName
	}
	return folderName
}

func (wh *HandleT) initUploaders() {
	for i := 0; i < noOfWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					makeLoadFilesJob := <-wh.createLoadFilesQ
					timer := warehouseutils.DestStat(stats.TimerType, "process_staging_file_time", makeLoadFilesJob.Warehouse.Destination.ID)
					timer.Start()
					loadFileIDs, err := wh.processStagingFile(makeLoadFilesJob)
					timer.End()
					if err != nil {
						makeLoadFilesJob.Wg.Err(err)
					} else {
						makeLoadFilesJob.LoadFileIDsChan <- loadFileIDs
						makeLoadFilesJob.Wg.Done()
					}
				}
			}()
		})
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
	if err != nil {
		panic(err)
	}

	// change table_name type to text to support table_names upto length 127
	sqlStatement = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE TEXT`, warehouseLoadFilesTable, "table_name")
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseLoadFilesTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_upload_state_type
                                     AS ENUM(
										 	  'waiting',
											  'generating_load_file',
											  'generating_load_file_failed',
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
	if err != nil {
		panic(err)
	}

	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'waiting';`
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'aborted';`
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

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
	if err != nil {
		panic(err)
	}

	// index on status
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_index ON %[1]s (status);`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

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
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseSchemasTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

//Enable enables a router :)
func (wh *HandleT) Enable() {
	wh.isEnabled = true
}

//Disable disables a router:)
func (wh *HandleT) Disable() {
	wh.isEnabled = false
}

func (wh *HandleT) setInterruptedDestinations() (err error) {
	if !misc.Contains(crashRecoverWarehouses, wh.destType) {
		return
	}
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s')`, warehouseUploadsTable, wh.destType, warehouseutils.ExportingDataState, warehouseutils.ExportingDataFailedState)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var destID string
		err := rows.Scan(&destID)
		if err != nil {
			panic(err)
		}
		inRecoveryMap[destID] = true
	}
	return err
}

func (wh *HandleT) Setup(whType string) {
	logger.Infof("WH: Warehouse Router started: %s", whType)
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	wh.dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	wh.setupTables()
	wh.destType = whType
	wh.setInterruptedDestinations()
	wh.Enable()
	wh.uploadToWarehouseQ = make(chan []ProcessStagingFilesJobT)
	wh.createLoadFilesQ = make(chan LoadFileJobT)
	rruntime.Go(func() {
		wh.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		wh.initUploaders()
	})
	rruntime.Go(func() {
		wh.initWorkers()
	})
	rruntime.Go(func() {
		wh.mainLoop()
	})
}
