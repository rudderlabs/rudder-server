package ingest

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	configSubscriberLock sync.RWMutex
	inProgressMap        map[string]bool
	inRecoveryMap        map[string]bool
	inProgressMapLock    sync.RWMutex
	lastExecMap          map[string]int64
	lastExecMapLock      sync.RWMutex
)

type ProcessStagingFilesJobT struct {
	Upload    warehouseutils.UploadT
	List      []*StagingFileT
	Warehouse warehouseutils.WarehouseT
}

type StagingFileT struct {
	ID        int64
	Location  string
	SourceID  string
	Schema    json.RawMessage
	Status    string // enum
	CreatedAt time.Time
}

func (ig *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status= '%[4]s' OR %[1]s.status = '%[5]s') ORDER BY %[1]s.id DESC`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)
	err := ig.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := ig.dbHandle.Query(sqlStatement)
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

func (ig *HandleT) jobpush(warehouse warehouseutils.WarehouseT) {
	destType := warehouse.Destination.DestinationDefinition.Name

	// fetch any pending wh_uploads records (query for not successful/aborted uploads)
	// pendingUploads, ok := wh.getPendingUploads(warehouse)
	ok := false
	if ok {
		// logger.Infof("WH: Found pending uploads: %v for %s:%s", len(pendingUploads), destType, warehouse.Destination.ID)
		// jobs := []ProcessStagingFilesJobT{}
		// for _, pendingUpload := range pendingUploads {
		// 	stagingFilesList, err := wh.getStagingFiles(warehouse, pendingUpload.StartStagingFileID, pendingUpload.EndStagingFileID)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	jobs = append(jobs, ProcessStagingFilesJobT{
		// 		List:      stagingFilesList,
		// 		Warehouse: warehouse,
		// 		Upload:    pendingUpload,
		// 	})
		// }
		// wh.uploadToWarehouseQ <- jobs
	} else {
		// fetch staging files that are not processed yet
		stagingFilesList, err := ig.getPendingStagingFiles(warehouse)
		if err != nil {
			panic(err)
		}
		if len(stagingFilesList) == 0 {
			logger.Debugf("WH: Found no pending staging files for %s:%s", destType, warehouse.Destination.ID)
			setDestInProgress(warehouse, false)
			//TODO:
			//continue
			return
		}
		logger.Infof("WH: Found %v pending staging files for %s:%s", len(stagingFilesList), destType, warehouse.Destination.ID)

		count := 0
		jobs := []ProcessStagingFilesJobT{}
		// process staging files in batches of stagingFilesBatchSize
		for {

			lastIndex := count + stagingFilesBatchSize
			if lastIndex >= len(stagingFilesList) {
				lastIndex = len(stagingFilesList)
			}
			// merge schemas over all staging files in this batch
			consolidatedSchema := ig.consolidateSchema(warehouse, stagingFilesList[count:lastIndex])
			// create record in wh_uploads to mark start of upload to warehouse flow
			upload := ig.initUpload(warehouse, stagingFilesList[count:lastIndex], consolidatedSchema)
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
		ig.UpdateJobQueue(jobs)
		// wh.uploadToWarehouseQ <- jobs
	}
}

func (ig *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, schema map[string]map[string]string) warehouseutils.UploadT {
	destType := warehouse.Destination.DestinationDefinition.Name

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13) RETURNING id`, warehouseUploadsTable)
	logger.Infof("WH: %s: Creating record in wh_load_files id: %v", destType, sqlStatement)
	stmt, err := ig.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	currentSchema, err := json.Marshal(schema)
	namespace := misc.TruncateStr(strings.ToLower(strcase.ToSnake(warehouse.Source.Name)), 127)
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, destType, startJSONID, endJSONID, 0, 0, warehouseutils.WaitingState, currentSchema, "{}", time.Now(), time.Now())

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
		DestinationType:    destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             warehouseutils.WaitingState,
		Schema:             schema,
	}
}

func (ig *HandleT) consolidateSchema(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaInDB, err := warehouseutils.GetCurrentSchema(ig.dbHandle, warehouse)
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
	// TODO: remove hardcoded value
	var uploadFreqInS int64
	uploadFreqInS = 10
	if lastExecTime, ok := lastExecMap[connectionString(warehouse)]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	lastExecMap[connectionString(warehouse)] = time.Now().Unix()
	return false
}

func (ig *HandleT) BeginETLbatch(warehouses []warehouseutils.WarehouseT) {
	// for {
	// 	// if !wh.isEnabled {
	// 	// 	time.Sleep(mainLoopSleep)
	// 	// 	continue
	// 	// }

	for _, warehouse := range warehouses {
		fmt.Printf("%+v\n", warehouse)
		destType := warehouse.Destination.DestinationDefinition.Name
		if isDestInProgress(warehouse) {
			logger.Debugf("WH: Skipping upload loop since %s:%s upload in progess", destType, warehouse.Destination.ID)
			continue
		}
		if uploadFrequencyExceeded(warehouse) {
			logger.Debugf("WH: Skipping upload loop since %s:%s upload freq not exceeded", destType, warehouse.Destination.ID)
			continue
		}
		setDestInProgress(warehouse, true)

		// _, ok := inRecoveryMap[warehouse.Destination.ID]
		// if ok {
		// 	whManager, err := NewWhManager(destType)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	logger.Infof("WH: Crash recovering for %s:%s", destType, warehouse.Destination.ID)
		// 	err = whManager.CrashRecover(warehouseutils.ConfigT{
		// 		DbHandle:  wh.dbHandle,
		// 		Warehouse: warehouse,
		// 	})
		// 	if err != nil {
		// 		setDestInProgress(warehouse, false)
		// 		continue
		// 	}
		// 	delete(inRecoveryMap, warehouse.Destination.ID)
		// }

		ig.jobpush(warehouse)
	}
	// 	time.Sleep(2)
	// }
}
