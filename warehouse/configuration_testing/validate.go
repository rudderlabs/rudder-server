package configuration_testing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func (ct *CTHandleT) validateDestinationFunc(req json.RawMessage, step string) (json.RawMessage, error) {
	ct.infoRequest = &DestinationValidationRequest{}
	if err := ct.parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}
	pkgLogger.Infof("Validating destination configuration for destinationId: %s, destinationType: %s, step: %s",
		ct.infoRequest.Destination.ID,
		ct.GetDestinationType(),
		step,
	)

	// Getting warehouse manager
	var err error
	if ct.whManager, err = manager.New(ct.GetDestinationType()); err != nil {
		return nil, err
	}
	ct.warehouse = ct.warehouseAdapter()

	resp := DestinationValidationResponse{}
	// check if req has specified a step in query params
	if step != "" {
		stepI, err := strconv.Atoi(step)
		if err != nil {
			resp.Error = fmt.Sprintf("%s: %s", InvalidStep, step)
			return json.Marshal(resp)
		}

		// get validation step
		var v *validationStep
		for _, s := range ct.validationSteps() {
			if s.ID == stepI {
				v = s
				break
			}
		}

		if v == nil {
			resp.Error = fmt.Sprintf("%s: %s", InvalidStep, step)
			return json.Marshal(resp)
		}

		resp.Steps = append(resp.Steps, v)
	} else {
		resp.Steps = ct.validationSteps()
	}

	// Iterate over all selected steps and validate
	for idx, s := range resp.Steps {
		stepError := s.Validator()
		if stepError != nil {
			resp.Steps[idx].Error = stepError.Error()
			pkgLogger.Errorf("error occurred while destination configuration validation for destinationId: %s, destinationType: %s, step: %s with error: %s",
				ct.infoRequest.Destination.ID,
				ct.GetDestinationType(),
				s.Name,
				stepError.Error(),
			)
		} else {
			resp.Steps[idx].Success = true
		}

		// if any of steps fails, the whole validation fails
		if !resp.Steps[idx].Success {
			resp.Error = resp.Steps[idx].Error
			break
		}
	}

	if resp.Error == "" {
		resp.Success = true
	}

	return json.Marshal(resp)
}

func (ct *CTHandleT) verifyingObjectStorage() (err error) {
	// creating load file
	tempPath, err := ct.createLoadFile()
	if err != nil {
		return
	}

	// uploading load file to object storage
	uploadOutput, err := ct.uploadLoadFile(tempPath)
	if err != nil {
		return
	}

	// downloading load file from object storage
	err = ct.downloadLoadFile(uploadOutput.ObjectName)
	return
}

func (ct *CTHandleT) verifyingConnections() (err error) {
	// Getting warehouse manager
	whManager, err := manager.New(ct.GetDestinationType())
	if err != nil {
		return
	}

	// calling test connection on warehouse
	err = whManager.TestConnection(ct.warehouseAdapter())
	return
}

func (ct *CTHandleT) verifyingCreateSchema() (err error) {
	// Getting warehouse client
	ct.client, err = ct.whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}
	defer ct.client.Close()

	if ct.GetDestinationType() == warehouseutils.BQ {
		bqHandle := ct.GetBigQueryHandle()
		return bqHandle.CreateSchema()
	}

	// Creating schema query and running over the warehouse
	_, err = ct.client.Query(ct.CreateSchemaQuery(), client.Write)
	return
}

func (ct *CTHandleT) verifyingCreateTable() (err error) {
	// Getting warehouse client
	ct.client, err = ct.whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}

	// Cleanup
	defer ct.cleanup()

	// Create table
	err = ct.createTable()
	if err != nil {
		return
	}

	// Alter table
	err = ct.alterTable()
	return
}

func (ct *CTHandleT) verifyingFetchSchema() (err error) {
	// Getting warehouse client
	ct.client, err = ct.whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}
	// Creating create table query and running over the warehouse
	if ct.GetDestinationType() == warehouseutils.DELTALAKE {
		dbHandle := ct.GetDatabricksHandle()
		_, err = dbHandle.FetchSchema(ct.warehouse)
	} else {
		_, err = ct.client.Query(ct.FetchSchemaQuery(), client.Read)
	}
	return
}

func (ct *CTHandleT) verifyingLoadTable() (err error) {
	// creating load file
	tempPath, err := ct.createLoadFile()
	if err != nil {
		return
	}

	// uploading load file
	uploadOutput, err := ct.uploadLoadFile(tempPath)
	if err != nil {
		return
	}

	// loading table
	err = ct.loadTable(uploadOutput.Location)
	return
}

func (ct *CTHandleT) createLoadFile() (filePath string, err error) {
	destination := ct.infoRequest.Destination

	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to create tmp dir with error: %s", err.Error())
		return
	}

	// creating file path for temporary file
	filePath = fmt.Sprintf("%v/%v/%v.%v.%v", tmpDirPath, connectionTestingFolder, ct.GetDestinationType(), time.Now().Unix(), warehouseutils.GetLoadFileFormat(ct.GetDestinationType()))
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to make dir filePath: %s with error: %s", filePath, err.Error())
		return
	}

	// creating writer for writing to temporary file based on file type
	var writer warehouseutils.LoadFileWriterI
	if warehouseutils.GetLoadFileType(ct.GetDestinationType()) == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		writer, err = warehouseutils.CreateParquetWriter(TestTableSchemaMap, filePath, ct.GetDestinationType())
	} else {
		writer, err = misc.CreateGZ(filePath)
	}
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to create writer with error: %s", err.Error())
		return
	}

	// creating event loader to add columns to temporary file
	eventLoader := warehouseutils.GetNewEventLoader(destination.DestinationDefinition.Name, warehouseutils.GetLoadFileType(destination.DestinationDefinition.Name), writer)
	eventLoader.AddColumn("id", TestTableSchemaMap["id"], TestPayloadMap["id"])
	eventLoader.AddColumn("val", TestTableSchemaMap["val"], TestPayloadMap["val"])

	// writing to file
	err = eventLoader.Write()
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to write event with error: %s", err.Error())
		return
	}
	err = writer.Close()
	if err != nil {
		pkgLogger.Errorf("[WH]: Error while closing load file with error: %s", err.Error())
		return
	}
	return
}

func (ct *CTHandleT) uploadLoadFile(filePath string) (uploadOutput filemanager.UploadOutput, err error) {
	// getting file manager
	uploader, err := ct.fileManagerAdapter()
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager with error: %s", err.Error())
		return
	}

	// opening file at temporary location
	uploadFile, err := os.Open(filePath)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to open filePath: %s with error: %s", filePath, err.Error())
		return
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer uploadFile.Close()

	// uploading file to object storage
	keyPrefixes := []string{connectionTestingFolder, ct.GetDestinationType(), GetRandomString(), time.Now().Format("01-02-2006")}
	uploadOutput, err = uploader.Upload(context.TODO(), uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to upload filePath: %s with error: %s", filePath, err.Error())
		return
	}
	return uploadOutput, err
}

func (ct *CTHandleT) downloadLoadFile(location string) (err error) {
	// getting file manager
	downloader, err := ct.fileManagerAdapter()
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config with error: %s", err.Error())
		return
	}

	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create temp directory with error: %s", err.Error())
		return
	}

	// creating file path for temporary file
	testFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.%v", tmpDirPath, connectionTestingFolder, ct.GetDestinationType(), GetRandomString(), time.Now().Unix(), warehouseutils.GetLoadFileFormat(ct.GetDestinationType()))
	err = os.MkdirAll(filepath.Dir(testFilePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create directory at tempFilePath %s: with error: %s", testFilePath, err.Error())
		return
	}

	// creating temporary file
	testFile, err := os.Create(testFilePath)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create tempFilePath: %s with error: %s", testFilePath, err.Error())
		return
	}

	// cleanup
	defer misc.RemoveFilePaths(testFilePath)
	defer testFile.Close()

	// downloading temporary file to specified from object storage location
	err = downloader.Download(context.TODO(), testFile, location)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to download tempFilePath: %s with error: %s", location, err.Error())
		return
	}
	return
}

func (ct *CTHandleT) loadTable(loadFileLocation string) (err error) {
	// Getting warehouse client
	ct.client, err = ct.whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}

	// Cleanup
	defer ct.cleanup()

	// Create table
	err = ct.createTable()
	if err != nil {
		return
	}

	// loading test table from staging file
	err = ct.whManager.LoadTestTable(&ct.client, loadFileLocation, ct.warehouseAdapter(), ct.stagingTableName, TestPayloadMap, warehouseutils.GetLoadFileFormat(ct.GetDestinationType()))
	return
}

func (ct *CTHandleT) createTable() (err error) {
	// Set staging table name
	ct.stagingTableName = fmt.Sprintf(`%s%s`,
		StagingTablePrefix,
		GetRandomString(),
	)
	// Creating create table query and running over the warehouse
	if ct.GetDestinationType() == warehouseutils.BQ {
		bqHandle := ct.GetBigQueryHandle()
		err = bqHandle.CreateTable(ct.stagingTableName, TestTableSchemaMap)
	} else {
		_, err = ct.client.Query(ct.CreateTableQuery(), client.Write)
	}
	return
}

func (ct *CTHandleT) alterTable() (err error) {
	// Creating alter table query and running over the warehouse
	if ct.GetDestinationType() == warehouseutils.BQ {
		bqHandle := ct.GetBigQueryHandle()
		for columnName, columnType := range AlterColumnMap {
			err = bqHandle.AddColumn(ct.stagingTableName, columnName, columnType)
		}
	} else {
		_, err = ct.client.Query(ct.AlterTableQuery(), client.Write)
	}
	return
}

func (ct *CTHandleT) cleanup() {
	// Dropping table
	if ct.GetDestinationType() == warehouseutils.BQ {
		bqHandle := ct.GetBigQueryHandle()
		bqHandle.DeleteTable(ct.stagingTableName)
		bqHandle.DeleteTable(ct.stagingTableName + "_view")
	} else {
		ct.client.Query(ct.DropTableQuery(), client.Write)
	}

	// Closing connection
	ct.client.Close()
}
