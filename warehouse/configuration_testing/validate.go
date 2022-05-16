package configuration_testing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func (ct *CTHandleT) validateDestinationFunc(req json.RawMessage, step string) (json.RawMessage, error) {
	ct.infoRequest = &DestinationValidationRequest{}
	if err := parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}

	destination := ct.infoRequest.Destination
	destinationID := destination.ID
	destinationType := destination.DestinationDefinition.Name

	pkgLogger.Infof("Validating destination configuration for destinationId: %s, destinationType: %s, step: %s",
		destinationID,
		destinationType,
		step,
	)

	resp := DestinationValidationResponse{}
	// check if req has specified a step in query params
	if step != "" {
		stepI, err := strconv.Atoi(step)
		if err != nil {
			resp.Error = fmt.Sprintf("%s: %s", warehouseutils.CTInvalidStep, step)
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
			resp.Error = fmt.Sprintf("%s: %s", warehouseutils.CTInvalidStep, step)
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
				destinationID,
				destinationType,
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
	tempPath, err := createLoadFile(ct.infoRequest)
	if err != nil {
		return
	}

	// uploading load file to object storage
	uploadOutput, err := uploadLoadFile(ct.infoRequest, tempPath)
	if err != nil {
		return
	}

	// downloading load file from object storage
	err = downloadLoadFile(ct.infoRequest, uploadOutput.ObjectName)
	return
}

func (ct *CTHandleT) initManager() (err error) {
	ct.warehouse = warehouse(ct.infoRequest)

	// Initializing manager
	ct.manager, err = manager.NewWarehouseOperations(ct.warehouse.Destination.DestinationDefinition.Name)
	if err != nil {
		return
	}

	// Setting test connection timeout
	ct.manager.SetConnectionTimeout(warehouseutils.TestConnectionTimeout)

	// setting up the manager
	err = ct.manager.Setup(ct.warehouse, &CTUploadJob{
		infoRequest: ct.infoRequest,
	})
	return
}

func (ct *CTHandleT) verifyingConnections() (err error) {
	err = ct.initManager()
	if err != nil {
		return
	}

	err = ct.manager.TestConnection(ct.warehouse)
	return
}

func (ct *CTHandleT) verifyingCreateSchema() (err error) {
	err = ct.initManager()
	if err != nil {
		return
	}

	err = ct.manager.CreateSchema()
	return
}

func (ct *CTHandleT) verifyingCreateAlterTable() (err error) {
	err = ct.initManager()
	if err != nil {
		return
	}

	stagingTableName := stagingTableName()

	// Create table
	err = ct.manager.CreateTable(stagingTableName, TestTableSchemaMap)
	if err != nil {
		return
	}

	// Drop table
	defer ct.manager.DropTable(stagingTableName)

	// Alter table
	for columnName, columnType := range AlterColumnMap {
		err = ct.manager.AddColumn(stagingTableName, columnName, columnType)
		if err != nil {
			return
		}
	}
	return
}

func (ct *CTHandleT) verifyingFetchSchema() (err error) {
	err = ct.initManager()
	if err != nil {
		return
	}

	_, err = ct.manager.FetchSchema(ct.warehouse)
	return
}

func (ct *CTHandleT) verifyingLoadTable() (err error) {
	err = ct.initManager()
	if err != nil {
		return
	}

	// creating load file
	tempPath, err := createLoadFile(ct.infoRequest)
	if err != nil {
		return
	}

	// uploading load file
	uploadOutput, err := uploadLoadFile(ct.infoRequest, tempPath)
	if err != nil {
		return
	}

	// loading table
	err = ct.loadTable(uploadOutput.Location)
	return
}

func createLoadFile(req *DestinationValidationRequest) (filePath string, err error) {
	destination := req.Destination
	destinationType := destination.DestinationDefinition.Name

	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to create tmp dir with error: %s", err.Error())
		return
	}

	// creating file path for temporary file
	filePath = fmt.Sprintf("%v/%v/%v.%v.%v", tmpDirPath, connectionTestingFolder, destinationType, time.Now().Unix(), warehouseutils.GetLoadFileFormat(destinationType))
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to make dir filePath: %s with error: %s", filePath, err.Error())
		return
	}

	// creating writer for writing to temporary file based on file type
	var writer warehouseutils.LoadFileWriterI
	if warehouseutils.GetLoadFileType(destinationType) == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		writer, err = warehouseutils.CreateParquetWriter(TestTableSchemaMap, filePath, destinationType)
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

	// closing writer
	err = writer.Close()
	if err != nil {
		pkgLogger.Errorf("[WH]: Error while closing load file with error: %s", err.Error())
		return
	}
	return
}

func uploadLoadFile(req *DestinationValidationRequest, filePath string) (uploadOutput filemanager.UploadOutput, err error) {
	destination := req.Destination
	destinationType := destination.DestinationDefinition.Name

	// getting file manager
	fm, err := fileManager(req)
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
	keyPrefixes := []string{connectionTestingFolder, destinationType, randomString(), time.Now().Format("01-02-2006")}
	uploadOutput, err = fm.Upload(context.TODO(), uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to upload filePath: %s with error: %s", filePath, err.Error())
		return
	}
	return uploadOutput, err
}

func downloadLoadFile(req *DestinationValidationRequest, location string) (err error) {
	destination := req.Destination
	destinationType := destination.DestinationDefinition.Name

	// getting file manager
	fm, err := fileManager(req)
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
	testFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.%v", tmpDirPath, connectionTestingFolder, destinationType, randomString(), time.Now().Unix(), warehouseutils.GetLoadFileFormat(destinationType))
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
	err = fm.Download(context.TODO(), testFile, location)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to download tempFilePath: %s with error: %s", location, err.Error())
		return
	}
	return
}

func (ct *CTHandleT) loadTable(loadFileLocation string) (err error) {
	destination := ct.infoRequest.Destination
	destinationType := destination.DestinationDefinition.Name

	stagingTableName := stagingTableName()

	// Create table
	err = ct.manager.CreateTable(stagingTableName, TestTableSchemaMap)
	if err != nil {
		return
	}

	// Drop table
	defer ct.manager.DropTable(stagingTableName)

	// loading test table from staging file
	err = ct.manager.LoadTestTable(loadFileLocation, stagingTableName, TestPayloadMap, warehouseutils.GetLoadFileFormat(destinationType))
	return
}
