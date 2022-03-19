package configuration_testing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func (ct *CTHandleT) validateDestinationFunc(ctx context.Context, req json.RawMessage, step string) (json.RawMessage, error) {
	ct.infoRequest = &infoRequest{}
	if err := ct.parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}

	resp := validationResponse{}

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
		result := s.Validator(ctx, &validationRequest{
			validationStep: s,
			infoRequest:    ct.infoRequest,
		})
		resp.Steps[idx] = result

		// if any of steps fails, the whole validation fails
		if !result.Success {
			resp.Error = result.Error
			break
		}
	}

	if resp.Error == "" {
		resp.Success = true
	}

	return json.Marshal(resp)
}

func (ct *CTHandleT) verifyingObjectStorage(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := ct.verifyObjectStorage()
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (ct *CTHandleT) verifyingConnections(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := ct.verifyConnections()
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (ct *CTHandleT) verifyingCreateSchema(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := ct.verifyCreateSchema()
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (ct *CTHandleT) verifyingCreateTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := ct.verifyCreateTable()
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (ct *CTHandleT) verifyingLoadTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := ct.verifyLoadTable()
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (ct *CTHandleT) verifyObjectStorage() (err error) {
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

func (ct *CTHandleT) createLoadFile() (filePath string, err error) {
	destination := ct.infoRequest.Destination

	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to create tmp dir for testing destinationID: %s with error: %s", destination.ID, err.Error())
		return
	}

	// creating file path for temporary file
	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		connectionTestingFolder,
		ct.infoRequest.Destination.ID,
		uuid.Must(uuid.NewV4()),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(destination.Name),
	)
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to make dir %s for testing destinationID: %s with error: %s", filePath, destination.ID, err.Error())
		return
	}

	// creating writer for writing to temporary file based on file type
	var writer warehouseutils.LoadFileWriterI
	if warehouseutils.GetLoadFileType(destination.DestinationDefinition.Name) == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		writer, err = warehouseutils.CreateParquetWriter(TestTableSchemaMap, filePath, destination.DestinationDefinition.Name)
	} else {
		writer, err = misc.CreateGZ(filePath)
	}
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to create writer for testing destinationID: %s with error: %s", destination.ID, err.Error())
		return
	}

	// creating event loader to add columns to temporary file
	eventLoader := warehouseutils.GetNewEventLoader(destination.DestinationDefinition.Name, warehouseutils.GetLoadFileType(destination.DestinationDefinition.Name), writer)
	for columnName, columnValue := range TestPayloadMap {
		eventLoader.AddColumn(columnName, TestTableSchemaMap[columnName], columnValue)
	}

	// writing to file
	err = eventLoader.Write()
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to write event for testing destinationID: %s with error: %s", destination.ID, err.Error())
	}
	return
}

func (ct *CTHandleT) uploadLoadFile(filePath string) (uploadOutput filemanager.UploadOutput, err error) {
	destination := ct.infoRequest.Destination

	// getting file manager
	uploader, err := ct.fileManagerAdapter()
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing destinationID: %s with error: %s", destination.ID, err.Error())
		return
	}

	// opening file at temporary location
	uploadFile, err := os.Open(filePath)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to open file %s for testing destinationID: %s with error: %s", filePath, destination.ID, err)
		return
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer uploadFile.Close()

	// uploading file to object storage
	keyPrefixes := []string{connectionTestingFolder, destination.ID, time.Now().Format("01-02-2006")}
	uploadOutput, err = uploader.Upload(context.TODO(), uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to upload test file %s for testing destinationID: %s with error: %s", filePath, destination.ID, err)
		return
	}
	return uploadOutput, err
}

func (ct *CTHandleT) downloadLoadFile(location string) (err error) {
	destination := ct.infoRequest.Destination

	// getting file manager
	downloader, err := ct.fileManagerAdapter()
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}

	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create temp directory for testing this destination id %s: err %v", destination.ID, err)
		return
	}

	// creating file path for temporary file
	testFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		connectionTestingFolder,
		destination.ID,
		uuid.Must(uuid.NewV4()),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(destination.DestinationDefinition.Name),
	)
	err = os.MkdirAll(filepath.Dir(testFilePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create directory at path %s: err %v", testFilePath, err)
		return
	}

	// creating temporary file
	testFile, err := os.Create(testFilePath)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create tempFilePath: %s for testing this destination id %s: err %v", testFilePath, destination.ID, err)
		return
	}

	// cleanup
	defer misc.RemoveFilePaths(testFilePath)
	defer testFile.Close()

	// downloading temporary file to specified from object storage location
	err = downloader.Download(context.TODO(), testFile, location)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to download test file %s for testing this destination id %s: err %v", location, destination.ID, err)
		return
	}
	return
}

func (ct *CTHandleT) verifyConnections() (err error) {
	destinationType := ct.infoRequest.Destination.DestinationDefinition.Name

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}

	// calling test connection on warehouse
	err = whManager.TestConnection(ct.warehouseAdapter())
	return
}

func (ct *CTHandleT) verifyCreateSchema() (err error) {
	destinationType := ct.infoRequest.Destination.DestinationDefinition.Name

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}
	ct.warehouse = ct.warehouseAdapter()

	// Getting warehouse client
	ct.client, err = whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}
	defer ct.client.Close()

	if destinationType == "BQ" {
		bqHandle := bigquery.HandleT{}
		return bqHandle.VerifyCreateSchema(&ct.client, ct.warehouse, context.TODO())
	}

	// Creating schema query and running over the warehouse
	rows, err := ct.client.QueryWrite(ct.CreateSchemaQuery())
	fmt.Println(rows)
	return
}

func (ct *CTHandleT) verifyCreateTable() (err error) {
	destinationType := ct.infoRequest.Destination.DestinationDefinition.Name

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}
	ct.warehouse = ct.warehouseAdapter()

	// Getting warehouse client
	ct.client, err = whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}
	defer ct.client.Close()

	createTableQuery := ct.CreateTableQuery()
	dropTableQuery := ct.DropTableQuery()

	if destinationType == "BQ" {
		bqHandle := bigquery.HandleT{}
		return bqHandle.VerifyCreateTable(&ct.client, ct.warehouse, ct.stagingTableName, TestTableSchemaMap, context.TODO())
	}

	// Creating create table query and running over the warehouse
	rows, err := ct.client.QueryWrite(createTableQuery)
	fmt.Println(rows)
	if err != nil {
		return
	}

	// Creating drop table query and running over the warehouse
	_, err = ct.client.QueryWrite(dropTableQuery)
	return
}

func (ct *CTHandleT) verifyLoadTable() (err error) {
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

func (ct *CTHandleT) loadTable(loadFileLocation string) (err error) {
	destination := ct.infoRequest.Destination
	destinationType := destination.DestinationDefinition.Name

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}
	ct.warehouse = ct.warehouseAdapter()

	// Getting warehouse client
	ct.client, err = whManager.Connect(ct.warehouse)
	if err != nil {
		return
	}
	defer ct.client.Close()

	createTableQuery := ct.CreateTableQuery()
	dropTableQuery := ct.DropTableQuery()

	// Creating create table query and running over the warehouse
	_, err = ct.client.QueryWrite(createTableQuery)
	if err != nil {
		return
	}

	// getting load file format
	loadFileFormat := warehouseutils.GetLoadFileFormat(destination.DestinationDefinition.Name)

	// loading test table from staging file
	err = whManager.LoadTestTable(&ct.client, loadFileLocation, ct.warehouseAdapter(), ct.stagingTableName, TestTableSchemaMap, TestPayloadMap, loadFileFormat)

	// Creating drop table query and running over the warehouse
	_, err = ct.client.QueryWrite(dropTableQuery)
	return
}
