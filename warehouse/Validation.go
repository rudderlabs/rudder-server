package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	rudderConnectionTestingFolder string
)

const (
	InvalidStep = "Invalid step"
)

var (
	TestTableSchemaMap = map[string]string{
		"id":  "int",
		"val": "string",
	}
	TestPayloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	TestNamespace = "_rudderstack_setup_test"
)

type validationFunc struct {
	Path string
	Func func(context.Context, json.RawMessage, string) (json.RawMessage, error)
}

type infoRequest struct {
	Destination backendconfig.DestinationT `json:"destination"`
}

type validationRequest struct {
	validationStep *validationStep
	infoRequest    *infoRequest
}

type validationResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Steps   []*validationStep `json:"steps"`
}

type validationStep struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Success   bool      `json:"success"`
	Error     string    `json:"error"`
	Validator validator `json:"-"`
}

type validator func(ctx context.Context, req *validationRequest) *validationStep

type validationStepsResponse struct {
	Steps []*validationStep `json:"steps"`
}

func Init7() {
	rudderConnectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
}

func (w *warehousegrpc) Validating(ctx context.Context, req *proto.WHValidationRequest, _ UploadAPIT) (*proto.WHValidationResponse, error) {
	validationFunctions := w.getValidationFunctions()
	f, ok := validationFunctions[req.Path]
	if !ok {
		return nil, errors.New("path not found")
	}
	step := req.Step
	result, err := f.Func(ctx, json.RawMessage(req.Body), step)
	errorMessage := ""
	if err != nil {
		errorMessage = err.Error()
	}
	return &proto.WHValidationResponse{
		Error: errorMessage,
		Data:  string(result),
	}, nil
}

// TODO: Register these functions in mux so that we can call these using rudder-cli
func (w *warehousegrpc) getValidationFunctions() map[string]*validationFunc {
	return map[string]*validationFunc{
		"validate": {
			Path: "/validate",
			Func: w.validateDestinationFunc,
		},
		"steps": {
			Path: "/steps",
			Func: w.validationStepsFunc,
		},
	}
}

func (w *warehousegrpc) getValidationSteps() []*validationStep {
	return []*validationStep{
		{
			ID:        1,
			Name:      "Verifying Object Storage",
			Validator: w.verifyingObjectStorage,
		},
		{
			ID:        2,
			Name:      "Verifying Connections",
			Validator: w.verifyingConnections,
		},
		{
			ID:        3,
			Name:      "Verifying Create Schema",
			Validator: w.verifyingCreateSchema,
		},
		{
			ID:        4,
			Name:      "Verifying Create Table",
			Validator: w.verifyingCreateTable,
		},
		{
			ID:        5,
			Name:      "Verifying Load Table",
			Validator: w.verifyingLoadTable,
		},
	}
}

func (w *warehousegrpc) validationStepsFunc(_ context.Context, _ json.RawMessage, _ string) (json.RawMessage, error) {
	return json.Marshal(validationStepsResponse{
		Steps: w.getValidationSteps(),
	})
}

func (w *warehousegrpc) validateDestinationFunc(ctx context.Context, req json.RawMessage, step string) (json.RawMessage, error) {
	infoRequest := &infoRequest{}
	if err := parseOptions(req, infoRequest); err != nil {
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
		for _, s := range w.getValidationSteps() {
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
		resp.Steps = w.getValidationSteps()
	}

	// Iterate over all selected steps and validate
	for idx, s := range resp.Steps {
		result := s.Validator(ctx, &validationRequest{
			validationStep: s,
			infoRequest:    infoRequest,
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

func (w *warehousegrpc) verifyingObjectStorage(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyObjectStorage(&vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) verifyingConnections(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyConnections(&vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) verifyingCreateSchema(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyCreateSchema(&vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) verifyingCreateTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyCreateTable(&vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) verifyingLoadTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyLoadTable(&vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

/*
	Helper functions
*/
func verifyObjectStorage(dest *backendconfig.DestinationT) (err error) {
	// creating load file
	tempPath, err := createLoadFile(dest)
	if err != nil {
		return
	}

	// uploading load file to object storage
	uploadOutput, err := uploadLoadFile(tempPath, dest)
	if err != nil {
		return
	}

	// downloading load file from object storage
	err = downloadLoadFile(uploadOutput.ObjectName, dest)
	return
}

func createLoadFile(destination *backendconfig.DestinationT) (filePath string, err error) {
	// creating temp directory path
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to create tmp dir for testing destinationID: %s with error: %s", destination.ID, err.Error())
		return
	}

	// creating file path for temporary file
	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		rudderConnectionTestingFolder,
		destination.ID,
		uuid.Must(uuid.NewV4()),
		time.Now().Unix(),
		getLoadFileFormat(destination.DestinationDefinition.Name),
	)
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("[DCT] Failed to make dir %s for testing destinationID: %s with error: %s", filePath, destination.ID, err.Error())
		return
	}

	// creating writer for writing to temporary file based on file type
	var writer warehouseutils.LoadFileWriterI
	if getLoadFileType(destination.DestinationDefinition.Name) == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		writer, err = warehouseutils.CreateParquetWriter(TestTableSchemaMap, filePath, destination.DestinationDefinition.Name)
	} else {
		writer, err = misc.CreateGZ(filePath)
	}
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to create writer for testing destinationID: %s with error: %s", destination.ID, err.Error())
		return
	}

	// creating event loader to add columns to temporary file
	eventLoader := warehouseutils.GetNewEventLoader(destination.DestinationDefinition.Name, getLoadFileType(destination.DestinationDefinition.Name), writer)
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

func uploadLoadFile(filePath string, destination *backendconfig.DestinationT) (uploadOutput filemanager.UploadOutput, err error) {
	// getting file manager
	uploader, err := fileManagerAdapter(destination)
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
	keyPrefixes := []string{rudderConnectionTestingFolder, destination.ID, time.Now().Format("01-02-2006")}
	uploadOutput, err = uploader.Upload(uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to upload test file %s for testing destinationID: %s with error: %s", filePath, destination.ID, err)
		return
	}
	return uploadOutput, err
}

func downloadLoadFile(location string, destination *backendconfig.DestinationT) (err error) {
	// getting file manager
	downloader, err := fileManagerAdapter(destination)
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
		rudderConnectionTestingFolder,
		destination.ID,
		uuid.Must(uuid.NewV4()),
		time.Now().Unix(),
		getLoadFileFormat(destination.DestinationDefinition.Name),
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
	err = downloader.Download(testFile, location)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to download test file %s for testing this destination id %s: err %v", location, destination.ID, err)
		return
	}
	return
}

func verifyConnections(destination *backendconfig.DestinationT) (err error) {
	destinationType := destination.DestinationDefinition.Name

	// no need to do for time window destinations
	if misc.ContainsString(timeWindowDestinations, destinationType) {
		return
	}

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}

	// calling test connection on warehouse
	err = whManager.TestConnection(warehouseAdapter(destination))
	return
}

func verifyCreateSchema(destination *backendconfig.DestinationT) (err error) {
	destinationType := destination.DestinationDefinition.Name

	// no need to do for time window destinations
	if misc.ContainsString(timeWindowDestinations, destinationType) {
		return
	}

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}

	// calling test schema
	err = whManager.CreateTestSchema(warehouseAdapter(destination))
	return
}

func verifyCreateTable(destination *backendconfig.DestinationT) (err error) {
	destinationType := destination.DestinationDefinition.Name

	// no need to do for time window destinations
	if misc.ContainsString(timeWindowDestinations, destinationType) {
		return
	}

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}

	// preparing staging table name
	stagingTableName := fmt.Sprintf(`%s%s`,
		"setup_test_staging_",
		strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""),
	)

	// creating test table
	err = whManager.CreateTestTable(warehouseAdapter(destination), stagingTableName, TestTableSchemaMap)
	return
}

func verifyLoadTable(destination *backendconfig.DestinationT) (err error) {
	// creating load file
	tempPath, err := createLoadFile(destination)
	if err != nil {
		return
	}

	// uploading load file
	uploadOutput, err := uploadLoadFile(tempPath, destination)
	if err != nil {
		return
	}

	// loading table
	err = loadTable(uploadOutput.Location, destination)
	return
}

func loadTable(loadFileLocation string, destination *backendconfig.DestinationT) (err error) {
	destinationType := destination.DestinationDefinition.Name

	// no need to do for time window destinations
	if misc.ContainsString(timeWindowDestinations, destinationType) {
		return
	}

	// Getting warehouse manager
	whManager, err := manager.New(destinationType)
	if err != nil {
		return
	}

	// preparing staging table name
	stagingTableName := fmt.Sprintf(`%s%s`,
		"setup_test_staging_",
		strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""),
	)

	// getting load file format
	loadFileFormat := getLoadFileFormat(destination.DestinationDefinition.Name)

	// loading test table from staging file
	err = whManager.LoadTestTable(loadFileLocation, warehouseAdapter(destination), stagingTableName, TestTableSchemaMap, TestPayloadMap, loadFileFormat)
	return
}

func warehouseAdapter(destination *backendconfig.DestinationT) warehouseutils.WarehouseT {
	randomSourceId := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	randomSourceName := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	return warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   randomSourceId,
			Name: randomSourceName,
		},
		Destination: *destination,
		Namespace:   TestNamespace,
		Type:        destination.DestinationDefinition.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, randomSourceId, destination.ID),
	}
}

func fileManagerAdapter(destination *backendconfig.DestinationT) (fileManager filemanager.FileManager, err error) {
	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	fileManager, err = filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config)}),
	})
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}
	return
}

func parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}
