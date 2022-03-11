package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	rudderConnectionTestingFolder string
)

const (
	testPayload   = "1,RudderStack"
	InvalidStep   = "Invalid step"
	TestNamespace = "_rudderstack_setup_test"
)

type ValidationFunc struct {
	Path string
	Func func(context.Context, json.RawMessage, string) (json.RawMessage, error)
}

type InfoRequest struct {
	Destination *backendconfig.DestinationT `json:"destination"`
	Location    string                      `json:"location"`
}

type validationRequest struct {
	validationStep *validationStep
	infoRequest    *InfoRequest
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
	Data      string    `json:"data"`
	Error     string    `json:"error"`
	Validator validator `json:"-"`
}

type validator func(ctx context.Context, req *validationRequest) *validationStep

type validationStepsResponse struct {
	Steps []*validationStep `json:"steps"`
}

//func Init() {
//	loadConfig()
//	pkgLogger = logger.NewLogger().Child("destination-connection-tester")
//}
//
//func loadConfig() {
//	rudderConnectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
//}

func (w *warehousegrpc) Validating(ctx context.Context, req *proto.ValidationRequest, api UploadAPIT) (*proto.ValidationResponse, error) {
	funcs := w.GetValidationFuncs()
	f, ok := funcs[req.Path]
	if !ok {
		return nil, errors.New("path not found")
	}
	step := req.Step
	result, err := f.Func(ctx, json.RawMessage(req.Body), step)
	errorMessage := ""
	if err != nil {
		errorMessage = err.Error()
	}
	return &proto.ValidationResponse{
		Error: errorMessage,
		Data:  string(result),
	}, nil
}

func (w *warehousegrpc) GetValidationFuncs() map[string]*ValidationFunc {
	return map[string]*ValidationFunc{
		"validate": {
			Path: "/validate",
			Func: w.validateSourceFunc,
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
			Name:      "Verifying Upload to Object Storage",
			Validator: w.VerifyingUploadToObjectStorage,
		},
		{
			ID:        2,
			Name:      "Verifying Download from Object Storage",
			Validator: w.VerifyingDownloadFromObjectStorage,
		},
		{
			ID:        3,
			Name:      "Verifying Connections",
			Validator: w.VerifyingConnections,
		},
		{
			ID:        4,
			Name:      "Verifying Create Schema",
			Validator: w.VerifyingConnections,
		},
		{
			ID:        4,
			Name:      "Verifying Create Schema",
			Validator: w.VerifyingCreateSchema,
		},
		{
			ID:        5,
			Name:      "Verifying Create Table",
			Validator: w.VerifyingCreateTable,
		},
		{
			ID:        6,
			Name:      "Verifying Load Table",
			Validator: w.VerifyingLoadTable,
		},
	}
}

func (w *warehousegrpc) validationStepsFunc(_ context.Context, _ json.RawMessage, _ string) (json.RawMessage, error) {
	return json.Marshal(validationStepsResponse{
		Steps: w.getValidationSteps(),
	})
}

func (w *warehousegrpc) validateSourceFunc(ctx context.Context, req json.RawMessage, step string) (json.RawMessage, error) {
	infoRequest := &InfoRequest{}
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
			}
		}

		if v == nil {
			resp.Error = fmt.Sprintf("%s: %s", InvalidStep, step)
			return json.Marshal(resp)
		}

		resp.Steps = append(resp.Steps, v)
	} else {
		resp.Error = fmt.Sprintf("%s: %s", InvalidStep, step)
		return json.Marshal(resp)
	}

	// Iterate over all selected steps and validate
	for idx, s := range resp.Steps {
		result := s.Validator(ctx, &validationRequest{
			validationStep: s,
			infoRequest:    infoRequest,
		})
		resp.Steps[idx] = result

		// if any one steps fails, the whole validation fails
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

func (w *warehousegrpc) VerifyingUploadToObjectStorage(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	response, err := verifyUploadToObjectStorage(vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Data = response
	step.Success = true
	return
}

func verifyUploadToObjectStorage(dest *backendconfig.DestinationT) (response string, err error) {
	tempPath, err := createLoadFile(dest.ID)
	if err != nil {
		return
	}

	uploadLocation, err := uploadLoadFile(tempPath, dest)
	if err != nil {
		return
	}

	responseJson, err := json.Marshal(fmt.Sprintf("Location: %s", uploadLocation))
	if err != nil {
		return
	}

	response = string(responseJson)
	return
}

func (w *warehousegrpc) VerifyingDownloadFromObjectStorage(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := downloadLoadFile(vr.infoRequest.Location, vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}
	return
}

func (w *warehousegrpc) VerifyingConnections(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyConnections(vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) VerifyingCreateSchema(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyCreateSchema(vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) VerifyingCreateTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyCreateTable(vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func (w *warehousegrpc) VerifyingLoadTable(ctx context.Context, vr *validationRequest) (step *validationStep) {
	step = vr.validationStep

	err := verifyLoadTable(vr.infoRequest.Location, vr.infoRequest.Destination)
	if err != nil {
		step.Error = err.Error()
		return
	}

	step.Success = true
	return
}

func createLoadFile(destinationID string) (gzipFilePath string, err error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create tmp dir for testing this destination id %s: err %v", destinationID, err)
		return
	}

	gzipFilePath = fmt.Sprintf("%v/%v/%v.%v.%v.csv.gz", tmpDirPath, rudderConnectionTestingFolder, destinationID, uuid.Must(uuid.NewV4()), time.Now().Unix())
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to make dir %s for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		return
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create gzip writer for testing this destination id %s: err %v", gzipFilePath, destinationID, err)
		return
	}
	gzWriter.WriteGZ(testPayload)
	gzWriter.CloseGZ()
	return
}

func uploadLoadFile(filePath string, destination *backendconfig.DestinationT) (objectName string, err error) {
	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))
	keyPrefixes := []string{rudderConnectionTestingFolder, destination.ID, time.Now().Format("01-02-2006")}

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config)}),
	})
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing destination id %s: with err %v", destination.ID, err)
		return
	}

	uploadFile, err := os.Open(filePath)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to open file %s for testing this destination id %s: err %v", filePath, destination.ID, err)
		return
	}

	defer misc.RemoveFilePaths(filePath)
	defer uploadFile.Close()

	uploadOutput, err := uploader.Upload(uploadFile, keyPrefixes...)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to upload test file %s for testing this destination id %s: err %v", filePath, destination.ID, err)
		return
	}

	return uploadOutput.ObjectName, err
}

func downloadLoadFile(location string, destination *backendconfig.DestinationT) (err error) {
	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config)}),
	})
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to initiate filemanager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create temp directory for testing this destination id %s: err %v", destination.ID, err)
		return
	}

	testFilePath := fmt.Sprintf("%v/%v/%v.%v.%v.csv.gz", tmpDirPath, rudderConnectionTestingFolder, destination.ID, uuid.Must(uuid.NewV4()), time.Now().Unix())
	err = os.MkdirAll(filepath.Dir(testFilePath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create directory at path %s: err %v", testFilePath, err)
		return
	}

	testFile, err := os.Create(testFilePath)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to create tempFilePath: %s for testing this destination id %s: err %v", testFilePath, destination.ID, err)
		return
	}

	err = downloader.Download(testFile, location)
	if err != nil {
		pkgLogger.Errorf("DCT: Failed to download test file %s for testing this destination id %s: err %v", location, destination.ID, err)
		return
	}

	testFile.Close()
	misc.RemoveFilePaths(testFilePath)
	return
}

func verifyConnections(destination *backendconfig.DestinationT) (err error) {
	provider := destination.DestinationDefinition.Name
	if misc.ContainsString(timeWindowDestinations, destination.DestinationDefinition.Name) {
		return
	}

	whManager, err := manager.New(provider)
	if err != nil {
		return
	}

	err = whManager.TestConnection(warehouseutils.WarehouseT{
		Destination: *destination,
	})
	return
}

func verifyCreateSchema(destination *backendconfig.DestinationT) (err error) {
	provider := destination.DestinationDefinition.Name
	if misc.ContainsString(timeWindowDestinations, destination.DestinationDefinition.Name) {
		return
	}

	whManager, err := manager.New(provider)
	if err != nil {
		return
	}
	warehouseT := warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   "",
			Name: "",
		},
		Destination: *destination,
		Namespace:   TestNamespace,
		Type:        destination.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.Name, "", destination.ID),
	}
	err = whManager.CreateTestSchema(warehouseT)
	return
}

func verifyCreateTable(destination *backendconfig.DestinationT) (err error) {
	provider := destination.DestinationDefinition.Name
	if misc.ContainsString(timeWindowDestinations, destination.DestinationDefinition.Name) {
		return
	}

	whManager, err := manager.New(provider)
	if err != nil {
		return
	}
	warehouseT := warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   "",
			Name: "",
		},
		Destination: *destination,
		Namespace:   TestNamespace,
		Type:        destination.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.Name, "", destination.ID),
	}
	err = whManager.CreateTestTable(warehouseT)
	return
}

func verifyLoadTable(location string, destination *backendconfig.DestinationT) (err error) {
	provider := destination.DestinationDefinition.Name
	if misc.ContainsString(timeWindowDestinations, destination.DestinationDefinition.Name) {
		return
	}

	whManager, err := manager.New(provider)
	if err != nil {
		return
	}
	warehouseT := warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   "",
			Name: "",
		},
		Destination: *destination,
		Namespace:   TestNamespace,
		Type:        destination.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.Name, "", destination.ID),
	}
	err = whManager.LoadTestTable(location, warehouseT)
	return
}

func parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}
