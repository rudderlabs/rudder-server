package validations

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type DestinationValidationResponse struct {
	Success bool          `json:"success"`
	Error   string        `json:"error"`
	Steps   []*model.Step `json:"steps"`
}

type Validator interface {
	Validate(ctx context.Context) error
}

type objectStorage struct {
	destination *backendconfig.DestinationT
}

type connections struct {
	manager     manager.WarehouseOperations
	destination *backendconfig.DestinationT
}

type createSchema struct {
	manager manager.WarehouseOperations
}

type createAlterTable struct {
	manager manager.WarehouseOperations
	table   string
}

type fetchSchema struct {
	manager     manager.WarehouseOperations
	destination *backendconfig.DestinationT
}

type loadTable struct {
	manager     manager.WarehouseOperations
	destination *backendconfig.DestinationT
	table       string
}

type DestinationValidator interface {
	Validate(ctx context.Context, dest *backendconfig.DestinationT) *DestinationValidationResponse
}

type destinationValidationImpl struct{}

func NewDestinationValidator() DestinationValidator {
	return &destinationValidationImpl{}
}

func (*destinationValidationImpl) Validate(ctx context.Context, dest *backendconfig.DestinationT) *DestinationValidationResponse {
	return validateDestination(ctx, dest, "")
}

func validateDestinationFunc(ctx context.Context, dest *backendconfig.DestinationT, stepToValidate string) (json.RawMessage, error) {
	return json.Marshal(validateDestination(ctx, dest, stepToValidate))
}

func validateDestination(ctx context.Context, dest *backendconfig.DestinationT, stepToValidate string) *DestinationValidationResponse {
	var (
		destID          = dest.ID
		destType        = dest.DestinationDefinition.Name
		stepsToValidate []*model.Step
		validator       Validator
		err             error
	)

	pkgLogger.Infow("validate destination configuration",
		logfield.DestinationID, destID,
		logfield.DestinationType, destType,
		logfield.DestinationRevisionID, dest.RevisionID,
		logfield.WorkspaceID, dest.WorkspaceID,
		logfield.DestinationValidationsStep, stepToValidate,
	)

	// check if req has specified a step in query params
	if stepToValidate != "" {
		stepI, err := strconv.Atoi(stepToValidate)
		if err != nil {
			return &DestinationValidationResponse{
				Error: fmt.Sprintf("Invalid step: %s", stepToValidate),
			}
		}

		// get validation step
		var vs *model.Step
		for _, s := range StepsToValidate(dest).Steps {
			if s.ID == stepI {
				vs = s
				break
			}
		}

		if vs == nil {
			return &DestinationValidationResponse{
				Error: fmt.Sprintf("Invalid step: %s", stepToValidate),
			}
		}

		stepsToValidate = append(stepsToValidate, vs)
	} else {
		stepsToValidate = append(stepsToValidate, StepsToValidate(dest).Steps...)
	}

	// Iterate over all selected steps and validate
	for _, step := range stepsToValidate {
		if validator, err = NewValidator(ctx, step.Name, dest); err != nil {
			err = fmt.Errorf("creating validator: %v", err)
			step.Error = err.Error()

			pkgLogger.Warnw("creating validator",
				logfield.DestinationID, destID,
				logfield.DestinationType, destType,
				logfield.DestinationRevisionID, dest.RevisionID,
				logfield.WorkspaceID, dest.WorkspaceID,
				logfield.DestinationValidationsStep, step.Name,
				logfield.Error, step.Error,
			)
			break
		}

		if stepError := validator.Validate(ctx); stepError != nil {
			err = stepError
			step.Error = stepError.Error()
		} else {
			step.Success = true
		}

		// if any of steps fails, the whole validation fails
		if !step.Success {
			pkgLogger.Warnw("not able to validate destination configuration",
				logfield.DestinationID, destID,
				logfield.DestinationType, destType,
				logfield.DestinationRevisionID, dest.RevisionID,
				logfield.WorkspaceID, dest.WorkspaceID,
				logfield.DestinationValidationsStep, step.Name,
				logfield.Error, step.Error,
			)
			break
		}
	}

	res := &DestinationValidationResponse{
		Steps:   stepsToValidate,
		Success: err == nil,
	}
	if err != nil {
		res.Error = err.Error()
	}

	return res
}

func NewValidator(ctx context.Context, step string, dest *backendconfig.DestinationT) (Validator, error) {
	var (
		operations manager.WarehouseOperations
		err        error
	)

	switch step {
	case model.VerifyingObjectStorage:
		return &objectStorage{
			destination: dest,
		}, nil
	case model.VerifyingConnections:
		if operations, err = createManager(ctx, dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &connections{
			destination: dest,
			manager:     operations,
		}, nil
	case model.VerifyingCreateSchema:
		if operations, err = createManager(ctx, dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &createSchema{
			manager: operations,
		}, nil
	case model.VerifyingCreateAndAlterTable:
		if operations, err = createManager(ctx, dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &createAlterTable{
			table:   getTable(dest),
			manager: operations,
		}, nil
	case model.VerifyingFetchSchema:
		if operations, err = createManager(ctx, dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &fetchSchema{
			destination: dest,
			manager:     operations,
		}, nil
	case model.VerifyingLoadTable:
		if operations, err = createManager(ctx, dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &loadTable{
			destination: dest,
			manager:     operations,
			table:       getTable(dest),
		}, nil
	}

	return nil, fmt.Errorf("invalid step: %s", step)
}

func (os *objectStorage) Validate(ctx context.Context) error {
	var (
		tempPath     string
		err          error
		uploadObject filemanager.UploadedFile
	)

	if tempPath, err = CreateTempLoadFile(os.destination); err != nil {
		return fmt.Errorf("creating temp load file: %w", err)
	}

	if uploadObject, err = uploadFile(ctx, os.destination, tempPath); err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err = downloadFile(ctx, os.destination, uploadObject.ObjectName); err != nil {
		return fmt.Errorf("download file: %w", err)
	}

	cleanupObjectStorageFiles, _ := os.destination.Config[model.CleanupObjectStorageFilesSetting.String()].(bool)
	if cleanupObjectStorageFiles {
		if err = deleteFile(ctx, os.destination, uploadObject.ObjectName); err != nil {
			return fmt.Errorf("delete file: %w. Ensure that delete permissions are granted because the option to delete files after a successful sync is enabled", err)
		}
	}

	return nil
}

func (c *connections) Validate(ctx context.Context) error {
	defer c.manager.Cleanup(ctx)

	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	return c.manager.TestConnection(ctx, createDummyWarehouse(c.destination))
}

func (cs *createSchema) Validate(ctx context.Context) error {
	defer cs.manager.Cleanup(ctx)

	return cs.manager.CreateSchema(ctx)
}

func (cat *createAlterTable) Validate(ctx context.Context) error {
	defer cat.manager.Cleanup(ctx)

	if err := cat.manager.CreateTable(ctx, cat.table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = cat.manager.DropTable(ctx, cat.table) }()

	for columnName, columnType := range alterColumnMap {
		if err := cat.manager.AddColumns(ctx, cat.table, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}}); err != nil {
			return fmt.Errorf("alter table: %w", err)
		}
	}

	return nil
}

func (fs *fetchSchema) Validate(ctx context.Context) error {
	defer fs.manager.Cleanup(ctx)

	if _, err := fs.manager.FetchSchema(ctx); err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}
	return nil
}

func (lt *loadTable) Validate(ctx context.Context) error {
	var (
		destinationType = lt.destination.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)

		tempPath     string
		uploadOutput filemanager.UploadedFile
		err          error
	)

	defer lt.manager.Cleanup(ctx)

	if tempPath, err = CreateTempLoadFile(lt.destination); err != nil {
		return fmt.Errorf("create temp load file: %w", err)
	}

	if uploadOutput, err = uploadFile(ctx, lt.destination, tempPath); err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err = lt.manager.CreateTable(ctx, lt.table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = lt.manager.DropTable(ctx, lt.table) }()

	if err = lt.manager.LoadTestTable(ctx, uploadOutput.Location, lt.table, payloadMap, loadFileType); err != nil {
		return fmt.Errorf("load test table: %w", err)
	}

	return nil
}

// CreateTempLoadFile creates a temporary load file
func CreateTempLoadFile(dest *backendconfig.DestinationT) (string, error) {
	var (
		tmpDirPath string
		filePath   string
		err        error
		writer     encoding.LoadFileWriter

		destinationType = dest.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return "", fmt.Errorf("create tmp dir: %w", err)
	}

	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		connectionTestingFolder,
		destinationType,
		warehouseutils.RandHex(),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(loadFileType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	ef := encoding.NewFactory(config.Default)
	writer, err = ef.NewLoadFileWriter(loadFileType, filePath, tableSchemaMap, destinationType)
	if err != nil {
		return "", fmt.Errorf("creating writer for file: %s with error: %w", filePath, err)
	}

	eventLoader := ef.NewEventLoader(writer, loadFileType, destinationType)
	for _, column := range []string{"id", "val"} {
		eventLoader.AddColumn(column, tableSchemaMap[column], payloadMap[column])
	}

	if err = eventLoader.Write(); err != nil {
		return "", fmt.Errorf("writing to file: %w", err)
	}

	if err = writer.Close(); err != nil {
		return "", fmt.Errorf("closing writer: %w", err)
	}

	return filePath, nil
}

func uploadFile(ctx context.Context, dest *backendconfig.DestinationT, filePath string) (filemanager.UploadedFile, error) {
	var (
		err        error
		output     filemanager.UploadedFile
		fm         filemanager.FileManager
		uploadFile *os.File

		destinationType = dest.DestinationDefinition.Name
		prefixes        = []string{connectionTestingFolder, destinationType, warehouseutils.RandHex(), time.Now().Format("01-02-2006")}
	)

	if fm, err = createFileManager(dest); err != nil {
		return filemanager.UploadedFile{}, err
	}

	if uploadFile, err = os.Open(filePath); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("opening file: %w", err)
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer func() { _ = uploadFile.Close() }()

	if output, err = fm.Upload(ctx, uploadFile, prefixes...); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("uploading file: %w", err)
	}

	return output, nil
}

func deleteFile(ctx context.Context, dest *backendconfig.DestinationT, location string) error {
	var (
		err error
		fm  filemanager.FileManager
	)
	if fm, err = createFileManager(dest); err != nil {
		return fmt.Errorf("create file manager: %w", err)
	}
	if err = fm.Delete(ctx, []string{location}); err != nil {
		return fmt.Errorf("delete file: %w", err)
	}
	return nil
}

func downloadFile(ctx context.Context, dest *backendconfig.DestinationT, location string) error {
	var (
		err          error
		fm           filemanager.FileManager
		downloadFile *os.File
		tmpDirPath   string
		filePath     string

		destinationType = dest.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	if fm, err = createFileManager(dest); err != nil {
		return err
	}

	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return fmt.Errorf("create tmp dir: %w", err)
	}

	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		connectionTestingFolder,
		destinationType,
		warehouseutils.RandHex(),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(loadFileType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	if downloadFile, err = os.Create(filePath); err != nil {
		return fmt.Errorf("creating file: %w", err)
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer func() { _ = downloadFile.Close() }()

	if err = fm.Download(ctx, downloadFile, location); err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}
	return nil
}

func createFileManager(dest *backendconfig.DestinationT) (filemanager.FileManager, error) {
	var (
		destType = dest.DestinationDefinition.Name
		conf     = dest.Config
		provider = warehouseutils.ObjectStorageType(destType, conf, misc.IsConfiguredToUseRudderObjectStorage(conf))
	)

	fileManager, err := fileManagerFactory(&filemanager.Settings{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           conf,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(conf),
			WorkspaceID:      dest.WorkspaceID,
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("creating file manager: %w", err)
	}

	fileManager.SetTimeout(objectStorageTimeout)

	return fileManager, nil
}

func createManager(ctx context.Context, dest *backendconfig.DestinationT) (manager.WarehouseOperations, error) {
	var (
		destType  = dest.DestinationDefinition.Name
		warehouse = createDummyWarehouse(dest)

		operations manager.WarehouseOperations
		err        error
	)

	if operations, err = manager.NewWarehouseOperations(destType, config.Default, pkgLogger, stats.Default); err != nil {
		return nil, fmt.Errorf("getting manager: %w", err)
	}

	operations.SetConnectionTimeout(queryTimeout)

	if err = operations.Setup(ctx, warehouse, &dummyUploader{
		dest: dest,
	}); err != nil {
		return nil, fmt.Errorf("setting up manager: %w", err)
	}

	return operations, nil
}

func createDummyWarehouse(dest *backendconfig.DestinationT) model.Warehouse {
	var (
		destType  = dest.DestinationDefinition.Name
		namespace = configuredNamespaceInDestination(dest)
	)

	return model.Warehouse{
		WorkspaceID: dest.WorkspaceID,
		Destination: *dest,
		Namespace:   namespace,
		Type:        destType,
	}
}

func configuredNamespaceInDestination(dest *backendconfig.DestinationT) string {
	var (
		destType = dest.DestinationDefinition.Name
		conf     = dest.Config
	)

	if destType == warehouseutils.CLICKHOUSE {
		return conf["database"].(string)
	}

	if conf["namespace"] != nil {
		namespace := conf["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
}

func getTable(dest *backendconfig.DestinationT) string {
	destType := dest.DestinationDefinition.Name
	conf := dest.Config

	if destType == warehouseutils.DELTALAKE {
		enableExternalLocation, _ := conf["enableExternalLocation"].(bool)
		externalLocation, _ := conf["externalLocation"].(string)
		if enableExternalLocation && externalLocation != "" {
			return tableWithUUID()
		}
	}

	return table
}

func tableWithUUID() string {
	return table + "_" + warehouseutils.RandHex()
}

type dummyUploader struct {
	dest *backendconfig.DestinationT
}

func (*dummyUploader) IsWarehouseSchemaEmpty() bool { return true }
func (*dummyUploader) GetLocalSchema(context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}
func (*dummyUploader) UpdateLocalSchema(context.Context, model.Schema) error { return nil }
func (*dummyUploader) ShouldOnDedupUseNewRecord() bool                       { return false }
func (*dummyUploader) GetTableSchemaInWarehouse(string) model.TableSchema    { return nil }
func (*dummyUploader) GetTableSchemaInUpload(string) model.TableSchema       { return nil }
func (*dummyUploader) CanAppend() bool                                       { return false }
func (*dummyUploader) GetSampleLoadFileLocation(context.Context, string) (string, error) {
	return "", nil
}

func (*dummyUploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return nil, nil
}

func (*dummyUploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (m *dummyUploader) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(m.dest.DestinationDefinition.Name)
}

func (m *dummyUploader) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(m.dest.Config)
}
