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

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Validator interface {
	Validate() error
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

type dummyUploader struct {
	dest *backendconfig.DestinationT
}

type DestinationValidator interface {
	Validate(dest *backendconfig.DestinationT) *model.DestinationValidationResponse
}

type destinationValidationImpl struct{}

func (*dummyUploader) GetSchemaInWarehouse() model.Schema               { return model.Schema{} }
func (*dummyUploader) GetLocalSchema() (model.Schema, error)            { return model.Schema{}, nil }
func (*dummyUploader) UpdateLocalSchema(_ model.Schema) error           { return nil }
func (*dummyUploader) ShouldOnDedupUseNewRecord() bool                  { return false }
func (*dummyUploader) GetFirstLastEvent() (time.Time, time.Time)        { return time.Time{}, time.Time{} }
func (*dummyUploader) GetLoadFileGenStartTIme() time.Time               { return time.Time{} }
func (*dummyUploader) GetSampleLoadFileLocation(string) (string, error) { return "", nil }
func (*dummyUploader) GetTableSchemaInWarehouse(string) model.TableSchema {
	return model.TableSchema{}
}

func (*dummyUploader) GetTableSchemaInUpload(string) model.TableSchema {
	return model.TableSchema{}
}

func (*dummyUploader) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return []warehouseutils.LoadFile{}
}

func (*dummyUploader) GetSingleLoadFile(string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (m *dummyUploader) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(m.dest.DestinationDefinition.Name)
}

func (m *dummyUploader) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(m.dest.Config)
}

func NewDestinationValidator() DestinationValidator {
	return &destinationValidationImpl{}
}

func (v *destinationValidationImpl) Validate(dest *backendconfig.DestinationT) *model.DestinationValidationResponse {
	return validateDestination(dest, "")
}

func validateDestinationFunc(dest *backendconfig.DestinationT, stepToValidate string) (json.RawMessage, error) {
	return json.Marshal(validateDestination(dest, stepToValidate))
}

func validateDestination(dest *backendconfig.DestinationT, stepToValidate string) *model.DestinationValidationResponse {
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
			return &model.DestinationValidationResponse{
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
			return &model.DestinationValidationResponse{
				Error: fmt.Sprintf("Invalid step: %s", stepToValidate),
			}
		}

		stepsToValidate = append(stepsToValidate, vs)
	} else {
		stepsToValidate = append(stepsToValidate, StepsToValidate(dest).Steps...)
	}

	// Iterate over all selected steps and validate
	for _, step := range stepsToValidate {
		if validator, err = NewValidator(step.Name, dest); err != nil {
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

		if stepError := validator.Validate(); stepError != nil {
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

	res := &model.DestinationValidationResponse{
		Steps:   stepsToValidate,
		Success: err == nil,
	}
	if err != nil {
		res.Error = err.Error()
	}

	return res
}

func NewValidator(step string, dest *backendconfig.DestinationT) (Validator, error) {
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
		if operations, err = createManager(dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &connections{
			destination: dest,
			manager:     operations,
		}, nil
	case model.VerifyingCreateSchema:
		if operations, err = createManager(dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &createSchema{
			manager: operations,
		}, nil
	case model.VerifyingCreateAndAlterTable:
		if operations, err = createManager(dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &createAlterTable{
			table:   Table,
			manager: operations,
		}, nil
	case model.VerifyingFetchSchema:
		if operations, err = createManager(dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &fetchSchema{
			destination: dest,
			manager:     operations,
		}, nil
	case model.VerifyingLoadTable:
		if operations, err = createManager(dest); err != nil {
			return nil, fmt.Errorf("create manager: %w", err)
		}
		return &loadTable{
			destination: dest,
			manager:     operations,
			table:       Table,
		}, nil
	}

	return nil, fmt.Errorf("invalid step: %s", step)
}

func (os *objectStorage) Validate() error {
	var (
		tempPath     string
		err          error
		uploadObject filemanager.UploadOutput
	)

	if tempPath, err = CreateTempLoadFile(os.destination); err != nil {
		return fmt.Errorf("creating temp load file: %w", err)
	}

	if uploadObject, err = uploadFile(os.destination, tempPath); err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err = downloadFile(os.destination, uploadObject.ObjectName); err != nil {
		return fmt.Errorf("download file: %w", err)
	}

	return nil
}

func (c *connections) Validate() error {
	defer c.manager.Cleanup()

	return c.manager.TestConnection(createDummyWarehouse(c.destination))
}

func (cs *createSchema) Validate() error {
	defer cs.manager.Cleanup()

	return cs.manager.CreateSchema()
}

func (cat *createAlterTable) Validate() error {
	defer cat.manager.Cleanup()

	if err := cat.manager.CreateTable(cat.table, TableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = cat.manager.DropTable(cat.table) }()

	for columnName, columnType := range AlterColumnMap {
		if err := cat.manager.AddColumns(
			cat.table,
			[]warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}},
		); err != nil {
			return fmt.Errorf("alter table: %w", err)
		}
	}

	return nil
}

func (fs *fetchSchema) Validate() error {
	defer fs.manager.Cleanup()

	if _, _, err := fs.manager.FetchSchema(createDummyWarehouse(fs.destination)); err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}
	return nil
}

func (lt *loadTable) Validate() error {
	var (
		destinationType = lt.destination.DestinationDefinition.Name

		tempPath     string
		uploadOutput filemanager.UploadOutput
		err          error
	)

	defer lt.manager.Cleanup()

	if tempPath, err = CreateTempLoadFile(lt.destination); err != nil {
		return fmt.Errorf("create temp load file: %w", err)
	}

	if uploadOutput, err = uploadFile(lt.destination, tempPath); err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err = lt.manager.CreateTable(lt.table, TableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = lt.manager.DropTable(lt.table) }()

	if err = lt.manager.LoadTestTable(
		uploadOutput.Location,
		lt.table,
		PayloadMap,
		warehouseutils.GetLoadFileFormat(destinationType),
	); err != nil {
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
		warehouseutils.GetLoadFileFormat(destinationType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	if warehouseutils.GetLoadFileType(destinationType) == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		writer, err = encoding.CreateParquetWriter(TableSchemaMap, filePath, destinationType)
	} else {
		writer, err = misc.CreateGZ(filePath)
	}
	if err != nil {
		return "", fmt.Errorf("creating writer for file: %s with error: %w", filePath, err)
	}

	eventLoader := encoding.GetNewEventLoader(destinationType, warehouseutils.GetLoadFileType(destinationType), writer)
	for _, column := range []string{"id", "val"} {
		eventLoader.AddColumn(column, TableSchemaMap[column], PayloadMap[column])
	}

	if err = eventLoader.Write(); err != nil {
		return "", fmt.Errorf("writing to file: %w", err)
	}

	if err = writer.Close(); err != nil {
		return "", fmt.Errorf("closing writer: %w", err)
	}

	return filePath, nil
}

func uploadFile(dest *backendconfig.DestinationT, filePath string) (filemanager.UploadOutput, error) {
	var (
		err        error
		output     filemanager.UploadOutput
		fm         filemanager.FileManager
		uploadFile *os.File

		destinationType = dest.DestinationDefinition.Name
		prefixes        = []string{connectionTestingFolder, destinationType, warehouseutils.RandHex(), time.Now().Format("01-02-2006")}
	)

	if fm, err = createFileManager(dest); err != nil {
		return filemanager.UploadOutput{}, err
	}

	if uploadFile, err = os.Open(filePath); err != nil {
		return filemanager.UploadOutput{}, fmt.Errorf("opening file: %w", err)
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer func() { _ = uploadFile.Close() }()

	if output, err = fm.Upload(context.TODO(), uploadFile, prefixes...); err != nil {
		return filemanager.UploadOutput{}, fmt.Errorf("uploading file: %w", err)
	}

	return output, nil
}

func downloadFile(dest *backendconfig.DestinationT, location string) error {
	var (
		err          error
		fm           filemanager.FileManager
		downloadFile *os.File
		tmpDirPath   string
		filePath     string

		destinationType = dest.DestinationDefinition.Name
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
		warehouseutils.GetLoadFileFormat(destinationType),
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

	if err = fm.Download(context.TODO(), downloadFile, location); err != nil {
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

	fileManager, err := fileManagerFactory.New(&filemanager.SettingsT{
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

	fileManager.SetTimeout(objectStorageValidationTimeout)

	return fileManager, nil
}

func createManager(dest *backendconfig.DestinationT) (manager.WarehouseOperations, error) {
	var (
		destType  = dest.DestinationDefinition.Name
		warehouse = createDummyWarehouse(dest)

		operations manager.WarehouseOperations
		err        error
	)

	if operations, err = manager.NewWarehouseOperations(destType); err != nil {
		return nil, fmt.Errorf("getting manager: %w", err)
	}

	operations.SetConnectionTimeout(warehouseutils.TestConnectionTimeout)

	if err = operations.Setup(warehouse, &dummyUploader{
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
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, Namespace))
}
