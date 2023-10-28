package validations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	defaultNamespace = "rudderstack_setup_test"
	defautTableName  = "setup_test_staging"
)

var (
	tableSchemaMap = model.TableSchema{
		"id":  "int",
		"val": "string",
	}
	payloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	alterColumnMap = model.TableSchema{
		"val_alter": "string",
	}
)

//go:generate mockgen -destination=../internal/mocks/validations/mock_validator.go -package mock_validator github.com/rudderlabs/rudder-server/warehouse/validations Validator
type Validator interface {
	Validate(ctx context.Context, dest *backendconfig.DestinationT, stepToValidate string) *model.DestinationValidationResponse
	Steps(dest *backendconfig.DestinationT) *model.StepsResponse
	CreateTempLoadFile(dest *backendconfig.DestinationT) (string, error)
}

type handler struct {
	conf               *config.Config
	logger             logger.Logger
	statsFactory       stats.Stats
	fileManagerFactory filemanager.Factory
	encodingFactory    *encoding.Factory

	config struct {
		connectionTestingFolder string
		objectStorageTimeout    time.Duration
		queryTimeout            time.Duration
	}
}

func NewValidator(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	fileManagerFactory filemanager.Factory,
	encodingFactory *encoding.Factory,
) Validator {
	h := &handler{
		conf:               conf,
		logger:             logger,
		statsFactory:       statsFactory,
		fileManagerFactory: fileManagerFactory,
		encodingFactory:    encodingFactory,
	}
	h.config.objectStorageTimeout = h.conf.GetDuration("Warehouse.Validations.ObjectStorageTimeout", 15, time.Second)
	h.config.queryTimeout = h.conf.GetDuration("Warehouse.Validations.QueryTimeout", 25, time.Second) // Since we have a cp-router default timeout of 30 seconds, keeping the query timeout to 25 seconds
	h.config.connectionTestingFolder = h.conf.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	return h
}

func (h *handler) Validate(ctx context.Context, dest *backendconfig.DestinationT, stepToValidate string) *model.DestinationValidationResponse {
	var (
		destID          = dest.ID
		destType        = dest.DestinationDefinition.Name
		stepsToValidate []*model.Step
		err             error
	)

	log := h.logger.With(
		logfield.DestinationID, destID,
		logfield.DestinationType, destType,
		logfield.DestinationRevisionID, dest.RevisionID,
		logfield.WorkspaceID, dest.WorkspaceID,
		logfield.DestinationValidationsStep, stepToValidate,
	)
	log.Infow("validate destination configuration")

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
		for _, s := range h.Steps(dest).Steps {
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
		stepsToValidate = append(stepsToValidate, h.Steps(dest).Steps...)
	}

	// Iterate over all selected steps and validate
	for _, step := range stepsToValidate {
		if stepError := h.validateStep(ctx, step.Name, dest); stepError != nil {
			err = stepError
			step.Error = stepError.Error()
		} else {
			step.Success = true
		}

		// if any of steps fails, the whole validation fails
		if !step.Success {
			log.Warnw("not able to validate destination configuration",
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

func (h *handler) Steps(dest *backendconfig.DestinationT) *model.StepsResponse {
	var (
		destType = dest.DestinationDefinition.Name
		steps    []*model.Step
	)

	steps = []*model.Step{{
		ID:   len(steps) + 1,
		Name: model.VerifyingObjectStorage,
	}}

	switch destType {
	case warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
	case warehouseutils.S3Datalake:
		wh := h.createDummyWarehouse(dest)
		if canUseGlue := schemarepository.UseGlue(&wh); !canUseGlue {
			break
		}

		steps = append(steps,
			&model.Step{
				ID:   len(steps) + 1,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   len(steps) + 2,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   len(steps) + 3,
				Name: model.VerifyingFetchSchema,
			},
		)
	default:
		steps = append(steps,
			&model.Step{
				ID:   len(steps) + 1,
				Name: model.VerifyingConnections,
			},
			&model.Step{
				ID:   len(steps) + 2,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   len(steps) + 3,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   len(steps) + 4,
				Name: model.VerifyingFetchSchema,
			},
			&model.Step{
				ID:   len(steps) + 5,
				Name: model.VerifyingLoadTable,
			},
		)
	}
	return &model.StepsResponse{
		Steps: steps,
	}
}

func (h *handler) validateStep(ctx context.Context, step string, dest *backendconfig.DestinationT) error {
	var (
		operations manager.WarehouseOperations
		err        error
	)

	switch step {
	case model.VerifyingObjectStorage:
		return h.verifyObjectStorage(ctx, dest)
	case model.VerifyingConnections:
		if operations, err = h.createManager(ctx, dest); err != nil {
			return fmt.Errorf("create manager: %w", err)
		}
		return h.verifyTestConnection(ctx, operations, dest)
	case model.VerifyingCreateSchema:
		if operations, err = h.createManager(ctx, dest); err != nil {
			return fmt.Errorf("create manager: %w", err)
		}
		return h.verifyCreateSchema(ctx, operations)
	case model.VerifyingCreateAndAlterTable:
		if operations, err = h.createManager(ctx, dest); err != nil {
			return fmt.Errorf("create manager: %w", err)
		}
		return h.verifyCreateAlterTable(ctx, operations, h.tableName(dest))
	case model.VerifyingFetchSchema:
		if operations, err = h.createManager(ctx, dest); err != nil {
			return fmt.Errorf("create manager: %w", err)
		}
		return h.verifyFetchSchema(ctx, operations)
	case model.VerifyingLoadTable:
		if operations, err = h.createManager(ctx, dest); err != nil {
			return fmt.Errorf("create manager: %w", err)
		}
		return h.verifyLoadTable(ctx, operations, dest, h.tableName(dest))
	}
	return fmt.Errorf("invalid step: %s", step)
}

func (h *handler) verifyObjectStorage(ctx context.Context, dest *backendconfig.DestinationT) error {
	tempPath, err := h.CreateTempLoadFile(dest)
	if err != nil {
		return fmt.Errorf("creating temp load file: %w", err)
	}

	uploadObject, err := h.uploadFile(ctx, dest, tempPath)
	if err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	err = h.downloadFile(ctx, dest, uploadObject.ObjectName)
	if err != nil {
		return fmt.Errorf("download file: %w", err)
	}
	return nil
}

func (h *handler) verifyTestConnection(ctx context.Context, manager manager.WarehouseOperations, dest *backendconfig.DestinationT) error {
	defer manager.Cleanup(ctx)

	ctx, cancel := context.WithTimeout(ctx, h.config.queryTimeout)
	defer cancel()

	return manager.TestConnection(ctx, h.createDummyWarehouse(dest))
}

func (h *handler) verifyCreateSchema(ctx context.Context, manager manager.WarehouseOperations) error {
	defer manager.Cleanup(ctx)

	return manager.CreateSchema(ctx)
}

func (h *handler) verifyCreateAlterTable(ctx context.Context, manager manager.WarehouseOperations, table string) error {
	defer manager.Cleanup(ctx)

	if err := manager.CreateTable(ctx, table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = manager.DropTable(ctx, table) }()

	for columnName, columnType := range alterColumnMap {
		err := manager.AddColumns(ctx, table, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}})
		if err != nil {
			return fmt.Errorf("alter table add column %s: %w", columnName, err)
		}
	}
	return nil
}

func (h *handler) verifyFetchSchema(ctx context.Context, manager manager.WarehouseOperations) error {
	defer manager.Cleanup(ctx)

	if _, _, err := manager.FetchSchema(ctx); err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}
	return nil
}

func (h *handler) verifyLoadTable(
	ctx context.Context,
	manager manager.WarehouseOperations,
	destination *backendconfig.DestinationT,
	table string,
) error {
	var (
		destinationType = destination.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	defer manager.Cleanup(ctx)

	tempPath, err := h.CreateTempLoadFile(destination)
	if err != nil {
		return fmt.Errorf("create temp load file: %w", err)
	}

	uploadOutput, err := h.uploadFile(ctx, destination, tempPath)
	if err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err = manager.CreateTable(ctx, table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	defer func() {
		_ = manager.DropTable(ctx, table)
	}()

	err = manager.LoadTestTable(ctx, uploadOutput.Location, table, payloadMap, loadFileType)
	if err != nil {
		return fmt.Errorf("load test table: %w", err)
	}
	return nil
}

func (h *handler) CreateTempLoadFile(dest *backendconfig.DestinationT) (string, error) {
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
		h.config.connectionTestingFolder,
		destinationType,
		warehouseutils.RandHex(),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(loadFileType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	writer, err = h.encodingFactory.NewLoadFileWriter(loadFileType, filePath, tableSchemaMap, destinationType)
	if err != nil {
		return "", fmt.Errorf("creating writer for file: %s with error: %w", filePath, err)
	}

	eventLoader := h.encodingFactory.NewEventLoader(writer, loadFileType, destinationType)
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

func (h *handler) uploadFile(ctx context.Context, dest *backendconfig.DestinationT, filePath string) (filemanager.UploadedFile, error) {
	var (
		err        error
		output     filemanager.UploadedFile
		fm         filemanager.FileManager
		uploadFile *os.File

		destinationType = dest.DestinationDefinition.Name
		prefixes        = []string{h.config.connectionTestingFolder, destinationType, warehouseutils.RandHex(), time.Now().Format("01-02-2006")}
	)

	if fm, err = h.createFileManager(dest); err != nil {
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

func (h *handler) downloadFile(ctx context.Context, dest *backendconfig.DestinationT, location string) error {
	var (
		err          error
		fm           filemanager.FileManager
		downloadFile *os.File
		tmpDirPath   string
		filePath     string

		destinationType = dest.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	if fm, err = h.createFileManager(dest); err != nil {
		return err
	}

	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return fmt.Errorf("create tmp dir: %w", err)
	}

	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		h.config.connectionTestingFolder,
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

func (h *handler) createFileManager(dest *backendconfig.DestinationT) (filemanager.FileManager, error) {
	var (
		destType = dest.DestinationDefinition.Name
		conf     = dest.Config
		provider = warehouseutils.ObjectStorageType(destType, conf, misc.IsConfiguredToUseRudderObjectStorage(conf))
	)

	fileManager, err := h.fileManagerFactory(&filemanager.Settings{
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

	fileManager.SetTimeout(h.config.queryTimeout)

	return fileManager, nil
}

func (h *handler) createManager(ctx context.Context, dest *backendconfig.DestinationT) (manager.WarehouseOperations, error) {
	var (
		destType  = dest.DestinationDefinition.Name
		warehouse = h.createDummyWarehouse(dest)
	)

	operations, err := manager.NewWarehouseOperations(destType, h.conf, h.logger, h.statsFactory)
	if err != nil {
		return nil, fmt.Errorf("getting manager: %w", err)
	}

	operations.SetConnectionTimeout(h.config.queryTimeout)

	err = operations.Setup(ctx, warehouse, &uploader{
		dest: dest,
	})
	if err != nil {
		return nil, fmt.Errorf("setting up manager: %w", err)
	}
	return operations, nil
}

func (h *handler) createDummyWarehouse(dest *backendconfig.DestinationT) model.Warehouse {
	var (
		destType  = dest.DestinationDefinition.Name
		namespace = h.configuredNamespaceInDestination(dest)
	)

	return model.Warehouse{
		WorkspaceID: dest.WorkspaceID,
		Destination: *dest,
		Namespace:   namespace,
		Type:        destType,
	}
}

func (h *handler) configuredNamespaceInDestination(dest *backendconfig.DestinationT) string {
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
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, defaultNamespace))
}

func (h *handler) tableName(dest *backendconfig.DestinationT) string {
	destType := dest.DestinationDefinition.Name
	conf := dest.Config

	if destType == warehouseutils.DELTALAKE {
		enableExternalLocation, _ := conf["enableExternalLocation"].(bool)
		externalLocation, _ := conf["externalLocation"].(string)
		if enableExternalLocation && externalLocation != "" {
			return tableWithUUID()
		}
	}
	return defautTableName
}

func tableWithUUID() string {
	return defautTableName + "_" + warehouseutils.RandHex()
}

type uploader struct {
	dest *backendconfig.DestinationT
}

func (*uploader) IsWarehouseSchemaEmpty() bool                                      { return true }
func (*uploader) GetLocalSchema(context.Context) (model.Schema, error)              { return nil, nil }
func (*uploader) UpdateLocalSchema(context.Context, model.Schema) error             { return nil }
func (*uploader) ShouldOnDedupUseNewRecord() bool                                   { return false }
func (*uploader) GetFirstLastEvent() (time.Time, time.Time)                         { return time.Time{}, time.Time{} }
func (*uploader) GetLoadFileGenStartTIme() time.Time                                { return time.Time{} }
func (*uploader) GetTableSchemaInWarehouse(string) model.TableSchema                { return nil }
func (*uploader) GetTableSchemaInUpload(string) model.TableSchema                   { return nil }
func (*uploader) GetSampleLoadFileLocation(context.Context, string) (string, error) { return "", nil }
func (*uploader) CanAppend() bool                                                   { return false }
func (*uploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return nil, nil
}
func (*uploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}
func (m *uploader) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(m.dest.DestinationDefinition.Name)
}
func (m *uploader) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(m.dest.Config)
}
