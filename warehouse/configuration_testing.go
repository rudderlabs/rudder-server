package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/config/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	connectionTestingFolder string
	//pkgLogger               logger.LoggerI
)

const (
	InvalidStep        = "Invalid step"
	StagingTablePrefix = "setup_test_staging_"
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

type CTHandleT struct {
	infoRequest      *infoRequest
	client           client.Client
	warehouse        warehouseutils.WarehouseT
	stagingTableName string
}

func Init7() {
	connectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	//pkgLogger = logger.NewLogger().Child("warehouse").Child("connection_testing")
}

func (w *warehousegrpc) Validate(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	handleT := CTHandleT{}
	return handleT.Validating(ctx, req)
}

/*
	Helper Queries
*/
func (ct *CTHandleT) CreateSchemaQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT]  Create schema query with sqlStatement: %s", sqlStatement)
	}()
	switch ct.warehouse.Type {
	case "POSTGRES", "SNOWFLAKE", "RS":
		sqlStatement = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, ct.warehouse.Namespace)
	case "DELTALAKE":
		sqlStatement = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, ct.warehouse.Namespace)
	case "AZURE_SYNAPSE", "MSSQL":
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N'%s' ) EXEC('CREATE SCHEMA [%s]');`,
			ct.warehouse.Namespace, ct.warehouse.Namespace)
	case "CLICKHOUSE":
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		clusterClause := ""
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		}
		sqlStatement = fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s" %s`, ct.warehouse.Namespace, clusterClause)
	}
	return
}

func (ct *CTHandleT) CreateTableQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT]  Create table query with sqlStatement: %s", sqlStatement)
	}()
	// preparing staging table name
	ct.stagingTableName = fmt.Sprintf(`%s%s`,
		StagingTablePrefix,
		strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""),
	)

	switch ct.warehouse.Type {
	case "AZURE_SYNAPSE", "MSSQL":
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
			CREATE TABLE %[1]s ( %v )`,
			ct.stagingTableName,
			mssql.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case "POSTGRES":
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			postgres.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case "RS":
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			redshift.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case "SNOWFLAKE":
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			snowflake.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case "DELTALAKE":
		sqlStatement = fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s ( %v ) USING DELTA;`,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			deltalake.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case "CLICKHOUSE":
		clusterClause := ""
		engine := "ReplacingMergeTree"
		engineOptions := ""
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
			engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
			engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
		}
		sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" %s ( %v ) ENGINE = %s(%s) ORDER BY id PARTITION BY id`,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			clusterClause,
			clickhouse.ColumnsWithDataTypes(ct.stagingTableName, TestTableSchemaMap, []string{"id"}),
			engine,
			engineOptions,
		)
	}
	return
}

func (ct *CTHandleT) DropTableQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT] drop table query with sqlStatement: %s", sqlStatement)
	}()
	switch ct.warehouse.Type {
	case "POSTGRES", "SNOWFLAKE", "RS", "AZURE_SYNAPSE", "MSSQL":
		sqlStatement = fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, ct.warehouse.Namespace, ct.stagingTableName)
	case "DELTALAKE":
		sqlStatement = fmt.Sprintf(`DROP TABLE %[1]s.%[2]s`, ct.warehouse.Namespace, ct.stagingTableName)
	case "CLICKHOUSE":
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		clusterClause := ""
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		}
		sqlStatement = fmt.Sprintf(`DROP TABLE "%s"."%s" %s `, ct.warehouse.Namespace, ct.stagingTableName, clusterClause)
	}
	return
}

/*
	Validation Facade
*/
func (ct *CTHandleT) Validating(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	validationFunctions := ct.getValidationFunctions()
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

func (ct *CTHandleT) getValidationFunctions() map[string]*validationFunc {
	return map[string]*validationFunc{
		"validate": {
			Path: "/validate",
			Func: ct.validateDestinationFunc,
		},
		"steps": {
			Path: "/steps",
			Func: ct.validationStepsFunc,
		},
	}
}

func (ct *CTHandleT) validationStepsFunc(_ context.Context, req json.RawMessage, _ string) (json.RawMessage, error) {
	ct.infoRequest = &infoRequest{}
	if err := ct.parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}

	return json.Marshal(validationStepsResponse{
		Steps: ct.getValidationSteps(),
	})
}

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
		for _, s := range ct.getValidationSteps() {
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
		resp.Steps = ct.getValidationSteps()
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

func (ct *CTHandleT) getValidationSteps() (steps []*validationStep) {
	steps = append(steps, &validationStep{
		ID:        1,
		Name:      "Verifying Object Storage",
		Validator: ct.verifyingObjectStorage,
	})

	// Time window destination contains only object storage verification
	if misc.ContainsString(timeWindowDestinations, ct.warehouse.Destination.Name) {
		return
	}

	steps = append(steps, &validationStep{
		ID:        2,
		Name:      "Verifying Connections",
		Validator: ct.verifyingConnections,
	}, &validationStep{
		ID:        3,
		Name:      "Verifying Create Schema",
		Validator: ct.verifyingCreateSchema,
	}, &validationStep{
		ID:        4,
		Name:      "Verifying Create Table",
		Validator: ct.verifyingCreateTable,
	}, &validationStep{
		ID:        5,
		Name:      "Verifying Load Table",
		Validator: ct.verifyingLoadTable,
	})
	return
}

/*
	Step functions
*/
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

/*
	Helper functions
*/
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
		getLoadFileFormat(destination.Name),
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
	loadFileFormat := getLoadFileFormat(destination.DestinationDefinition.Name)

	// loading test table from staging file
	err = whManager.LoadTestTable(&ct.client, loadFileLocation, ct.warehouseAdapter(), ct.stagingTableName, TestTableSchemaMap, TestPayloadMap, loadFileFormat)

	// Creating drop table query and running over the warehouse
	_, err = ct.client.QueryWrite(dropTableQuery)
	return
}

func (ct *CTHandleT) warehouseAdapter() warehouseutils.WarehouseT {
	destination := ct.infoRequest.Destination

	randomSourceId := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	randomSourceName := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	return warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   randomSourceId,
			Name: randomSourceName,
		},
		Destination: destination,
		Namespace:   TestNamespace,
		Type:        destination.DestinationDefinition.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, randomSourceId, destination.ID),
	}
}

func (ct *CTHandleT) fileManagerAdapter() (fileManager filemanager.FileManager, err error) {
	destination := ct.infoRequest.Destination

	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	fileManager, err = filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config)}),
	})
	fileManagerTimeout := time.Duration(15 * time.Second)
	fileManager.SetTimeout(&fileManagerTimeout)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}
	return
}

func (ct *CTHandleT) parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}
