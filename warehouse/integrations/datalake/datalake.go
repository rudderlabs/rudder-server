package datalake

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`AccessDeniedException: Insufficient Lake Formation permission.*: Required Create Database on Catalog`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`AccessDeniedException: User: .* is not authorized to perform: .* on resource: .*`),
	},
}

type Datalake struct {
	SchemaRepository schemarepository.SchemaRepository
	Warehouse        model.Warehouse
	Uploader         warehouseutils.Uploader
	Logger           logger.Logger
}

func New() *Datalake {
	return &Datalake{
		Logger: logger.NewLogger().Child("warehouse").Child("integrations").Child("datalake"),
	}
}

func (d *Datalake) Setup(warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	d.Warehouse = warehouse
	d.Uploader = uploader

	d.SchemaRepository, err = schemarepository.NewSchemaRepository(d.Warehouse, d.Uploader)

	return err
}

func (*Datalake) CrashRecover() {}

func (d *Datalake) FetchSchema() (model.Schema, model.Schema, error) {
	return d.SchemaRepository.FetchSchema(d.Warehouse)
}

func (d *Datalake) CreateSchema() (err error) {
	return d.SchemaRepository.CreateSchema()
}

func (d *Datalake) CreateTable(tableName string, columnMap model.TableSchema) (err error) {
	return d.SchemaRepository.CreateTable(tableName, columnMap)
}

func (*Datalake) DropTable(_ string) (err error) {
	return fmt.Errorf("datalake err :not implemented")
}

func (d *Datalake) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return d.SchemaRepository.AddColumns(tableName, columnsInfo)
}

func (d *Datalake) AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return d.SchemaRepository.AlterColumn(tableName, columnName, columnType)
}

func (d *Datalake) LoadTable(_ context.Context, tableName string) error {
	d.Logger.Infof("Skipping load for table %s : %s is a datalake destination", tableName, d.Warehouse.Destination.ID)
	return nil
}

func (*Datalake) DeleteBy([]string, warehouseutils.DeleteByParams) (err error) {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func (d *Datalake) LoadUserTables(context.Context) map[string]error {
	d.Logger.Infof("Skipping load for user tables : %s is a datalake destination", d.Warehouse.Destination.ID)
	// return map with nil error entries for identifies and users(if any) tables
	// this is so that they are marked as succeeded
	errorMap := map[string]error{warehouseutils.IdentifiesTable: nil}
	if len(d.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) > 0 {
		errorMap[warehouseutils.UsersTable] = nil
	}
	return errorMap
}

func (d *Datalake) LoadIdentityMergeRulesTable() error {
	d.Logger.Infof("Skipping load for identity merge rules : %s is a datalake destination", d.Warehouse.Destination.ID)
	return nil
}

func (d *Datalake) LoadIdentityMappingsTable() error {
	d.Logger.Infof("Skipping load for identity mappings : %s is a datalake destination", d.Warehouse.Destination.ID)
	return nil
}

func (*Datalake) Cleanup() {
}

func (*Datalake) IsEmpty(_ model.Warehouse) (bool, error) {
	return false, nil
}

func (*Datalake) TestConnection(context.Context, model.Warehouse) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) DownloadIdentityRules(*misc.GZipWriter) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) GetTotalCountInTable(context.Context, string) (int64, error) {
	return 0, nil
}

func (*Datalake) Connect(_ model.Warehouse) (client.Client, error) {
	return client.Client{}, fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) LoadTestTable(_, _ string, _ map[string]interface{}, _ string) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) SetConnectionTimeout(_ time.Duration) {
}

func (*Datalake) ErrorMappings() []model.JobError {
	return errorsMappings
}
