package datalake

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"

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
	conf             *config.Config
	logger           logger.Logger
}

func New(conf *config.Config, log logger.Logger) *Datalake {
	d := &Datalake{}

	d.conf = conf
	d.logger = log.Child("integrations").Child("datalake")

	return d
}

func (d *Datalake) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	d.Warehouse = warehouse
	d.Uploader = uploader

	d.SchemaRepository, err = schemarepository.NewSchemaRepository(d.conf, d.logger, d.Warehouse, d.Uploader)

	return err
}

func (d *Datalake) FetchSchema(ctx context.Context) (model.Schema, error) {
	return d.SchemaRepository.FetchSchema(ctx, d.Warehouse)
}

func (d *Datalake) CreateSchema(ctx context.Context) (err error) {
	return d.SchemaRepository.CreateSchema(ctx)
}

func (d *Datalake) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	return d.SchemaRepository.CreateTable(ctx, tableName, columnMap)
}

func (*Datalake) DropTable(context.Context, string) (err error) {
	return fmt.Errorf("datalake err :not implemented")
}

func (d *Datalake) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return d.SchemaRepository.AddColumns(ctx, tableName, columnsInfo)
}

func (d *Datalake) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return d.SchemaRepository.AlterColumn(ctx, tableName, columnName, columnType)
}

func (d *Datalake) LoadTable(_ context.Context, tableName string) (*types.LoadTableStats, error) {
	d.logger.Infof("Skipping load for table %s : %s is a datalake destination", tableName, d.Warehouse.Destination.ID)
	return &types.LoadTableStats{}, nil
}

func (*Datalake) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) (err error) {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func (d *Datalake) LoadUserTables(context.Context) map[string]error {
	d.logger.Infof("Skipping load for user tables : %s is a datalake destination", d.Warehouse.Destination.ID)
	// return map with nil error entries for identifies and users(if any) tables
	// this is so that they are marked as succeeded
	errorMap := map[string]error{warehouseutils.IdentifiesTable: nil}
	if len(d.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) > 0 {
		errorMap[warehouseutils.UsersTable] = nil
	}
	return errorMap
}

func (d *Datalake) LoadIdentityMergeRulesTable(context.Context) error {
	d.logger.Infof("Skipping load for identity merge rules : %s is a datalake destination", d.Warehouse.Destination.ID)
	return nil
}

func (d *Datalake) LoadIdentityMappingsTable(context.Context) error {
	d.logger.Infof("Skipping load for identity mappings : %s is a datalake destination", d.Warehouse.Destination.ID)
	return nil
}

func (*Datalake) Cleanup(context.Context) {
}

func (*Datalake) IsEmpty(context.Context, model.Warehouse) (bool, error) {
	return false, nil
}

func (*Datalake) TestConnection(context.Context, model.Warehouse) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) Connect(context.Context, model.Warehouse) (client.Client, error) {
	return client.Client{}, fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) LoadTestTable(context.Context, string, string, map[string]interface{}, string) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*Datalake) SetConnectionTimeout(_ time.Duration) {
}

func (*Datalake) ErrorMappings() []model.JobError {
	return errorsMappings
}
