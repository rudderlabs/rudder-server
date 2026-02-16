package datalake

import (
	"context"
	"fmt"
	"regexp"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
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
	d.logger = log.Child("integrations").Child("datalake").Withn(
		obskit.DestinationID(d.Warehouse.Destination.ID),
	)

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
	d.logger.Infon("Skipping load for table", logger.NewStringField("table", tableName))
	return &types.LoadTableStats{}, nil
}

func (*Datalake) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) (err error) {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func (d *Datalake) LoadUserTables(context.Context) map[string]error {
	d.logger.Infon("Skipping load for user tables")
	// return map with nil error entries for identifies and users(if any) tables
	// this is so that they are marked as succeeded
	errorMap := map[string]error{warehouseutils.IdentifiesTable: nil}
	if len(d.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) > 0 {
		errorMap[warehouseutils.UsersTable] = nil
	}
	return errorMap
}

func (d *Datalake) LoadIdentityMergeRulesTable(context.Context) error {
	d.logger.Infon("Skipping load for identity merge rules")
	return nil
}

func (d *Datalake) LoadIdentityMappingsTable(context.Context) error {
	d.logger.Infon("Skipping load for identity mappings")
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

func (*Datalake) TestLoadTable(context.Context, string, string, map[string]any, string) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (d *Datalake) TestFetchSchema(ctx context.Context) error {
	_, err := d.FetchSchema(ctx)
	return err
}

func (*Datalake) SetConnectionTimeout(_ time.Duration) {
}

func (*Datalake) ErrorMappings() []model.JobError {
	return errorsMappings
}
