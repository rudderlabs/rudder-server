package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Manager interface {
	Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error
	FetchSchema(ctx context.Context) (model.Schema, error)
	CreateSchema(ctx context.Context) (err error)
	CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error)
	AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error)
	LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error)
	LoadUserTables(ctx context.Context) map[string]error
	LoadIdentityMergeRulesTable(ctx context.Context) error
	LoadIdentityMappingsTable(ctx context.Context) error
	Cleanup(ctx context.Context)
	IsEmpty(ctx context.Context, warehouse model.Warehouse) (bool, error)
	DownloadIdentityRules(ctx context.Context, gzWriter *misc.GZipWriter) error
	Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error)
	SetConnectionTimeout(timeout time.Duration)
	ErrorMappings() []model.JobError

	TestConnection(ctx context.Context, warehouse model.Warehouse) error
	TestFetchSchema(ctx context.Context) error
	TestLoadTable(ctx context.Context, location, stagingTableName string, payloadMap map[string]any, loadFileFormat string) error
}

type WarehouseDelete interface {
	DropTable(ctx context.Context, tableName string) (err error)
	DeleteBy(ctx context.Context, tableName []string, params warehouseutils.DeleteByParams) error
}

type WarehouseOperations interface {
	Manager
	WarehouseDelete
}

// New is a Factory function that returns a Manager of a given destination-type
func New(destType string, conf *config.Config, logger logger.Logger, stats stats.Stats) (Manager, error) {
	m, err := newManager(destType, conf, logger, stats)
	if err != nil {
		return nil, fmt.Errorf("creating warehouse manager: %w", err)
	}
	return newStatsManager(m, stats), nil
}

// clickhouseSelector holds both ClickHouse implementations and picks between
// them per destination, rather than once per process.
//
// The factories only receive a destination type, so the choice cannot be made
// at construction. Every method that carries a model.Warehouse selects from it
// before delegating; the rest run after one of those has already chosen, and
// reach the chosen implementation through the embedded interface.
//
// That embedded field starts as v1 so a call arriving before any selection
// still works. INVARIANT: every interface method taking a model.Warehouse must
// be overridden here. Miss one and it would silently run v1 against a
// destination configured for v2.
type clickhouseSelector struct {
	WarehouseOperations

	v1, v2 WarehouseOperations
	conf   *config.Config
}

func newClickhouse(conf *config.Config, log logger.Logger, stat stats.Stats) WarehouseOperations {
	v1 := clickhouse.New(conf, log, stat)
	return &clickhouseSelector{
		WarehouseOperations: v1,
		v1:                  v1,
		v2:                  clickhouse.NewV2(conf, log, stat),
		conf:                conf,
	}
}

// selectFor resolves the implementation for warehouse and keeps it for the
// calls that follow, which do not carry a warehouse of their own.
func (s *clickhouseSelector) selectFor(warehouse model.Warehouse) WarehouseOperations {
	if useV2Driver(s.conf, warehouse) {
		s.WarehouseOperations = s.v2
	} else {
		s.WarehouseOperations = s.v1
	}
	return s.WarehouseOperations
}

func (s *clickhouseSelector) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	return s.selectFor(warehouse).Setup(ctx, warehouse, uploader)
}

// Connect is reached without Setup from the warehouse admin query path, which
// is why selecting only in Setup would leave that path on v1.
func (s *clickhouseSelector) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	return s.selectFor(warehouse).Connect(ctx, warehouse)
}

func (s *clickhouseSelector) IsEmpty(ctx context.Context, warehouse model.Warehouse) (bool, error) {
	return s.selectFor(warehouse).IsEmpty(ctx, warehouse)
}

func (s *clickhouseSelector) TestConnection(ctx context.Context, warehouse model.Warehouse) error {
	return s.selectFor(warehouse).TestConnection(ctx, warehouse)
}

// SetConnectionTimeout carries no warehouse and is called before Setup, so it
// has to reach whichever implementation is chosen afterwards.
func (s *clickhouseSelector) SetConnectionTimeout(timeout time.Duration) {
	s.v1.SetConnectionTimeout(timeout)
	s.v2.SetConnectionTimeout(timeout)
}

// useV2Driver reports whether this destination should load through the v2
// ClickHouse driver.
//
// The keys are ordered most specific first and GetBoolVar takes the first one
// that is set, so a single destination can be switched on ahead of its
// workspace, and — the case a rollout actually depends on — switched back off
// after the global flag has been turned on. An allowlist could only ever add.
//
// A manager is built per upload job, and GetBoolVar registers a hot-reloadable
// var, so a change takes effect on the next job without a restart.
func useV2Driver(conf *config.Config, warehouse model.Warehouse) bool {
	keys := make([]string, 0, 3)
	if warehouse.Destination.ID != "" {
		keys = append(keys, "Warehouse.clickhouse."+warehouse.Destination.ID+".useV2Driver")
	}
	if warehouse.WorkspaceID != "" {
		keys = append(keys, "Warehouse.clickhouse."+warehouse.WorkspaceID+".useV2Driver")
	}
	keys = append(keys, "Warehouse.clickhouse.useV2Driver")
	return conf.GetBoolVar(false, keys...)
}

func newManager(destType string, conf *config.Config, logger logger.Logger, stats stats.Stats) (Manager, error) {
	switch destType {
	case warehouseutils.RS:
		return redshift.New(conf, logger, stats), nil
	case warehouseutils.BQ, warehouseutils.BQStreamAllEvents:
		return bigquery.New(conf, logger), nil
	case warehouseutils.SNOWFLAKE, warehouseutils.SnowpipeStreaming:
		return snowflake.New(conf, logger, stats), nil
	case warehouseutils.POSTGRES:
		return postgres.New(conf, logger, stats), nil
	case warehouseutils.CLICKHOUSE:
		return newClickhouse(conf, logger, stats), nil
	case warehouseutils.MSSQL:
		return mssql.New(conf, logger, stats), nil
	case warehouseutils.AzureSynapse:
		return azuresynapse.New(conf, logger, stats), nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		return datalake.New(conf, logger), nil
	case warehouseutils.DELTALAKE:
		return deltalake.New(conf, logger, stats), nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
func NewWarehouseOperations(destType string, conf *config.Config, logger logger.Logger, stats stats.Stats) (WarehouseOperations, error) {
	switch destType {
	case warehouseutils.RS:
		return redshift.New(conf, logger, stats), nil
	case warehouseutils.BQ, warehouseutils.BQStreamAllEvents:
		return bigquery.New(conf, logger), nil
	case warehouseutils.SNOWFLAKE, warehouseutils.SnowpipeStreaming:
		return snowflake.New(conf, logger, stats), nil
	case warehouseutils.POSTGRES:
		return postgres.New(conf, logger, stats), nil
	case warehouseutils.CLICKHOUSE:
		return newClickhouse(conf, logger, stats), nil
	case warehouseutils.MSSQL:
		return mssql.New(conf, logger, stats), nil
	case warehouseutils.AzureSynapse:
		return azuresynapse.New(conf, logger, stats), nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		return datalake.New(conf, logger), nil
	case warehouseutils.DELTALAKE:
		return deltalake.New(conf, logger, stats), nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
