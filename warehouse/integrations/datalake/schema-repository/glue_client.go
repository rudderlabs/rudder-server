package schemarepository

import (
	"context"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type GlueClient interface {
	GetTables(ctx context.Context, databaseName string) ([]Table, error)
	CreateDatabase(ctx context.Context, databaseName string) error
	CreateTable(ctx context.Context, databaseName string, table Table) error
	UpdateTable(ctx context.Context, databaseName string, table Table) error
	DeleteDatabase(ctx context.Context, databaseName string) error
	RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error
	// Add other methods as needed (e.g., partition management)
}
