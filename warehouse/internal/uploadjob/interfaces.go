package uploadjob

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

// UploadJobRunner is the interface that both router and slave must implement to run upload jobs
type UploadJobRunner interface {
	// RunUploadJob runs an upload job
	RunUploadJob(ctx context.Context, uploadJob *model.UploadJob) error
}

// UploadJobFactoryInterface is the interface for creating upload jobs
type UploadJobFactoryInterface interface {
	// NewUploadJob creates a new upload job
	NewUploadJob(ctx context.Context, dto *model.UploadJob, whManager manager.Manager) *UploadJob
}

// UploadJobInterface is the interface for upload job operations
type UploadJobInterface interface {
	// Run executes the upload job
	Run() error

	// DTO returns the upload job data transfer object
	DTO() *model.UploadJob

	// GetLoadFilesMetadata returns the load files metadata
	GetLoadFilesMetadata(ctx context.Context, options utils.GetLoadFilesOptions) (loadFiles []utils.LoadFile, err error)

	// IsWarehouseSchemaEmpty returns true if the warehouse schema is empty
	IsWarehouseSchemaEmpty() bool

	// GetTableSchemaInWarehouse returns the table schema in the warehouse
	GetTableSchemaInWarehouse(tableName string) model.TableSchema

	// GetTableSchemaInUpload returns the table schema in the upload
	GetTableSchemaInUpload(tableName string) model.TableSchema

	// GetSingleLoadFile returns a single load file
	GetSingleLoadFile(ctx context.Context, tableName string) (utils.LoadFile, error)

	// ShouldOnDedupUseNewRecord returns true if new record should be used on dedup
	ShouldOnDedupUseNewRecord() bool

	// UseRudderStorage returns true if rudder storage should be used
	UseRudderStorage() bool

	// GetLoadFileType returns the load file type
	GetLoadFileType() string

	// GetLocalSchema returns the local schema
	GetLocalSchema(ctx context.Context) (model.Schema, error)

	// UpdateLocalSchema updates the local schema
	UpdateLocalSchema(ctx context.Context, schema model.Schema) error
}
