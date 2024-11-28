package warehouseutils

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

//go:generate mockgen -destination=../internal/mocks/utils/mock_uploader.go -package mock_uploader github.com/rudderlabs/rudder-server/warehouse/utils Uploader
type Uploader interface {
	IsWarehouseSchemaEmpty() bool
	GetLocalSchema(ctx context.Context) (model.Schema, error)
	UpdateLocalSchema(ctx context.Context, schema model.Schema) error
	GetTableSchemaInWarehouse(tableName string) model.TableSchema
	GetTableSchemaInUpload(tableName string) model.TableSchema
	GetLoadFilesMetadata(ctx context.Context, options GetLoadFilesOptions) ([]LoadFile, error)
	GetSampleLoadFileLocation(ctx context.Context, tableName string) (string, error)
	GetSingleLoadFile(ctx context.Context, tableName string) (LoadFile, error)
	ShouldOnDedupUseNewRecord() bool
	UseRudderStorage() bool
	GetLoadFileType() string
	CanAppend() bool
}

type noopUploader struct{}

func NewNoOpUploader() Uploader {
	return &noopUploader{}
}

func (n *noopUploader) IsWarehouseSchemaEmpty() bool {
	return false
}
func (n *noopUploader) GetLocalSchema(ctx context.Context) (model.Schema, error)         { return nil, nil } // nolint:nilnil
func (n *noopUploader) UpdateLocalSchema(ctx context.Context, schema model.Schema) error { return nil }
func (n *noopUploader) GetTableSchemaInWarehouse(tableName string) model.TableSchema     { return nil }
func (n *noopUploader) GetTableSchemaInUpload(tableName string) model.TableSchema        { return nil }
func (n *noopUploader) ShouldOnDedupUseNewRecord() bool                                  { return false }
func (n *noopUploader) UseRudderStorage() bool                                           { return false }
func (n *noopUploader) GetLoadFileType() string                                          { return "" }
func (n *noopUploader) CanAppend() bool                                                  { return false }
func (n *noopUploader) GetLoadFilesMetadata(ctx context.Context, options GetLoadFilesOptions) ([]LoadFile, error) {
	return nil, nil
}

func (n *noopUploader) GetSampleLoadFileLocation(ctx context.Context, tableName string) (string, error) {
	return "", nil
}

func (n *noopUploader) GetSingleLoadFile(ctx context.Context, tableName string) (LoadFile, error) {
	return LoadFile{}, nil
}
