package warehouseutils

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type (
	ModelWarehouse   = model.Warehouse
	ModelTableSchema = model.TableSchema
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

type NopUploader struct{}

func (n *NopUploader) IsWarehouseSchemaEmpty() bool {
	return false
}
func (n *NopUploader) GetLocalSchema(ctx context.Context) (model.Schema, error)         { return nil, nil } // nolint:nilnil
func (n *NopUploader) UpdateLocalSchema(ctx context.Context, schema model.Schema) error { return nil }
func (n *NopUploader) GetTableSchemaInWarehouse(tableName string) model.TableSchema     { return nil }
func (n *NopUploader) GetTableSchemaInUpload(tableName string) model.TableSchema        { return nil }
func (n *NopUploader) ShouldOnDedupUseNewRecord() bool                                  { return false }
func (n *NopUploader) UseRudderStorage() bool                                           { return false }
func (n *NopUploader) GetLoadFileGenStartTIme() time.Time                               { return time.Time{} }
func (n *NopUploader) GetLoadFileType() string                                          { return "" }
func (n *NopUploader) CanAppend() bool                                                  { return false }
func (n *NopUploader) GetLoadFilesMetadata(ctx context.Context, options GetLoadFilesOptions) ([]LoadFile, error) {
	return nil, nil
}

func (n *NopUploader) GetSampleLoadFileLocation(ctx context.Context, tableName string) (string, error) {
	return "", nil
}

func (n *NopUploader) GetSingleLoadFile(ctx context.Context, tableName string) (LoadFile, error) {
	return LoadFile{}, nil
}
func (n *NopUploader) GetFirstLastEvent() (time.Time, time.Time) { return time.Time{}, time.Time{} }
