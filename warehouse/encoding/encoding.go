package encoding

import (
	"io"
	"os"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	defaultParquetPageSize = 8192
	minParquetPageSize     = 4 << 10
	maxParquetPageSize     = 8 << 20

	UUIDTsColumn           = "uuid_ts"
	LoadedAtColumn         = "loaded_at"
	MaxComputeTimeFormat   = "2006-01-02 15:04:05.999999"
	MaxComputeUUIDTsFormat = "2006-01-02 15:04:05"
	BQLoadedAtFormat       = "2006-01-02 15:04:05.999999 Z"
	BQUuidTSFormat         = "2006-01-02 15:04:05 Z"
)

type Factory struct {
	logger                 logger.Logger
	warnedInvalidPageSizes sync.Map
	config                 struct {
		maxStagingFileReadBufferCapacityInK int
		parquetParallelWriters              config.ValueLoader[int64]
		parquetPageSize                     config.ValueLoader[int64]
		disableParquetColumnIndex           config.ValueLoader[bool]
	}
}

type FactoryOption func(*Factory)

func WithLogger(log logger.Logger) FactoryOption {
	return func(m *Factory) {
		m.logger = log.Child("encoding")
	}
}

func NewFactory(conf *config.Config, opts ...FactoryOption) *Factory {
	m := &Factory{logger: logger.NOP}
	for _, opt := range opts {
		opt(m)
	}

	m.config.maxStagingFileReadBufferCapacityInK = conf.GetIntVar(10240, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	m.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Warehouse.parquetParallelWriters")
	m.config.parquetPageSize = conf.GetReloadableInt64Var(defaultParquetPageSize, 1, "Warehouse.parquetPageSize")
	m.config.disableParquetColumnIndex = conf.GetReloadableBoolVar(true, "Warehouse.disableParquetColumnIndex")
	return m
}

// LoadFileWriter is an interface for writing events to a load file
type LoadFileWriter interface {
	WriteGZ(s string) error
	Write(p []byte) (int, error)
	WriteRow(r []any) error
	Close() error
	GetLoadFile() *os.File
}

func (m *Factory) NewLoadFileWriter(loadFileType, outputFilePath string, schema model.TableSchema, destType string) (LoadFileWriter, error) {
	switch loadFileType {
	case warehouseutils.LoadFileTypeParquet:
		pageSize := m.config.parquetPageSize.Load()
		if pageSize < minParquetPageSize || pageSize > maxParquetPageSize {
			if _, logged := m.warnedInvalidPageSizes.LoadOrStore(pageSize, struct{}{}); !logged {
				m.logger.Warnn("invalid parquet page size, using default", logger.NewIntField("pageSize", pageSize), logger.NewIntField("defaultPageSize", defaultParquetPageSize))
			}
			pageSize = defaultParquetPageSize
		}
		return createParquetWriter(outputFilePath, schema, destType, m.config.parquetParallelWriters.Load(), m.config.disableParquetColumnIndex.Load(), pageSize)
	default:
		return misc.CreateGZ(outputFilePath)
	}
}

// EventLoader is an interface for loading events into a load file
// It's used to load singular BatchRouterEvent events into a load file
type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFormat(columnName string) string
	AddColumn(columnName, columnType string, val any)
	AddRow(columnNames, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
	Write() error
}

func (m *Factory) NewEventLoader(w LoadFileWriter, loadFileType, destinationType string) EventLoader {
	switch loadFileType {
	case warehouseutils.LoadFileTypeJson:
		return newJSONLoader(w, destinationType)
	case warehouseutils.LoadFileTypeParquet:
		return newParquetLoader(w, destinationType)
	default:
		return newCSVLoader(w, destinationType)
	}
}

// EventReader is an interface for reading events from a load file
type EventReader interface {
	Read(columnNames []string) (record []string, err error)
}

func (m *Factory) NewEventReader(r io.Reader, destType string) EventReader {
	switch destType {
	case warehouseutils.BQ, warehouseutils.MAXCOMPUTE:
		return newJSONReader(r, m.config.maxStagingFileReadBufferCapacityInK)
	default:
		return newCsvReader(r)
	}
}
