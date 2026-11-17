package encoding

import (
	"io"
	"math"
	"os"

	"github.com/xitongsys/parquet-go/parquet"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	defaultParquetPageSize = 8192

	UUIDTsColumn           = "uuid_ts"
	LoadedAtColumn         = "loaded_at"
	MaxComputeTimeFormat   = "2006-01-02 15:04:05.999999"
	MaxComputeUUIDTsFormat = "2006-01-02 15:04:05"
	BQLoadedAtFormat       = "2006-01-02 15:04:05.999999 Z"
	BQUuidTSFormat         = "2006-01-02 15:04:05 Z"
)

var pkgLogger = logger.NewLogger().Child("warehouse").Child("encoding")

type Factory struct {
	config struct {
		maxStagingFileReadBufferCapacityInK int
		parquetParallelWriters              config.ValueLoader[int64]
		parquetPageSize                     config.ValueLoader[int64]
		parquetCompression                  config.ValueLoader[string]
		disableParquetColumnIndex           config.ValueLoader[bool]
	}
}

func NewFactory(conf *config.Config) *Factory {
	m := &Factory{}

	m.config.maxStagingFileReadBufferCapacityInK = conf.GetIntVar(10240, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	m.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Warehouse.parquetParallelWriters")
	m.config.parquetPageSize = conf.GetReloadableInt64Var(defaultParquetPageSize, 1, "Warehouse.parquetPageSize")
	m.config.parquetCompression = conf.GetReloadableStringVar(parquet.CompressionCodec_SNAPPY.String(), "Warehouse.parquetCompression")
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
		if pageSize < 1 || pageSize > math.MaxInt32 {
			pkgLogger.Warnn("invalid parquet page size, using default", logger.NewIntField("pageSize", pageSize), logger.NewIntField("defaultPageSize", defaultParquetPageSize))
			pageSize = defaultParquetPageSize
		}
		compression := m.config.parquetCompression.Load()
		codec, err := parquet.CompressionCodecFromString(compression)
		supported := err == nil
		if supported {
			switch codec {
			case parquet.CompressionCodec_UNCOMPRESSED, parquet.CompressionCodec_SNAPPY,
				parquet.CompressionCodec_GZIP, parquet.CompressionCodec_LZ4, parquet.CompressionCodec_ZSTD:
			default:
				supported = false
			}
		}
		if !supported {
			pkgLogger.Warnn("invalid or unsupported parquet compression, using SNAPPY", logger.NewStringField("compression", compression))
			codec = parquet.CompressionCodec_SNAPPY
		}
		return createParquetWriter(outputFilePath, schema, destType, m.config.parquetParallelWriters.Load(), m.config.disableParquetColumnIndex.Load(), pageSize, codec)
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
