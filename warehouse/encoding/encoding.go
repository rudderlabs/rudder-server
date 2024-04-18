package encoding

import (
	"io"
	"os"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const (
	UUIDTsColumn     = "uuid_ts"
	LoadedAtColumn   = "loaded_at"
	BQLoadedAtFormat = "2006-01-02 15:04:05.999999 Z"
	BQUuidTSFormat   = "2006-01-02 15:04:05 Z"
)

type Factory struct {
	config struct {
		maxStagingFileReadBufferCapacityInK int
		parquetParallelWriters              config.ValueLoader[int64]
	}
}

func NewFactory(conf *config.Config) *Factory {
	m := &Factory{}

	m.config.maxStagingFileReadBufferCapacityInK = conf.GetIntVar(10240, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	m.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Warehouse.parquetParallelWriters")

	return m
}

// LoadFileWriter is an interface for writing events to a load file
type LoadFileWriter interface {
	WriteGZ(s string) error
	Write(p []byte) (int, error)
	WriteRow(r []interface{}) error
	Close() error
	GetLoadFile() *os.File
}

func (m *Factory) NewLoadFileWriter(loadFileType, outputFilePath string, schema model.TableSchema, destType string) (LoadFileWriter, error) {
	switch loadFileType {
	case warehouseutils.LoadFileTypeParquet:
		return createParquetWriter(outputFilePath, schema, destType, m.config.parquetParallelWriters.Load())
	default:
		return misc.CreateGZ(outputFilePath)
	}
}

// EventLoader is an interface for loading events into a load file
// It's used to load singular BatchRouterEvent events into a load file
type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFormat(columnName string) string
	AddColumn(columnName, columnType string, val interface{})
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
	case warehouseutils.BQ:
		return newJSONReader(r, m.config.maxStagingFileReadBufferCapacityInK)
	default:
		return newCsvReader(r)
	}
}
