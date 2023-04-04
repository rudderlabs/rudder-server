package encoding

import (
	"os"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const (
	UUIDTsColumn     = "uuid_ts"
	LoadedAtColumn   = "loaded_at"
	BQLoadedAtFormat = "2006-01-02 15:04:05.999999 Z"
	BQUuidTSFormat   = "2006-01-02 15:04:05 Z"
)

var (
	maxStagingFileReadBufferCapacityInK int
	parquetParallelWriters              int64
)

type LoadFileWriter interface {
	WriteGZ(s string) error
	Write(p []byte) (int, error)
	WriteRow(r []interface{}) error
	Close() error
	GetLoadFile() *os.File
}

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, false, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	config.RegisterInt64ConfigVariable(8, &parquetParallelWriters, true, 1, "Warehouse.parquetParallelWriters")
}
