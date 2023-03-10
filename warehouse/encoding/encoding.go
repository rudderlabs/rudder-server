package encoding

import (
	"github.com/rudderlabs/rudder-server/config"
)

const (
	UUIDTsColumn   = "uuid_ts"
	LoadedAtColumn = "loaded_at"
)

var (
	maxStagingFileReadBufferCapacityInK int
	parquetParallelWriters              int64
)

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, false, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	config.RegisterInt64ConfigVariable(8, &parquetParallelWriters, true, 1, "Warehouse.parquetParallelWriters")
}
