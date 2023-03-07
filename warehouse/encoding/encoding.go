package encoding

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	maxStagingFileReadBufferCapacityInK int
	parquetParallelWriters              int64
	pkgLogger                           logger.Logger
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("encoding")
}

func loadConfig() {
	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, false, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	config.RegisterInt64ConfigVariable(8, &parquetParallelWriters, true, 1, "Warehouse.parquetParallelWriters")
}
