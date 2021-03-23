package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/utils"
)

var (
	archivalTimeInDays int
	archiverTickerTime time.Duration
)

func init() {
	loadConfigArchiver()
	rruntime.Go(func() {
		updateArchiverConfigFile()
	})
}

func loadConfigArchiver() {
	archivalTimeInDays = config.GetInt("JobsDB.archivalTimeInDays", 10)
	archiverTickerTime = config.GetDuration("JobsDB.archiverTickerTimeInMin", 1440) * time.Minute // default 1 day

}

func updateArchiverConfigFile() {
	ch := make(chan utils.DataEvent)
	config.GetUpdatedConfig(ch, "ConfigUpdate")
	for {
		<-ch
		archiverReloadableConfig()
	}
}

func archiverReloadableConfig() {
	_archivalTimeInDays := config.GetInt("JobsDB.archivalTimeInDays", 10)
	if _archivalTimeInDays != archivalTimeInDays {
		archivalTimeInDays = _archivalTimeInDays
		pkgLogger.Info("JobsDB.archivalTimeInDays changes to %s", archivalTimeInDays)
	}
}

func runArchiver(prefix string, dbHandle *sql.DB) {
	for {
		archiver.ArchiveOldRecords(fmt.Sprintf("%s_journal", prefix), "start_time", archivalTimeInDays, dbHandle)
		time.Sleep(archiverTickerTime)
	}
}
