package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/archiver"
)

var (
	archivalTimeInDays int
	archiverTickerTime time.Duration
)

func init() {
	archivalTimeInDays = config.GetInt("JobsDB.archivalTimeInDays", 10)
	archiverTickerTime = config.GetDuration("JobsDB.archiverTickerTimeInMin", 1440) * time.Minute // default 1 day
}

func runArchiver(prefix string, dbHandle *sql.DB) {
	for {
		archiver.ArchiveOldRecords(fmt.Sprintf("%s_journal", prefix), "start_time", archivalTimeInDays, dbHandle)
		time.Sleep(archiverTickerTime)
	}
}
