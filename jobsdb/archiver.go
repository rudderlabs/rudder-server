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
	loadConfigArchiver()
}

func loadConfigArchiver() {
	config.RegisterIntConfigVariable(10, &archivalTimeInDays, true, 1, "JobsDB.archivalTimeInDays")
	config.RegisterDurationConfigVariable(time.Duration(1440), &archiverTickerTime, true, time.Minute, []string{"JobsDB.archiverTickerTime","JobsDB.archiverTickerTimeInMin"}...) // default 1 day
}

func runArchiver(prefix string, dbHandle *sql.DB) {
	for {
		archiver.ArchiveOldRecords(fmt.Sprintf("%s_journal", prefix), "start_time", archivalTimeInDays, dbHandle)
		time.Sleep(archiverTickerTime)
	}
}
