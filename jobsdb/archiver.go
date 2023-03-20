package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/services/archiver"
)

var (
	archivalTimeInDays int
	archiverTickerTime time.Duration
)

func Init() {
	loadConfigArchiver()
}

func loadConfigArchiver() {
	config.RegisterIntConfigVariable(10, &archivalTimeInDays, true, 1, "JobsDB.archivalTimeInDays")
	config.RegisterDurationConfigVariable(1440, &archiverTickerTime, true, time.Minute, []string{"JobsDB.archiverTickerTime", "JobsDB.archiverTickerTimeInMin"}...) // default 1 day
}

func runArchiver(ctx context.Context, prefix string, dbHandle *sql.DB) {
	for {
		select {
		case <-time.After(archiverTickerTime):
		case <-ctx.Done():
			return
		}
		archiver.ArchiveOldRecords(fmt.Sprintf("%s_journal", prefix), "start_time", archivalTimeInDays, dbHandle)
	}
}
