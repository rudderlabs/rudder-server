package jobsdb

import (
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
)

type OptsFunc func(jd *HandleT)

// WithClearDB, if set to true it will remove all existing tables
func WithClearDB(clearDB bool) OptsFunc {
	return func(jd *HandleT) {
		jd.clearAll = clearDB
	}
}

func WithRetention(period time.Duration) OptsFunc {
	return func(jd *HandleT) {
		jd.dsRetentionPeriod = period
	}
}

func WithQueryFilterKeys(filters QueryFiltersT) OptsFunc {
	return func(jd *HandleT) {
		jd.queryFilterKeys = filters
	}
}

func WithMigrationMode(mode string) OptsFunc {
	return func(jd *HandleT) {
		jd.migrationState.migrationMode = mode
	}
}

func WithStatusHandler() OptsFunc {
	return func(jd *HandleT) {
		jd.registerStatusHandler = true
	}
}

// WithPreBackupHandlers, sets pre-backup handlers
func WithPreBackupHandlers(preBackupHandlers []prebackup.Handler) OptsFunc {
	return func(jd *HandleT) {
		jd.preBackupHandlers = preBackupHandlers
	}
}

// WithMaxDsSize, sets the maximum ds size
func WithMaxDsSize(maxDsSize int) OptsFunc {
	return func(jd *HandleT) {
		jd.MaxDSSize = &maxDsSize
	}
}

// WithTriggerAddNewDs, sets the maximum ds size
func WithTriggerAddNewDs(triggerAddNewDS func() <-chan time.Time) OptsFunc {
	return func(jd *HandleT) {
		jd.TriggerAddNewDS = triggerAddNewDS
	}
}
