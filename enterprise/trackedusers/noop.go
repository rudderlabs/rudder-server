package trackedusers

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/jobsdb"

	txn "github.com/rudderlabs/rudder-server/utils/tx"
)

type NoopDataCollector struct{}

func NewNoopDataCollector() *NoopDataCollector {
	return &NoopDataCollector{}
}

func (n *NoopDataCollector) ReportUsers(context.Context, []*UsersReport, *txn.Tx) error {
	return nil
}

func (n *NoopDataCollector) GenerateReportsFromJobs([]*jobsdb.JobT, map[string]bool) []*UsersReport {
	return nil
}

func (n *NoopDataCollector) MigrateDatabase(string, *config.Config) error {
	return nil
}
