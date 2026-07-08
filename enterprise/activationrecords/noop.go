package activationrecords

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/jobsdb"
	txn "github.com/rudderlabs/rudder-server/utils/tx"
)

// NoopActivationRecordsReporter is a no-op implementation of ActivationRecordsReporter.
type NoopActivationRecordsReporter struct{}

// NewNoopActivationRecordsReporter returns a NoopActivationRecordsReporter.
func NewNoopActivationRecordsReporter() *NoopActivationRecordsReporter {
	return &NoopActivationRecordsReporter{}
}

func (n *NoopActivationRecordsReporter) GenerateReportsFromJobs([]*jobsdb.JobT, map[string]string) []*ActivationRecord {
	return nil
}

func (n *NoopActivationRecordsReporter) ReportActivationRecords(context.Context, []*ActivationRecord, *txn.Tx) error {
	return nil
}

func (n *NoopActivationRecordsReporter) MigrateDatabase(string, *config.Config) error {
	return nil
}
