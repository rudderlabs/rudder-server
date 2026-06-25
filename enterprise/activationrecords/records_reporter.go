package activationrecords

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/go-hll"
	"github.com/spaolacci/murmur3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonparser"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	txn "github.com/rudderlabs/rudder-server/utils/tx"
)

const (
	// changing this will be non backwards compatible
	murmurSeed = 123

	activationRecordsTable = "activation_records_reports"
)

type recordKey struct {
	workspaceID   string
	sourceID      string
	destinationID string
}

type recordAccumulator struct {
	origin string
	hll    *hll.Hll
}

// ActivationRecord holds a single aggregated activation record for a workspace/source/destination grain.
type ActivationRecord struct {
	WorkspaceID    string
	SourceID       string
	DestinationID  string
	Origin         string
	FingerprintHll *hll.Hll
}

// ActivationRecordsReporter is the interface to report monthly active records (MAR).
type ActivationRecordsReporter interface {
	GenerateReportsFromJobs(jobs []*jobsdb.JobT) []*ActivationRecord
	ReportActivationRecords(ctx context.Context, reports []*ActivationRecord, tx *txn.Tx) error
	MigrateDatabase(dbConn string, conf *config.Config) error
}

// UniqueActivationRecordsReporter implements ActivationRecordsReporter using HLL sketches.
type UniqueActivationRecordsReporter struct {
	log         logger.Logger
	hllSettings *hll.Settings
	instanceID  string
	now         func() time.Time
	stats       stats.Stats
}

// NewUniqueActivationRecordsReporter constructs a UniqueActivationRecordsReporter.
func NewUniqueActivationRecordsReporter(log logger.Logger, conf *config.Config, stats stats.Stats) (*UniqueActivationRecordsReporter, error) {
	return &UniqueActivationRecordsReporter{
		log: log,
		hllSettings: &hll.Settings{
			Log2m:             conf.GetIntVar(16, 1, "ActivationRecords.precision"),
			Regwidth:          conf.GetIntVar(5, 1, "ActivationRecords.registerWidth"),
			ExplicitThreshold: hll.AutoExplicitThreshold,
			SparseEnabled:     true,
		},
		instanceID: config.GetStringVar("1", "INSTANCE_ID"),
		stats:      stats,
		now: func() time.Time {
			return timeutil.Now()
		},
	}, nil
}

func (u *UniqueActivationRecordsReporter) MigrateDatabase(dbConn string, conf *config.Config) error {
	dbHandle, err := sql.Open("postgres", dbConn)
	if err != nil {
		return err
	}
	dbHandle.SetMaxOpenConns(1)
	err = u.stats.RegisterCollector(collectors.NewDatabaseSQLStats("activation_records_reports", dbHandle))
	if err != nil {
		u.log.Errorn("error registering database sql stats", obskit.Error(err))
	}

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "activation_records_reports_migrations",
		ShouldForceSetLowerVersion: conf.GetBoolVar(true, "SQLMigrator.forceSetLowerVersion"),
	}
	err = m.Migrate("activation_records")
	if err != nil {
		return fmt.Errorf("migrating `activation_records_reports` table: %w", err)
	}
	return nil
}

// GenerateReportsFromJobs aggregates activation records from a batch of jobs.
// It is FAIL-CLOSED: jobs missing fingerprint or origin are skipped (counted via stats).
func (u *UniqueActivationRecordsReporter) GenerateReportsFromJobs(jobs []*jobsdb.JobT) []*ActivationRecord {
	if len(jobs) == 0 {
		return nil
	}

	accumulators := make(map[recordKey]*recordAccumulator)

	for _, job := range jobs {
		if job.WorkspaceId == "" {
			u.log.Warnn("workspace_id not found in job", logger.NewIntField("jobId", job.JobID))
			continue
		}

		sourceID := jsonparser.GetStringOrEmpty(job.Parameters, "source_id")
		destinationID := jsonparser.GetStringOrEmpty(job.Parameters, "destination_id")
		if sourceID == "" || destinationID == "" {
			u.log.Warnn("source_id or destination_id not found in job parameters",
				obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			u.stats.NewTaggedStat("activation_records_skipped", stats.CountType, stats.Tags{
				"reason": "missing_source_or_destination",
			}).Increment()
			continue
		}

		key := recordKey{
			workspaceID:   job.WorkspaceId,
			sourceID:      sourceID,
			destinationID: destinationID,
		}

		// Extract the raw batch array bytes, then iterate each element.
		batchRaw := jsonparser.GetValueOrEmpty(job.EventPayload, "batch")
		if len(batchRaw) == 0 {
			continue
		}
		var batchElements []json.RawMessage
		if err := jsonrs.Unmarshal(batchRaw, &batchElements); err != nil {
			// batch field absent or not an array — skip job gracefully.
			continue
		}
		for _, elem := range batchElements {
			fingerprint := jsonparser.GetStringOrEmpty(elem, "context", "activation", "fingerprint")
			origin := jsonparser.GetStringOrEmpty(elem, "context", "activation", "origin")
			if fingerprint == "" || origin == "" {
				u.stats.NewTaggedStat("activation_records_skipped", stats.CountType, stats.Tags{
					"reason": "missing_fingerprint_or_origin",
				}).Increment()
				continue
			}
			acc, exists := accumulators[key]
			if !exists {
				truncated, wasTruncated := truncateRunes(origin, 256)
				if wasTruncated {
					u.stats.NewTaggedStat("activation_records_origin_truncated", stats.CountType, stats.Tags{}).Increment()
				}
				newHll, hllErr := hll.NewHll(*u.hllSettings)
				if hllErr != nil {
					panic(hllErr)
				}
				acc = &recordAccumulator{origin: truncated, hll: &newHll}
				accumulators[key] = acc
			}
			acc.hll.AddRaw(murmur3.Sum64WithSeed([]byte(fingerprint), murmurSeed))
		}
	}

	if len(accumulators) == 0 {
		return nil
	}

	reports := make([]*ActivationRecord, 0, len(accumulators))
	for key, acc := range accumulators {
		reports = append(reports, &ActivationRecord{
			WorkspaceID:    key.workspaceID,
			SourceID:       key.sourceID,
			DestinationID:  key.destinationID,
			Origin:         acc.origin,
			FingerprintHll: acc.hll,
		})
	}
	return reports
}

// ReportActivationRecords writes activation records to the database via a COPY statement.
func (u *UniqueActivationRecordsReporter) ReportActivationRecords(ctx context.Context, reports []*ActivationRecord, tx *txn.Tx) error {
	if len(reports) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, misc.DBCopyIn(activationRecordsTable,
		"workspace_id",
		"instance_id",
		"source_id",
		"destination_id",
		"origin",
		"reported_at",
		"fingerprint_hll",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, report := range reports {
		u.recordHllSizeStats(report)
		fingerprintHllString, err := u.hllToString(report.FingerprintHll)
		if err != nil {
			return fmt.Errorf("converting fingerprint hll to string: %w", err)
		}
		_, err = stmt.Exec(
			report.WorkspaceID,
			u.instanceID,
			report.SourceID,
			report.DestinationID,
			report.Origin,
			u.now(),
			fingerprintHllString,
		)
		if err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("executing final statement: %w", err)
	}
	return nil
}

// hllToString converts an HLL struct to a hex-encoded string.
func (u *UniqueActivationRecordsReporter) hllToString(hllStruct *hll.Hll) (string, error) {
	if hllStruct == nil {
		newHllStruct, err := hll.NewHll(*u.hllSettings)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(newHllStruct.ToBytes()), nil
	}
	return hex.EncodeToString(hllStruct.ToBytes()), nil
}

// truncateRunes truncates s to at most max runes.
// Returns the (possibly truncated) string and whether truncation occurred.
func truncateRunes(s string, max int) (string, bool) {
	runes := []rune(s)
	if len(runes) <= max {
		return s, false
	}
	return string(runes[:max]), true
}

func (u *UniqueActivationRecordsReporter) recordHllSizeStats(report *ActivationRecord) {
	if report.FingerprintHll != nil {
		u.stats.NewTaggedStat("activation_records_hll_bytes", stats.HistogramType, stats.Tags{
			"workspace_id":   report.WorkspaceID,
			"source_id":      report.SourceID,
			"destination_id": report.DestinationID,
		}).Observe(float64(len(report.FingerprintHll.ToBytes())))
	}
}
