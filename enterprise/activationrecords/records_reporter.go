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

// recordKey is the aggregation grain for activation records: one HLL sketch per
// (workspace, source, destination). origin is intentionally NOT part of the key —
// it is constant per source, so it is carried on the report as a plain column
// (recordAccumulator.origin) rather than splitting a single connection into
// multiple grains/rows.
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
	hllSettings := &hll.Settings{
		Log2m:             conf.GetIntVar(16, 1, "ActivationRecords.precision"),
		Regwidth:          conf.GetIntVar(5, 1, "ActivationRecords.registerWidth"),
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled:     true,
	}
	// Validate the HLL settings once, at startup, so a misconfigured precision or
	// registerWidth fails fast here (the app handler surfaces this error) instead of
	// panicking deep in the processor hot path on every metered job.
	if _, err := hll.NewHll(*hllSettings); err != nil {
		return nil, fmt.Errorf("invalid activation records HLL settings: %w", err)
	}
	return &UniqueActivationRecordsReporter{
		log:         log,
		hllSettings: hllSettings,
		instanceID:  config.GetStringVar("1", "INSTANCE_ID"),
		stats:       stats,
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
			u.recordSkip("missing_workspace")
			continue
		}

		sourceID := jsonparser.GetStringOrEmpty(job.Parameters, "source_id")
		if sourceID == "" {
			u.log.Warnn("source_id not found in job parameters",
				obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			u.recordSkip("missing_source")
			continue
		}
		destinationID := jsonparser.GetStringOrEmpty(job.Parameters, "destination_id")
		if destinationID == "" {
			u.log.Warnn("destination_id not found in job parameters",
				obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			u.recordSkip("missing_destination")
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
			// A record job should always carry a non-empty batch; this should not
			// happen in prod, so emit a skip stat we can alert on.
			u.recordSkip("missing_batch")
			continue
		}
		var batchElements []json.RawMessage
		if err := jsonrs.Unmarshal(batchRaw, &batchElements); err != nil {
			// batch field present but not a JSON array — should not happen in prod.
			u.recordSkip("invalid_batch")
			continue
		}
		for _, elem := range batchElements {
			fingerprint := jsonparser.GetStringOrEmpty(elem, "context", "activation", "fingerprint")
			if fingerprint == "" {
				u.recordSkip("missing_fingerprint")
				continue
			}
			origin := jsonparser.GetStringOrEmpty(elem, "context", "activation", "origin")
			if origin == "" {
				u.recordSkip("missing_origin")
				continue
			}
			acc, exists := accumulators[key]
			if !exists {
				newHll, hllErr := hll.NewHll(*u.hllSettings)
				if hllErr != nil {
					// Settings are validated at construction, so this is unreachable in
					// practice; degrade gracefully (skip + stat) rather than crash the
					// processor pipeline if it ever does happen.
					u.log.Errorn("creating HLL for activation records", obskit.Error(hllErr))
					u.recordSkip("hll_init_failed")
					continue
				}
				// origin is client-controlled (stamped by rudder-sources into the event
				// payload). Defensively cap its length before carrying it onto the report
				// and writing it to the DB, so a malformed/over-long value cannot bloat
				// the row or the reporting POST. The column is TEXT, so this is
				// belt-and-suspenders.
				truncated, wasTruncated := truncateRunes(origin, 256)
				if wasTruncated {
					u.stats.NewStat("activation_records_origin_truncated", stats.CountType).Increment()
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

// recordSkip increments the activation_records_skipped counter with a reason tag.
// A skip is fail-closed (the record is not metered); the reason lets us alert on
// conditions that should not occur in production (missing workspace/source/
// destination/batch/fingerprint/origin).
func (u *UniqueActivationRecordsReporter) recordSkip(reason string) {
	u.stats.NewTaggedStat("activation_records_skipped", stats.CountType, stats.Tags{
		"reason": reason,
	}).Increment()
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
