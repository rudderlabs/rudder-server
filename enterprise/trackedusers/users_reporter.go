package trackedusers

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	txn "github.com/rudderlabs/rudder-server/utils/tx"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/segmentio/go-hll"
	"github.com/spaolacci/murmur3"
	"github.com/tidwall/gjson"
)

const (
	idTypeUserID                = "userID"
	idTypeAnonymousID           = "anonymousID"
	idTypeIdentifiedAnonymousID = "identifiedAnonymousID"

	// changing this will be non backwards compatible
	murmurSeed = 123

	trackUsersTable = "tracked_users_reports"
)

type UsersReport struct {
	WorkspaceID              string
	SourceID                 string
	UserIDHll                *hll.Hll
	AnonymousIDHll           *hll.Hll
	IdentifiedAnonymousIDHll *hll.Hll
}

// UsersReporter is interface to report unique users from reports
type UsersReporter interface {
	ReportUsers(ctx context.Context, reports []*UsersReport, tx *txn.Tx) error
	GenerateReportsFromJobs(jobs []*jobsdb.JobT, sourceIdFilter map[string]bool) []*UsersReport
	MigrateDatabase(dbConn string, conf *config.Config) error
}

type UniqueUsersReporter struct {
	log         logger.Logger
	hllSettings *hll.Settings
	instanceID  string
	now         func() time.Time
	stats       stats.Stats
}

func NewUniqueUsersReporter(log logger.Logger, conf *config.Config, stats stats.Stats) (*UniqueUsersReporter, error) {
	return &UniqueUsersReporter{
		log: log,
		hllSettings: &hll.Settings{
			Log2m:             conf.GetInt("TrackedUsers.precision", 14),
			Regwidth:          conf.GetInt("TrackedUsers.registerWidth", 5),
			ExplicitThreshold: hll.AutoExplicitThreshold,
			SparseEnabled:     true,
		},
		instanceID: config.GetString("INSTANCE_ID", "1"),
		stats:      stats,
		now: func() time.Time {
			return time.Now()
		},
	}, nil
}

func (u *UniqueUsersReporter) MigrateDatabase(dbConn string, conf *config.Config) error {
	dbHandle, err := sql.Open("postgres", dbConn)
	if err != nil {
		return err
	}
	dbHandle.SetMaxOpenConns(1)

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "tracked_users_reports_migrations",
		ShouldForceSetLowerVersion: conf.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("tracked_users")
	if err != nil {
		return fmt.Errorf("migrating `tracked_users_reports` table: %w", err)
	}
	return nil
}

func (u *UniqueUsersReporter) GenerateReportsFromJobs(jobs []*jobsdb.JobT, sourceIDtoFilter map[string]bool) []*UsersReport {
	if len(jobs) == 0 {
		return nil
	}
	workspaceSourceUserIdTypeMap := make(map[string]map[string]map[string]*hll.Hll)
	for _, job := range jobs {
		if job.WorkspaceId == "" {
			u.log.Infon("[TRACKEDUSERS] workspace_id not found in job", logger.NewIntField("jobId", job.JobID))
			continue
		}

		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		if sourceID == "" {
			u.log.Infon("[TRACKEDUSERS] source_id not found in job parameters", obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			continue
		}

		if sourceIDtoFilter != nil && sourceIDtoFilter[sourceID] {
			u.log.Infon("[TRACKEDUSERS] source to filter", obskit.SourceID(sourceID))
			continue
		}
		userID := gjson.GetBytes(job.EventPayload, "batch.0.userId").String()
		anonymousID := gjson.GetBytes(job.EventPayload, "batch.0.anonymousId").String()

		if userID == "" && anonymousID == "" {
			u.log.Infon("[TRACKEDUSERS] both userID and anonymousID not found in job event payload", obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			continue
		}

		if workspaceSourceUserIdTypeMap[job.WorkspaceId] == nil {
			workspaceSourceUserIdTypeMap[job.WorkspaceId] = make(map[string]map[string]*hll.Hll)
		}

		if userID != "" {
			workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID] = u.recordIdentifier(workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID], userID, idTypeUserID)
		}

		if anonymousID != "" {
			workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID] = u.recordIdentifier(workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID], anonymousID, idTypeAnonymousID)
		}

		if userID != "" && anonymousID != "" {
			combinedUserIDAnonymousID := combineUserIDAnonymousID(userID, anonymousID)
			workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID] = u.recordIdentifier(workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID], combinedUserIDAnonymousID, idTypeIdentifiedAnonymousID)
		}
	}

	if len(workspaceSourceUserIdTypeMap) == 0 {
		u.log.Infon("[TRACKEDUSERS] no data to collect in", obskit.WorkspaceID(jobs[0].WorkspaceId))
		return nil
	}

	reports := make([]*UsersReport, 0)
	for workspaceID, sourceUserMp := range workspaceSourceUserIdTypeMap {
		reports = append(reports, lo.MapToSlice(sourceUserMp, func(sourceID string, userIdTypeMap map[string]*hll.Hll) *UsersReport {
			return &UsersReport{
				WorkspaceID:              workspaceID,
				SourceID:                 sourceID,
				UserIDHll:                userIdTypeMap[idTypeUserID],
				AnonymousIDHll:           userIdTypeMap[idTypeAnonymousID],
				IdentifiedAnonymousIDHll: userIdTypeMap[idTypeIdentifiedAnonymousID],
			}
		})...)
	}
	return reports
}

func (u *UniqueUsersReporter) ReportUsers(ctx context.Context, reports []*UsersReport, tx *txn.Tx) error {
	if len(reports) == 0 {
		u.log.Info("[TRACKEDUSERS] no reports to track")
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(trackUsersTable,
		"workspace_id",
		"instance_id",
		"source_id",
		"reported_at",
		"userid_hll",
		"anonymousid_hll",
		"identified_anonymousid_hll",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, report := range reports {
		u.recordHllSizeStats(report)
		userIDHllString, err := u.hllToString(report.UserIDHll)
		if err != nil {
			return fmt.Errorf("converting user id hll to string: %w", err)
		}
		anonIDHllString, err := u.hllToString(report.AnonymousIDHll)
		if err != nil {
			return fmt.Errorf("converting anon id hll to string: %w", err)
		}
		identifiedAnnIDHllString, err := u.hllToString(report.IdentifiedAnonymousIDHll)
		if err != nil {
			return fmt.Errorf("converting identified anon id hll to string: %w", err)
		}
		_, err = stmt.Exec(report.WorkspaceID,
			u.instanceID,
			report.SourceID,
			u.now(),
			userIDHllString,
			anonIDHllString,
			identifiedAnnIDHllString,
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

// convert hll to hexadecimal encoding
func (u *UniqueUsersReporter) hllToString(hllStruct *hll.Hll) (string, error) {
	if hllStruct == nil {
		newHllStruct, err := hll.NewHll(*u.hllSettings)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(newHllStruct.ToBytes()), nil
	}
	return hex.EncodeToString(hllStruct.ToBytes()), nil
}

func combineUserIDAnonymousID(userID, anonymousID string) string {
	return userID + ":" + anonymousID
}

func (u *UniqueUsersReporter) recordIdentifier(idTypeHllMap map[string]*hll.Hll, identifier, identifierType string) map[string]*hll.Hll {
	if idTypeHllMap == nil {
		idTypeHllMap = make(map[string]*hll.Hll)
	}
	if idTypeHllMap[identifierType] == nil {
		newHll, err := hll.NewHll(*u.hllSettings)
		if err != nil {
			panic(err)
		}
		idTypeHllMap[identifierType] = &newHll
	}
	idTypeHllMap[identifierType].AddRaw(murmur3.Sum64WithSeed([]byte(identifier), murmurSeed))
	return idTypeHllMap
}

func (u *UniqueUsersReporter) recordHllSizeStats(report *UsersReport) {
	if report.UserIDHll != nil {
		u.stats.NewTaggedStat("tracked_users_hll_bytes", stats.HistogramType, stats.Tags{
			"workspace_id": report.WorkspaceID,
			"source_id":    report.SourceID,
			"identifier":   idTypeUserID,
		}).Observe(float64(len(report.UserIDHll.ToBytes())))
	}
	if report.AnonymousIDHll != nil {
		u.stats.NewTaggedStat("tracked_users_hll_bytes", stats.HistogramType, stats.Tags{
			"workspace_id": report.WorkspaceID,
			"source_id":    report.SourceID,
			"identifier":   idTypeAnonymousID,
		}).Observe(float64(len(report.AnonymousIDHll.ToBytes())))
	}
	if report.IdentifiedAnonymousIDHll != nil {
		u.stats.NewTaggedStat("tracked_users_hll_bytes", stats.HistogramType, stats.Tags{
			"workspace_id": report.WorkspaceID,
			"source_id":    report.SourceID,
			"identifier":   idTypeIdentifiedAnonymousID,
		}).Observe(float64(len(report.IdentifiedAnonymousIDHll.ToBytes())))
	}
}
