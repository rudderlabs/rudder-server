package trackedusers

//go:generate mockgen -destination=./mocks/mock_data_collector.go -package=mockdatacollector github.com/rudderlabs/rudder-server/enterprise/trackedusers DataCollector

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/lib/pq"

	"github.com/spaolacci/murmur3"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
	txn "github.com/rudderlabs/rudder-server/utils/tx"
	"github.com/segmentio/go-hll"
)

const (
	idTypeUserID                       = "userID"
	idTypeAnonymousID                  = "anonymousID"
	idTypeUserIDAnonymousIDCombination = "userIDAnonymousIDCombination"

	// changing this will be non backwards compatible
	murmurSeed = 123
)

// DataCollector is interface to collect data from jobs
type DataCollector interface {
	CollectData(ctx context.Context, jobs []*jobsdb.JobT, tx *txn.Tx) error
}

type UniqueUsersCollector struct {
	log         logger.Logger
	hllSettings *hll.Settings
	instanceID  string
}

func NewUniqueUsersCollector(log logger.Logger, dbConn string) (*UniqueUsersCollector, error) {
	dbHandle, err := sql.Open("postgres", dbConn)
	if err != nil {
		panic(err)
	}
	dbHandle.SetMaxOpenConns(1)

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "tracked_users_reports_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("tracked_users")
	if err != nil {
		return nil, fmt.Errorf("could not run tracked_users_reports migrations: %w", err)
	}

	return &UniqueUsersCollector{
		log: log,
		hllSettings: &hll.Settings{
			Log2m:             config.GetInt("TrackedUsers.precision", 14),
			Regwidth:          config.GetInt("TrackedUsers.registerWidth", 5),
			ExplicitThreshold: hll.AutoExplicitThreshold,
			SparseEnabled:     true,
		},
		instanceID: config.GetString("INSTANCE_ID", "1"),
	}, nil
}

func (u *UniqueUsersCollector) CollectData(ctx context.Context, jobs []*jobsdb.JobT, tx *txn.Tx) error {
	if len(jobs) == 0 {
		return nil
	}
	workspaceSourceUserIdTypeMap := make(map[string]map[string]map[string]*hll.Hll)
	reportedAt := time.Now().UTC()
	for _, job := range jobs {
		if job.WorkspaceId == "" {
			u.log.Warn("workspace_id not found in job", logger.NewIntField("jobId", job.JobID))
			continue
		}

		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		if sourceID == "" {
			u.log.Warn("source_id not found in job parameters", obskit.WorkspaceID(job.WorkspaceId),
				logger.NewIntField("jobId", job.JobID))
			continue
		}
		userID := gjson.GetBytes(job.EventPayload, "batch.0.userId").String()
		anonymousID := gjson.GetBytes(job.EventPayload, "batch.0.anonymousId").String()

		if userID == "" && anonymousID == "" {
			u.log.Warn("both userID and anonymousID not found in job event payload", obskit.WorkspaceID(job.WorkspaceId),
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
			workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID] = u.recordIdentifier(workspaceSourceUserIdTypeMap[job.WorkspaceId][sourceID], combinedUserIDAnonymousID, idTypeUserIDAnonymousIDCombination)
		}
	}

	if len(workspaceSourceUserIdTypeMap) == 0 {
		u.log.Warn("no data to collect", obskit.WorkspaceID(jobs[0].WorkspaceId))
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tracked_users_reports",
		"workspace_id",
		"instance_id",
		"source_id",
		"reported_at",
		"userid_hll",
		"anonymousid_hll",
		"identified_anonymousid_hll",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %v", err)
	}
	defer func() { _ = stmt.Close() }()

	for workspaceID, sourceUserIdTypeMap := range workspaceSourceUserIdTypeMap {
		for sourceID, userIdTypeMap := range sourceUserIdTypeMap {
			_, err := stmt.Exec(workspaceID,
				u.instanceID,
				sourceID,
				reportedAt,
				hllToString(userIdTypeMap[idTypeUserID]),
				hllToString(userIdTypeMap[idTypeAnonymousID]),
				hllToString(userIdTypeMap[idTypeUserIDAnonymousIDCombination]),
			)
			if err != nil {
				return fmt.Errorf("executing statement: %v", err)
			}

		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("executing final statement: %v", err)
	}
	return nil
}

// convert hll to hexadecimal encoding compatible with Aggregate Knowledge HLL Storage Spec
func hllToString(hll *hll.Hll) string {
	if hll != nil {
		return "\\x" + hex.EncodeToString(hll.ToBytes())
	}
	return "\\x"
}

func combineUserIDAnonymousID(userID string, anonymousID string) string {
	return userID + ":" + anonymousID
}

func (u *UniqueUsersCollector) recordIdentifier(idTypeHllMap map[string]*hll.Hll, identifier string, identifierType string) map[string]*hll.Hll {
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
