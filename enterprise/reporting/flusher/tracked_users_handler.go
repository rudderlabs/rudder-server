package flusher

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/segmentio/go-hll"
)

type TrackedUsersHandler struct {
}

func CreateTrackedUsersFlusher(ctx context.Context, c types.SyncerConfig, log logger.Logger, stats stats.Stats, table string) *Flusher {
	labels := []string{"workspace_id", "source_id", "instance_id"}
	values := []string{"userid_hll", "anonymousid_hll", "identified_anonymousid_hll"}

	reportingURL := fmt.Sprintf("%s/trackedUser", strings.TrimSuffix(config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/"), "/"))

	maxOpenConns := config.GetIntVar(4, 1, "Reporting.maxOpenConnections")
	db, err := db.NewPostgresDB(c.ConnInfo, maxOpenConns)
	if err != nil {
		log.Errorw("failed to create database", "error", err)
		return nil
	}

	tuHandler := &TrackedUsersHandler{}
	flusher := NewFlusher(ctx, db, log, stats, table, labels, values, reportingURL, tuHandler)
	return flusher
}

func (t *TrackedUsersHandler) Aggregate(aggReport interface{}, report interface{}) error {
	tuReport := report.(*TrackedUsersReport)
	tuAggReport := aggReport.(*TrackedUsersReport)

	tuAggReport.UserIDHLL.Union(tuReport.UserIDHLL)
	tuAggReport.AnonymousIDHLL.Union(tuReport.AnonymousIDHLL)
	tuAggReport.IdentifiedAnonymousIDHLL.Union(tuReport.IdentifiedAnonymousIDHLL)

	return nil
}

func (t *TrackedUsersHandler) decodeHLL(encoded string) (*hll.Hll, error) {
	data, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	hll, err := hll.FromBytes(data)
	if err != nil {
		return nil, err
	}
	return &hll, nil
}

func (t *TrackedUsersHandler) Decode(report map[string]interface{}) (interface{}, error) {
	tuReport := &TrackedUsersReport{
		ReportedAt:  report["reported_at"].(time.Time),
		WorkspaceID: report["workspace_id"].(string),
		SourceID:    report["source_id"].(string),
		InstanceID:  report["instance_id"].(string),
	}
	userIDHLL, err := t.decodeHLL(report["userid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.UserIDHLL = *userIDHLL

	anonymousIDHLL, err := t.decodeHLL(report["anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.AnonymousIDHLL = *anonymousIDHLL

	identifiedAnonymousIDHLL, err := t.decodeHLL(report["identified_anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.IdentifiedAnonymousIDHLL = *identifiedAnonymousIDHLL

	return tuReport, nil
}
