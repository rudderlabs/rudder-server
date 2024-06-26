package flusher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/segmentio/go-hll"
)

type TrackedUsersReport struct {
	ReportedAt                  time.Time `json:"reportedAt"`
	WorkspaceID                 string    `json:"workspaceId"`
	SourceID                    string    `json:"sourceId"`
	InstanceID                  string    `json:"instanceId"`
	UserIDHLL                   hll.Hll   `json:"-"`
	AnonymousIDHLL              hll.Hll   `json:"-"`
	IdentifiedAnonymousIDHLL    hll.Hll   `json:"-"`
	UserIDHLLHex                string    `json:"userIdHLL"`
	AnonymousIDHLLHex           string    `json:"anonymousIdHLL"`
	IdentifiedAnonymousIDHLLHex string    `json:"identifiedAnonymousIdHLL"`
}

func (t *TrackedUsersReport) MarshalJSON() ([]byte, error) {
	t.UserIDHLLHex = hex.EncodeToString(t.UserIDHLL.ToBytes())
	t.AnonymousIDHLLHex = hex.EncodeToString(t.AnonymousIDHLL.ToBytes())
	t.IdentifiedAnonymousIDHLLHex = hex.EncodeToString(t.IdentifiedAnonymousIDHLL.ToBytes())

	type Alias TrackedUsersReport
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	})
}

type TrackedUsersHandler struct {
	table  string
	labels []string
}

var (
	trackedUsersHandlerInstance *TrackedUsersHandler
	flusherInstance             *Flusher
	once                        sync.Once
)

func CreateTrackedUsersFlusher(ctx context.Context, log logger.Logger, stats stats.Stats, table string) *Flusher {
	once.Do(func() {
		labels := []string{"workspace_id", "source_id", "instance_id"}
		values := []string{"userid_hll", "anonymousid_hll", "identified_anonymousid_hll"}

		reportingURL := fmt.Sprintf("%s/trackedUser", strings.TrimSuffix(config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/"), "/"))

		connStr := misc.GetConnectionString(config.Default, "reporting")
		maxOpenConns := config.GetIntVar(4, 1, "Reporting.maxOpenConnections")
		db := db.NewPostgresDB(connStr, maxOpenConns)

		trackedUsersHandlerInstance = &TrackedUsersHandler{
			table:  table,
			labels: labels,
		}

		flusherInstance = NewFlusher(ctx, db, log, stats, table, labels, values, reportingURL, true, trackedUsersHandlerInstance)
	})

	return flusherInstance
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

func (t *TrackedUsersHandler) Decode(r map[string]interface{}) (interface{}, error) {
	tuReport := &TrackedUsersReport{
		ReportedAt:  r["reported_at"].(time.Time),
		WorkspaceID: r["workspace_id"].(string),
		SourceID:    r["source_id"].(string),
		InstanceID:  r["instance_id"].(string),
	}

	userIDHLL, err := t.decodeHLL(r["userid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.UserIDHLL = *userIDHLL

	anonymousIDHLL, err := t.decodeHLL(r["anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.AnonymousIDHLL = *anonymousIDHLL

	identifiedAnonymousIDHLL, err := t.decodeHLL(r["identified_anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.IdentifiedAnonymousIDHLL = *identifiedAnonymousIDHLL

	return tuReport, nil
}
