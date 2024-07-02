package aggregator

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/go-hll"
)

type TrackedUsersInAppAggregator struct {
	db *sql.DB
}

func NewTrackedUsersInAppAggregator(db *sql.DB) *TrackedUsersInAppAggregator {
	return &TrackedUsersInAppAggregator{db: db}
}

func (a *TrackedUsersInAppAggregator) Aggregate(ctx context.Context, start, end time.Time) (jsonReports []json.RawMessage, total int, unique int, err error) {
	selectColumns := "workspace_id, source_id, instance_id, userid_hll, anonymousid_hll, identified_anonymousid_hll"
	query := fmt.Sprintf(`SELECT %s FROM tracked_users_reports WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at`, selectColumns)

	rows, err := a.db.Query(query, start, end)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	total = 0
	aggReportsMap := make(map[string]TrackedUsersReport)
	for rows.Next() {
		total += 1
		r := TrackedUsersReport{}
		err := rows.Scan(&r.WorkspaceID, &r.SourceID, &r.InstanceID, &r.UserIDHLLHex, &r.AnonymousIDHLLHex, &r.IdentifiedAnonymousIDHLLHex)
		if err != nil {
			return nil, 0, 0, err
		}
		r.UserIDHLL, err = a.decodeHLL(r.UserIDHLLHex)
		if err != nil {
			return nil, 0, 0, err
		}
		r.AnonymousIDHLL, err = a.decodeHLL(r.AnonymousIDHLLHex)
		if err != nil {
			return nil, 0, 0, err
		}
		r.IdentifiedAnonymousIDHLL, err = a.decodeHLL(r.IdentifiedAnonymousIDHLLHex)
		if err != nil {
			return nil, 0, 0, err
		}

		k := fmt.Sprintf("%s-%s-%s", r.WorkspaceID, r.SourceID, r.InstanceID)

		if agg, exists := aggReportsMap[k]; exists {
			if err := a.aggregate(agg, r); err != nil {
				return nil, 0, 0, err
			}
		} else {
			aggReportsMap[k] = r
		}

	}

	jsonReports, err = marshalReports(aggReportsMap)
	if err != nil {
		return nil, 0, 0, err
	}

	unique = len(aggReportsMap)

	return jsonReports, total, unique, nil
}

func (a *TrackedUsersInAppAggregator) aggregate(aggReport, report TrackedUsersReport) error {
	aggReport.UserIDHLL.Union(*report.UserIDHLL)
	aggReport.AnonymousIDHLL.Union(*report.AnonymousIDHLL)
	aggReport.IdentifiedAnonymousIDHLL.Union(*report.IdentifiedAnonymousIDHLL)

	return nil
}

func (a *TrackedUsersInAppAggregator) decodeHLL(encoded string) (*hll.Hll, error) {
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

func marshalReports(aggReportsMap map[string]TrackedUsersReport) ([]json.RawMessage, error) {
	jsonReports := make([]json.RawMessage, 0, len(aggReportsMap))
	for _, v := range aggReportsMap {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		jsonReports = append(jsonReports, data)
	}
	return jsonReports, nil
}
