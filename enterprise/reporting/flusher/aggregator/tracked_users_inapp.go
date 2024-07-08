package aggregator

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/go-hll"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

const tableName = `tracked_users_reports`

type TrackedUsersInAppAggregator struct {
	db    *sql.DB
	stats stats.Stats

	reportsCounter    stats.Measurement
	aggReportsCounter stats.Measurement
}

func NewTrackedUsersInAppAggregator(db *sql.DB, s stats.Stats, conf *config.Config, module string) *TrackedUsersInAppAggregator {
	t := TrackedUsersInAppAggregator{db: db, stats: s}
	tags := stats.Tags{
		"instance": conf.GetString("INSTANCE_ID", "1"),
		"table":    tableName,
		"module":   module,
	}
	t.reportsCounter = t.stats.NewTaggedStat("reporting_flusher_get_reports_count", stats.HistogramType, tags)
	t.aggReportsCounter = t.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_count", stats.HistogramType, tags)
	return &t
}

func (a *TrackedUsersInAppAggregator) Aggregate(ctx context.Context, start, end time.Time) (jsonReports []json.RawMessage, err error) {
	query := `SELECT reported_at, workspace_id, source_id, instance_id, userid_hll, anonymousid_hll, identified_anonymousid_hll FROM ` + tableName + `  WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at`

	rows, err := a.db.Query(query, start, end)
	if err != nil {
		return nil, fmt.Errorf("cannot read reports %w", err)
	}
	defer rows.Close()

	total := 0
	aggReportsMap := make(map[string]*TrackedUsersReport)
	for rows.Next() {
		total += 1
		r := TrackedUsersReport{}
		err := rows.Scan(&r.ReportedAt, &r.WorkspaceID, &r.SourceID, &r.InstanceID, &r.UserIDHLLHex, &r.AnonymousIDHLLHex, &r.IdentifiedAnonymousIDHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot scan row %w", err)
		}
		r.UserIDHLL, err = a.decodeHLL(r.UserIDHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot decode hll %w", err)
		}
		r.AnonymousIDHLL, err = a.decodeHLL(r.AnonymousIDHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot decode hll %w", err)
		}
		r.IdentifiedAnonymousIDHLL, err = a.decodeHLL(r.IdentifiedAnonymousIDHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot decode hll %w", err)
		}

		k := fmt.Sprintf("%s-%s-%s", r.WorkspaceID, r.SourceID, r.InstanceID)

		if agg, exists := aggReportsMap[k]; exists {
			agg.UserIDHLL.Union(*r.UserIDHLL)
			agg.AnonymousIDHLL.Union(*r.AnonymousIDHLL)
			agg.IdentifiedAnonymousIDHLL.Union(*r.IdentifiedAnonymousIDHLL)
			aggReportsMap[k] = agg
		} else {
			aggReportsMap[k] = &r
		}

	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows errors %w", err)
	}

	jsonReports, err = marshalReports(aggReportsMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling reports %w", err)
	}

	a.reportsCounter.Observe(float64(total))
	a.aggReportsCounter.Observe(float64(len(jsonReports)))

	return jsonReports, nil
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

func marshalReports(aggReportsMap map[string]*TrackedUsersReport) ([]json.RawMessage, error) {
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
