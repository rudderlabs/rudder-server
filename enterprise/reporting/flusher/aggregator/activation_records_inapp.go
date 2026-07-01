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
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

const activationRecordsTableName = `activation_records_reports`

type ActivationRecordsInAppAggregator struct {
	db    *sql.DB
	stats stats.Stats

	reportsCounter    stats.Measurement
	aggReportsCounter stats.Measurement
}

func NewActivationRecordsInAppAggregator(db *sql.DB, s stats.Stats, conf *config.Config, module string) *ActivationRecordsInAppAggregator {
	a := ActivationRecordsInAppAggregator{db: db, stats: s}
	tags := stats.Tags{
		"instance": conf.GetStringVar("1", "INSTANCE_ID"),
		"table":    activationRecordsTableName,
		"module":   module,
	}
	a.reportsCounter = a.stats.NewTaggedStat("reporting_flusher_get_reports_count", stats.HistogramType, tags)
	a.aggReportsCounter = a.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_count", stats.HistogramType, tags)
	return &a
}

func (a *ActivationRecordsInAppAggregator) Aggregate(ctx context.Context, start, end time.Time) (jsonReports []json.RawMessage, err error) {
	query := `SELECT reported_at, workspace_id, source_id, destination_id, origin, instance_id, fingerprint_hll FROM activation_records_reports WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at`

	rows, err := a.db.Query(query, start, end)
	if err != nil {
		return nil, fmt.Errorf("cannot read reports %w", err)
	}
	defer rows.Close()

	total := 0
	aggReportsMap := make(map[string]*ActivationRecordsReport)
	for rows.Next() {
		total += 1
		r := ActivationRecordsReport{}
		err := rows.Scan(&r.ReportedAt, &r.WorkspaceID, &r.SourceID, &r.DestinationID, &r.Origin, &r.InstanceID, &r.FingerprintHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot scan row %w", err)
		}
		r.FingerprintHLL, err = a.decodeHLL(r.FingerprintHLLHex)
		if err != nil {
			return nil, fmt.Errorf("cannot decode hll %w", err)
		}

		k := fmt.Sprintf("%s-%s-%s-%s", r.WorkspaceID, r.SourceID, r.DestinationID, r.InstanceID)

		if agg, exists := aggReportsMap[k]; exists {
			agg.FingerprintHLL.Union(*r.FingerprintHLL)
			aggReportsMap[k] = agg
		} else {
			aggReportsMap[k] = &r
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows errors %w", err)
	}

	jsonReports, err = marshalActivationRecordsReports(aggReportsMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling reports %w", err)
	}

	a.reportsCounter.Observe(float64(total))
	a.aggReportsCounter.Observe(float64(len(jsonReports)))

	return jsonReports, nil
}

func (a *ActivationRecordsInAppAggregator) decodeHLL(encoded string) (*hll.Hll, error) {
	data, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	h, err := hll.FromBytes(data)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func marshalActivationRecordsReports(aggReportsMap map[string]*ActivationRecordsReport) ([]json.RawMessage, error) {
	jsonReports := make([]json.RawMessage, 0, len(aggReportsMap))
	for _, v := range aggReportsMap {
		data, err := jsonrs.Marshal(v)
		if err != nil {
			return nil, err
		}
		jsonReports = append(jsonReports, data)
	}
	return jsonReports, nil
}
