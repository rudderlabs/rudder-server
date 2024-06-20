package flusher

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/segmentio/go-hll"
)

type TrackedUsersFlusher struct {
	*Flusher
}

func NewTrackedUsersFlusher(ctx context.Context, db db.Database, log logger.Logger, stats stats.Stats, tableName string, migrationTableName string) *TrackedUsersFlusher {
	trackedUsersFlusher := &TrackedUsersFlusher{}
	trackedUsersFlusher.Flusher = NewFlusher(trackedUsersFlusher)
	return trackedUsersFlusher
}

func (t *TrackedUsersFlusher) AddReportToAggregate(aggregatedReports map[string]interface{}, report map[string]interface{}) error {
	return nil
}

func (t *TrackedUsersFlusher) decodeHLL(encoded string) (*hll.Hll, error) {
	return nil, nil
}

func (t *TrackedUsersFlusher) deserialize(report map[string]interface{}) (*TrackedUsersReport, error) {
	return nil, nil
}
