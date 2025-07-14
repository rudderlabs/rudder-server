package cache

import (
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// StagingFileSchemaSnapshotsExpiryStrategy defines the interface for cache expiration strategies.
type StagingFileSchemaSnapshotsExpiryStrategy interface {
	IsExpired(snapshot *model.StagingFileSchemaSnapshot) bool
}

// StagingFileSchemaSnapshotsTimeBasedExpiryStrategy expires snapshots after a fixed duration from CreatedAt.
type StagingFileSchemaSnapshotsTimeBasedExpiryStrategy struct {
	duration time.Duration
}

func (t *StagingFileSchemaSnapshotsTimeBasedExpiryStrategy) IsExpired(snapshot *model.StagingFileSchemaSnapshot) bool {
	return time.Since(snapshot.CreatedAt) > t.duration
}

// NewStagingFileSchemaSnapshotsTimeBasedExpiryStrategy returns a time-based expiry strategy for the cache.
func NewStagingFileSchemaSnapshotsTimeBasedExpiryStrategy(duration time.Duration) StagingFileSchemaSnapshotsExpiryStrategy {
	return &StagingFileSchemaSnapshotsTimeBasedExpiryStrategy{duration: duration}
}
