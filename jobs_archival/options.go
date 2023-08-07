package archiver

import (
	"time"

	"github.com/rudderlabs/rudder-server/utils/payload"
)

type config interface {
	GetInt(key string, defaultValue int) (value int)
	GetInt64(key string, defaultValue int64) (value int64)
	GetString(key string, defaultValue string) (value string)
	GetDuration(key string, defaultValueInTimescaleUnits int64, timeScale time.Duration) time.Duration
}

type Option func(*archiver)

func WithAdaptiveLimit(limiter payload.AdaptiveLimiterFunc) Option {
	return func(a *archiver) {
		a.adaptivePayloadLimitFunc = limiter
	}
}

func WithArchiveTrigger(trigger func() <-chan time.Time) Option {
	return func(a *archiver) {
		a.archiveTrigger = trigger
	}
}

func WithPartitionStrategy(partitionType string) Option {
	return func(a *archiver) {
		a.partitionStrategy = getPartitionStrategy(partitionType)
	}
}

func WithArchiveFrom(archiveFrom string) Option {
	return func(a *archiver) {
		a.archiveFrom = archiveFrom
	}
}
