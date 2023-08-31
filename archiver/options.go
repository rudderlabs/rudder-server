package archiver

import (
	"time"

	"github.com/rudderlabs/rudder-server/utils/payload"
)

type configGetter interface {
	IsSet(key string) bool
	GetInt(key string, defaultValue int) (value int)
	GetInt64(key string, defaultValue int64) (value int64)
	GetString(key, defaultValue string) (value string)
	GetDuration(key string, defaultValueInTimescaleUnits int64, timeScale time.Duration) time.Duration
	GetBool(key string, defaultValue bool) (value bool)
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

func WithArchiveFrom(archiveFrom string) Option {
	return func(a *archiver) {
		a.archiveFrom = archiveFrom
	}
}
