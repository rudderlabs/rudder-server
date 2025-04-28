package archiver

import (
	"time"

	"github.com/rudderlabs/rudder-server/utils/payload"
)

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
