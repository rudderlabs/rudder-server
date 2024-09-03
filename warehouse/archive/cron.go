package archive

import (
	"context"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

func CronArchiver(ctx context.Context, a *Archiver) {
	for {
		select {
		case <-ctx.Done():
			a.log.Infof("context is cancelled, stopped running archiving")
			return
		case <-time.After(a.config.archiverTickerTime.Load()):
			if a.config.archiveUploadRelatedRecords.Load() {
				err := a.Do(ctx)
				if err != nil {
					a.log.Errorn(`Error archiving uploads`, obskit.Error(err))
				}
			}
			if a.config.canDeleteUploads.Load() {
				err := a.Delete(ctx)
				if err != nil {
					a.log.Errorn(`Error deleting uploads`, obskit.Error(err))
				}
			}
		}
	}
}
