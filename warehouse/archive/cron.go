package archive

import (
	"context"
	"time"
)

func CronArchiver(ctx context.Context, a *Archiver) {
	for {
		select {
		case <-ctx.Done():
			a.log.Infof("context is cancelled, stopped running archiving")
			return
		case <-time.After(a.config.archiverTickerTime):
			if a.config.archiveUploadRelatedRecords {
				err := a.Do(ctx)
				if err != nil {
					a.log.Errorf(`Error archiving uploads: %v`, err)
				}
			}
		}
	}
}
