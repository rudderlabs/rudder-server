package archive

import (
	"context"
	"time"
)

func CronArchiver(ctx context.Context, a *Archiver) {
	for {
		select {
		case <-ctx.Done():
			a.Logger.Infof("context is cancelled, stopped running archiving")
			return
		case <-time.After(archiverTickerTime):
			if archiveUploadRelatedRecords {
				err := a.Do(ctx)
				if err != nil {
					a.Logger.Errorf(`Error archiving uploads: %v`, err)
				}
			}
		}
	}
}
