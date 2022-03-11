package jobsdb

import (
	"context"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"
)

type Housekeep struct {
	ctx             context.Context
	cancel          context.CancelFunc
	backgroundGroup *errgroup.Group

	jobsdb *HandleT
}

func NewHousekeeper(jobsdb *HandleT) *Housekeep {
	return &Housekeep{
		jobsdb: jobsdb,
	}
}

func (h *Housekeep) Start() {
	h.ctx, h.cancel = context.WithCancel(context.Background())
	h.backgroundGroup, _ = errgroup.WithContext(h.ctx)

	// TODO: read/write seperation

	h.startNewDSLoop(h.ctx)
	h.startBackupDSLoop(h.ctx)
	h.startMigrateDSLoop(h.ctx)

}

func (h *Housekeep) startNewDSLoop(ctx context.Context) {
	h.backgroundGroup.Go(misc.WithBugsnag(func() error {
		h.jobsdb.addNewDSLoop(ctx)
		return nil
	}))
}

func (h *Housekeep) startBackupDSLoop(ctx context.Context) {
	var err error
	if h.jobsdb.BackupSettings.BackupEnabled {
		h.jobsdb.jobsFileUploader, err = h.jobsdb.getFileUploader()
		h.jobsdb.assertError(err)
		h.backgroundGroup.Go(misc.WithBugsnag(func() error {
			h.jobsdb.backupDSLoop(ctx)
			return nil
		}))
	}
}

func (h *Housekeep) startMigrateDSLoop(ctx context.Context) {
	h.backgroundGroup.Go(misc.WithBugsnag(func() error {
		h.jobsdb.migrateDSLoop(ctx)
		return nil
	}))
}

func (h *Housekeep) Stop() {
	h.cancel()
	h.backgroundGroup.Wait()
}
