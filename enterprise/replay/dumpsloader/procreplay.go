package dumpsloader

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func (p *procErrorRequestHandler) Start() {
	p.g = errgroup.Group{}
	p.handleRecovery()
	p.g.Go(func() error {
		return p.fetchDumpsList(p.ctx)
	})
}

func (p *procErrorRequestHandler) Stop() error {
	p.cancel()
	return p.g.Wait()
}

func (p *procErrorRequestHandler) handleRecovery() {
	// remove dangling executing
	p.dbHandle.FailExecuting()
}

func (p *procErrorRequestHandler) fetchDumpsList(ctx context.Context) error {
	objects := make([]OrderedJobs, 0)
	p.log.Info("Fetching proc err files list")
	var err error
	maxItems := config.GetInt64("MAX_ITEMS", 1000)         // MAX_ITEMS is the max number of files to be fetched in one iteration from object storage
	uploadMaxItems := config.GetInt("UPLOAD_MAX_ITEMS", 1) // UPLOAD_MAX_ITEMS is the max number of objects to be uploaded to postgres

	iter := filemanager.IterateFilesWithPrefix(ctx,
		p.config.prefix,
		p.config.startAfterKey,
		maxItems,
		p.uploader,
	)
	for iter.Next() {
		object := iter.Get()
		if strings.Contains(object.Key, "rudder-proc-err-logs") {
			if object.LastModified.Before(p.config.startTime) || (object.LastModified.Sub(p.config.endTime).Hours() > 1) {
				p.log.Debugf("Skipping object: %v ObjectLastModifiedTime: %v", object.Key, object.LastModified)
				continue
			}
			key := object.Key
			tokens := strings.Split(key, "proc-err")
			tokens = strings.Split(tokens[1], "/")
			tokens = strings.Split(tokens[len(tokens)-1], ".")
			tokens = strings.Split(tokens[2], "-")
			var idx int
			if idx, err = strconv.Atoi(tokens[0]); err != nil {
				continue
			}

			job := jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       fmt.Sprintf(`random-%s`, uuid.New()),
				Parameters:   []byte(`{}`),
				CustomVal:    "replay",
				EventPayload: []byte(fmt.Sprintf(`{"location": %q}`, object.Key)),
			}
			objects = append(objects, OrderedJobs{Job: &job, SortIndex: idx})
		}
		if len(objects) >= uploadMaxItems {
			err := storeJobs(ctx, objects, p.dbHandle, p.log)
			if err != nil {
				return err
			}
			objects = nil
		}

	}
	if iter.Err() != nil {
		return fmt.Errorf("failed to iterate proc err files with error: %w", iter.Err())
	}
	if len(objects) != 0 {
		err := storeJobs(ctx, objects, p.dbHandle, p.log)
		if err != nil {
			return err
		}
	}

	p.log.Info("Dumps loader job is done")
	return nil
}
