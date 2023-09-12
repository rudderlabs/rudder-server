package dumpsloader

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func (g *gwReplayRequestHandler) Start() {
	g.g = errgroup.Group{}
	g.handleRecovery()
	g.g.Go(func() error {
		return g.fetchDumpsList(g.handle.ctx)
	})
}

func (g *gwReplayRequestHandler) Stop() error {
	g.handle.cancel()
	return g.g.Wait()
}

func (g *gwReplayRequestHandler) IsDone() bool {
	return g.handle.done
}

func (g *gwReplayRequestHandler) handleRecovery() {
	// remove dangling executing
	g.handle.dbHandle.FailExecuting()
}

func (g *gwReplayRequestHandler) fetchDumpsList(ctx context.Context) error {
	maxItems := config.GetInt64("MAX_ITEMS", 1000)         // MAX_ITEMS is the max number of files to be fetched in one iteration from object storage
	uploadMaxItems := config.GetInt("UPLOAD_MAX_ITEMS", 1) // UPLOAD_MAX_ITEMS is the max number of objects to be uploaded to postgres

	g.handle.log.Info("Fetching gw dump files list")
	objects := make([]OrderedJobs, 0)
	iter := filemanager.IterateFilesWithPrefix(ctx,
		g.handle.config.prefix,
		g.handle.config.startAfterKey,
		maxItems,
		g.handle.uploader,
	)
	for iter.Next() {
		object := iter.Get()
		filePath := object.Key
		if strings.Contains(filePath, "gw_jobs_") {
			startTimeMilli := g.handle.config.startTime.UnixNano() / int64(time.Millisecond)
			endTimeMilli := g.handle.config.endTime.UnixNano() / int64(time.Millisecond)
			key := object.Key
			tokens := strings.Split(key, "gw_jobs_")
			tokens = strings.Split(tokens[1], ".")
			var idx int
			var err error
			if idx, err = strconv.Atoi(tokens[0]); err != nil {
				continue
			}

			// gw dump file name format gw_jobs_<table_index>.<start_job_id>.<end_job_id>.<min_created_at>_<max_created_at>.gz
			// ex: gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
			minJobCreatedAt, maxJobCreatedAt, err := utils.GetMinMaxCreatedAt(object.Key)
			var pass bool
			if err == nil {
				pass = maxJobCreatedAt >= startTimeMilli && minJobCreatedAt <= endTimeMilli
			} else {
				g.handle.log.Infof("gw dump name(%s) is not of the expected format. Parse failed with error %w", object.Key, err)
				g.handle.log.Info("Falling back to comparing start and end time stamps with gw dump last modified.")
				pass = object.LastModified.After(g.handle.config.startTime) && object.LastModified.Before(g.handle.config.endTime)
			}
			if pass {
				job := jobsdb.JobT{
					UUID:         uuid.New(),
					UserID:       fmt.Sprintf(`random-%s`, uuid.New()),
					Parameters:   []byte(`{}`),
					CustomVal:    "replay",
					EventPayload: []byte(fmt.Sprintf(`{"location": %q}`, object.Key)),
				}
				objects = append(objects, OrderedJobs{Job: &job, SortIndex: idx})
			}
		} else {
			startTimeMilli := g.handle.config.startTime.Unix()
			fileName := strings.Split(filePath, "/")[len(strings.Split(filePath, "/"))-1]
			firstEventAt, err := strconv.ParseInt(strings.Split(fileName, "_")[0], 10, 64)
			if err != nil {
				g.handle.log.Info("Failed to parse firstEventAt from file name: ", fileName)
				continue
			}
			if firstEventAt >= startTimeMilli {
				job := jobsdb.JobT{
					UUID:         uuid.New(),
					UserID:       fmt.Sprintf(`random-%s`, uuid.New()),
					Parameters:   []byte(`{}`),
					CustomVal:    "replay",
					EventPayload: []byte(fmt.Sprintf(`{"location": %q}`, object.Key)),
				}
				objects = append(objects, OrderedJobs{Job: &job, SortIndex: int(firstEventAt)})
			}
		}

		if len(objects) >= uploadMaxItems {
			err := storeJobs(ctx, objects, g.handle.dbHandle, g.handle.log)
			if err != nil {
				return err
			}
			objects = nil
		}
	}

	if iter.Err() != nil {
		return fmt.Errorf("failed to iterate gw dump files with error: %w", iter.Err())
	}
	if len(objects) != 0 {
		err := storeJobs(ctx, objects, g.handle.dbHandle, g.handle.log)
		if err != nil {
			return err
		}
	}

	g.handle.log.Info("Dumps loader job is done")
	g.handle.done = true
	return nil
}
