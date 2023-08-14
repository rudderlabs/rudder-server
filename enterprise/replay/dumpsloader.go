package replay

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// DumpsLoaderHandleT - dumps-loader handle
type dumpsLoaderHandleT struct {
	log           logger.Logger
	dbHandle      *jobsdb.Handle
	prefix        string
	bucket        string
	startAfterKey string
	startTime     time.Time
	endTime       time.Time
	done          bool
	tablePrefix   string
	procError     *ProcErrorRequestHandler
	gwReplay      *GWReplayRequestHandler
	uploader      filemanager.FileManager
}

// ProcErrorRequestHandler is an empty struct to capture Proc Error re-stream request handling functionality
type ProcErrorRequestHandler struct {
	tablePrefix string
	handle      *dumpsLoaderHandleT
}

// GWReplayRequestHandler is an empty struct to capture Gateway replay handling functionality
type GWReplayRequestHandler struct {
	tablePrefix string
	handle      *dumpsLoaderHandleT
}

func getMinMaxCreatedAt(key string) (int64, int64, error) {
	var err error
	var minJobCreatedAt, maxJobCreatedAt int64
	keyTokens := strings.Split(key, "_")
	if len(keyTokens) != 3 {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with _ gave tokens more than 3. Expected 3", key)
	}
	keyTokens = strings.Split(keyTokens[2], ".")
	if len(keyTokens) > 7 {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with . gave tokens more than 7. Expected 6 or 7", keyTokens[2])
	}

	if len(keyTokens) < 6 { // for backward compatibility TODO: remove this check after some time
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with . gave tokens less than 6. Expected 6 or 7", keyTokens[2])
	}
	minJobCreatedAt, err = strconv.ParseInt(keyTokens[3], 10, 64)
	if err != nil {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("ParseInt of %s failed with err: %w", keyTokens[3], err)
	}

	maxJobCreatedAt, err = strconv.ParseInt(keyTokens[4], 10, 64)
	if err != nil {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("ParseInt of %s failed with err: %w", keyTokens[4], err)
	}

	return minJobCreatedAt, maxJobCreatedAt, nil
}

type OrderedJobs struct {
	SortIndex int
	Job       *jobsdb.JobT
}

func storeJobs(ctx context.Context, objects []OrderedJobs, dbHandle *jobsdb.Handle, log logger.Logger) {
	// sorting dumps list on index
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].SortIndex < objects[j].SortIndex
	})

	var jobs []*jobsdb.JobT
	for _, object := range objects {
		jobs = append(jobs, object.Job)
	}

	log.Info("Total dumps count : ", len(objects))
	err := dbHandle.Store(ctx, jobs)
	if err != nil {
		panic(fmt.Errorf("Failed to write dumps locations to DB with error: %w", err))
	}
}

func (gwHandle *GWReplayRequestHandler) fetchDumpsList(ctx context.Context) {
	startTimeMilli := gwHandle.handle.startTime.UnixNano() / int64(time.Millisecond)
	endTimeMilli := gwHandle.handle.endTime.UnixNano() / int64(time.Millisecond)
	var err error
	maxItems := config.GetInt64("MAX_ITEMS", 1000)           // MAX_ITEMS is the max number of files to be fetched in one iteration from object storage
	uploadMaxItems := config.GetInt64("UPLOAD_MAX_ITEMS", 1) // UPLOAD_MAX_ITEMS is the max number of objects to be uploaded to postgres

	gwHandle.handle.log.Info("Fetching gw dump files list")
	objects := make([]OrderedJobs, 0)

	iter := filemanager.IterateFilesWithPrefix(ctx,
		gwHandle.handle.prefix,
		gwHandle.handle.startAfterKey,
		maxItems,
		gwHandle.handle.uploader,
	)
	for iter.Next() {
		object := iter.Get()
		if strings.Contains(object.Key, "gw_jobs_") {
			// Getting rid of migrated dump files (ex: gw_jobs_1_1)
			key := object.Key
			tokens := strings.Split(key, "gw_jobs_")
			tokens = strings.Split(tokens[1], ".")
			var idx int
			if idx, err = strconv.Atoi(tokens[0]); err != nil {
				continue
			}

			// gw dump file name format gw_jobs_<table_index>.<start_job_id>.<end_job_id>.<min_created_at>_<max_created_at>.gz
			// ex: gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
			minJobCreatedAt, maxJobCreatedAt, err := getMinMaxCreatedAt(object.Key)
			var pass bool
			if err == nil {
				pass = maxJobCreatedAt >= startTimeMilli && minJobCreatedAt <= endTimeMilli
			} else {
				gwHandle.handle.log.Infof("gw dump name(%s) is not of the expected format. Parse failed with error %w", object.Key, err)
				gwHandle.handle.log.Info("Falling back to comparing start and end time stamps with gw dump last modified.")
				pass = object.LastModified.After(gwHandle.handle.startTime) && object.LastModified.Before(gwHandle.handle.endTime)
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
		}
		if len(objects) >= int(uploadMaxItems) {
			storeJobs(ctx, objects, gwHandle.handle.dbHandle, gwHandle.handle.log)
			objects = nil
		}
	}
	if iter.Err() != nil {
		panic(fmt.Errorf("Failed to iterate gw dump files with error: %w", iter.Err()))
	}
	if len(objects) != 0 {
		storeJobs(ctx, objects, gwHandle.handle.dbHandle, gwHandle.handle.log)
		objects = nil
	}

	gwHandle.handle.log.Info("Dumps loader job is done")
	gwHandle.handle.done = true
}

func (procHandle *ProcErrorRequestHandler) fetchDumpsList(ctx context.Context) {
	objects := make([]OrderedJobs, 0)
	procHandle.handle.log.Info("Fetching proc err files list")
	var err error
	maxItems := config.GetInt64("MAX_ITEMS", 1000)           // MAX_ITEMS is the max number of files to be fetched in one iteration from object storage
	uploadMaxItems := config.GetInt64("UPLOAD_MAX_ITEMS", 1) // UPLOAD_MAX_ITEMS is the max number of objects to be uploaded to postgres

	iter := filemanager.IterateFilesWithPrefix(ctx,
		procHandle.handle.prefix,
		procHandle.handle.startAfterKey,
		maxItems,
		procHandle.handle.uploader,
	)
	for iter.Next() {
		object := iter.Get()
		if strings.Contains(object.Key, "rudder-proc-err-logs") {
			if object.LastModified.Before(procHandle.handle.startTime) || (object.LastModified.Sub(procHandle.handle.endTime).Hours() > 1) {
				procHandle.handle.log.Debugf("Skipping object: %v ObjectLastModifiedTime: %v", object.Key, object.LastModified)
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
		if len(objects) >= int(uploadMaxItems) {
			storeJobs(ctx, objects, procHandle.handle.dbHandle, procHandle.handle.log)
			objects = nil
		}

	}
	if iter.Err() != nil {
		panic(fmt.Errorf("Failed to iterate proc err files with error: %w", iter.Err()))
	}
	if len(objects) != 0 {
		storeJobs(ctx, objects, procHandle.handle.dbHandle, procHandle.handle.log)
	}

	procHandle.handle.log.Info("Dumps loader job is done")
	procHandle.handle.done = true
}

func (handle *dumpsLoaderHandleT) handleRecovery() {
	// remove dangling executing
	handle.dbHandle.FailExecuting()
}

// Setup sets up dumps-loader.
func (handle *dumpsLoaderHandleT) Setup(ctx context.Context, db *jobsdb.Handle, tablePrefix string, uploader filemanager.FileManager, bucket string, log logger.Logger) {
	var err error
	handle.log = log
	handle.dbHandle = db
	handle.handleRecovery()

	lastJob := handle.dbHandle.GetLastJob()
	handle.startAfterKey = gjson.GetBytes(lastJob.EventPayload, "location").String()
	handle.bucket = bucket
	handle.uploader = uploader
	startTimeStr := strings.TrimSpace(config.GetString("START_TIME", "2000-10-02T15:04:05.000Z"))
	handle.startTime, err = time.Parse(misc.RFC3339Milli, startTimeStr)
	if err != nil {
		panic("invalid start time format provided")
	}
	handle.prefix = strings.TrimSpace(config.GetString("JOBS_REPLAY_BACKUP_PREFIX", ""))
	handle.tablePrefix = tablePrefix
	handle.procError = &ProcErrorRequestHandler{tablePrefix: tablePrefix, handle: handle}
	handle.gwReplay = &GWReplayRequestHandler{tablePrefix: tablePrefix, handle: handle}

	endTimeStr := strings.TrimSpace(config.GetString("END_TIME", ""))
	if endTimeStr == "" {
		handle.endTime = time.Now()
	} else {
		handle.endTime, err = time.Parse(misc.RFC3339Milli, endTimeStr)
		if err != nil {
			panic(fmt.Errorf("invalid END_TIME. Err: %w", err))
		}
	}

	switch tablePrefix {
	case "gw":
		go handle.gwReplay.fetchDumpsList(ctx)
	default:
		go handle.procError.fetchDumpsList(ctx)
	}
}
