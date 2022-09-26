package replay

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"

	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

func init() {
	pkgLogger = logger.NewLogger().Child("enterprise").Child("replay").Child("dumpsLoader")
}

// DumpsLoaderHandleT - dumps-loader handle
type dumpsLoaderHandleT struct {
	dbHandle      *jobsdb.HandleT
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
	if len(keyTokens) != 6 {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with . gave tokens more than 6. Expected 6", keyTokens[2])
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

type dbObjectT struct {
	SortIndex int
	Job       *jobsdb.JobT
}

func (gwHandle *GWReplayRequestHandler) fetchDumpsList(ctx context.Context) {
	startTimeMilli := gwHandle.handle.startTime.UnixNano() / int64(time.Millisecond)
	endTimeMilli := gwHandle.handle.endTime.UnixNano() / int64(time.Millisecond)

	for {
		pkgLogger.Info("fetching files list")
		objects := make([]dbObjectT, 0)
		s3Objects, err := gwHandle.handle.uploader.ListFilesWithPrefix(ctx, gwHandle.handle.startAfterKey, gwHandle.handle.prefix, 1000)
		if err != nil {
			panic(fmt.Errorf("failed to fetch File names with error:%w", err))
		}

		if len(s3Objects) == 0 {
			pkgLogger.Infof("no files found in %s", gwHandle.handle.prefix)
			break
		}

		pkgLogger.Infof(`Fetched files list from %v (lastModifiedAt: %v) to %v (lastModifiedAt: %v)`, s3Objects[0].Key, s3Objects[0].LastModified, s3Objects[len(s3Objects)-1].Key, s3Objects[len(s3Objects)-1].LastModified)

		for _, object := range s3Objects {
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
					pkgLogger.Infof("gw dump name(%s) is not of the expected format. Parse failed with error %w", object.Key, err)
					pkgLogger.Info("falling back to comparing start and end time stamps with gw dump last modified.")
					pass = object.LastModified.After(gwHandle.handle.startTime) && object.LastModified.Before(gwHandle.handle.endTime)
				}

				if pass {
					job := jobsdb.JobT{
						UUID:         uuid.Must(uuid.NewV4()),
						UserID:       fmt.Sprintf(`random-%s`, uuid.Must(uuid.NewV4())),
						Parameters:   []byte(`{}`),
						CustomVal:    "replay",
						EventPayload: []byte(fmt.Sprintf(`{"location": "%s"}`, object.Key)),
					}
					objects = append(objects, dbObjectT{Job: &job, SortIndex: idx})
				}
			}
		}

		// sorting dumps list on index
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].SortIndex < objects[j].SortIndex
		})

		var jobs []*jobsdb.JobT
		for _, object := range objects {
			jobs = append(jobs, object.Job)
		}

		pkgLogger.Info("Total gw_dumps count from S3 : ", len(objects))
		err = gwHandle.handle.dbHandle.Store(ctx, jobs)
		if err != nil {
			panic(fmt.Errorf("failed to write gw_dumps locations to DB with error: %w", err))
		}
	}

	pkgLogger.Info("Dumps loader job is done")
	gwHandle.handle.done = true
}

func (procHandle *ProcErrorRequestHandler) fetchDumpsList(ctx context.Context) {
	for {
		objects := make([]dbObjectT, 0)
		pkgLogger.Info("fetching files list")
		s3Objects, err := procHandle.handle.uploader.ListFilesWithPrefix(ctx, procHandle.handle.startAfterKey, procHandle.handle.prefix, 1000)
		if err != nil {
			panic(fmt.Errorf("failed to fetch File names with error: %w", err))
		}

		if len(s3Objects) == 0 {
			break
		}

		pkgLogger.Infof(`Fetched files list from %v (lastModifiedAt: %v) to %v (lastModifiedAt: %v)`, s3Objects[0].Key, s3Objects[0].LastModified, s3Objects[len(s3Objects)-1].Key, s3Objects[len(s3Objects)-1].LastModified)

		for _, object := range s3Objects {
			if strings.Contains(object.Key, "rudder-proc-err-logs") {
				if object.LastModified.Before(procHandle.handle.startTime) || (object.LastModified.Sub(procHandle.handle.endTime).Hours() > 1) {
					pkgLogger.Debugf("skipping S3 object: %v ObjectLastModifiedTime: %v", object.Key, object.LastModified)
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
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       fmt.Sprintf(`random-%s`, uuid.Must(uuid.NewV4())),
					Parameters:   []byte(`{}`),
					CustomVal:    "replay",
					EventPayload: []byte(fmt.Sprintf(`{"location": "%s"}`, object.Key)),
				}
				objects = append(objects, dbObjectT{Job: &job, SortIndex: idx})
			}
		}

		// sorting dumps list on index
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].SortIndex < objects[j].SortIndex
		})

		var jobs []*jobsdb.JobT
		for _, object := range objects {
			jobs = append(jobs, object.Job)
		}

		pkgLogger.Info("Total proc_error_dumps count from S3 : ", len(objects))
		if len(jobs) > 0 {
			err = procHandle.handle.dbHandle.Store(ctx, jobs)
			if err != nil {
				panic(fmt.Errorf("failed to write proc_error_dumps locations to DB with error: %w", err))
			}
		}
	}

	pkgLogger.Info("Dumps loader job is done")
	procHandle.handle.done = true
}

func (handle *dumpsLoaderHandleT) handleRecovery() {
	// remove dangling executing
	handle.dbHandle.DeleteExecuting()
}

// Setup sets up dumps-loader.
func (handle *dumpsLoaderHandleT) Setup(ctx context.Context, db *jobsdb.HandleT, tablePrefix string, uploader filemanager.FileManager, bucket string) {
	var err error
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
	handle.prefix = strings.TrimSpace(config.GetString("JOBS_BACKUP_PREFIX", ""))
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
