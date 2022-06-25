package replay

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/enterprise/replay/fileuploader"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

func init() {
	pkgLogger = logger.NewLogger().Child("enterprise").Child("replay").Child("dumpsLoader")
}

// DumpsLoaderHandleT - dumpsloader handle
type DumpsLoaderHandleT struct {
	DbHandle      *jobsdb.HandleT
	prefix        string
	bucket        string
	startAfterKey string
	startTime     time.Time
	endTime       time.Time
	done          bool
	tablePrefix   string
	procError     *ProcErrorRequestHandler
	gwReplay      *GWReplayRequestHandler
}

// ProcErrorRequestHandler is an empty struct to capture Proc Error Restream request handling functionality
type ProcErrorRequestHandler struct {
	tablePrefix string
}

// GWReplayRequestHandler is an empty struct to capture Gateway replay  handling functionality
type GWReplayRequestHandler struct {
	tablePrefix string
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

func (handle *GWReplayRequestHandler) fetchDumpsList(handleDump *DumpsLoaderHandleT) {
	var continuationToken *string
	startTimeMilli := handleDump.startTime.UnixNano() / int64(time.Millisecond)
	endTimeMilli := handleDump.endTime.UnixNano() / int64(time.Millisecond)

	for {
		pkgLogger.Info("fetching files list")
		objects := make([]dbObjectT, 0)
		s3Objects, err := fileuploader.ListFilesWithPrefix(handleDump.prefix, handleDump.bucket, handleDump.startAfterKey, continuationToken)
		if err != nil {
			panic(fmt.Errorf("Failed to fetch File names.%w", err))
		}

		if len(s3Objects.Objects) == 0 {
			break
		}

		pkgLogger.Infof(`Fetched files list from %v (lastModifiedAt: %v) to %v (lastModifiedAt: %v)`, s3Objects.Objects[0].Key, s3Objects.Objects[0].LastModifiedTime, s3Objects.Objects[len(s3Objects.Objects)-1].Key, s3Objects.Objects[len(s3Objects.Objects)-1].LastModifiedTime)

		for _, object := range s3Objects.Objects {
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
					pass = object.LastModifiedTime.After(handleDump.startTime) && object.LastModifiedTime.Before(handleDump.endTime)
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

		continuationToken = s3Objects.ContinuationToken

		// sorting dumps list on index
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].SortIndex < objects[j].SortIndex
		})

		var jobs []*jobsdb.JobT
		for _, object := range objects {
			jobs = append(jobs, object.Job)
		}

		pkgLogger.Info("Total gw_dumps count from S3 : ", len(objects))
		err = handleDump.DbHandle.Store(jobs)
		if err != nil {
			panic(fmt.Errorf("Failed to write gw_dumps locations to DB with error: %w", err))
		}

		if !s3Objects.Truncated {
			break
		}
	}

	pkgLogger.Info("Dumps loader job is done")
	handleDump.done = true
}

func (handle *ProcErrorRequestHandler) fetchDumpsList(handleDump *DumpsLoaderHandleT) {
	var continuationToken *string
	for {
		objects := make([]dbObjectT, 0)
		pkgLogger.Info("fetching files list")
		s3Objects, err := fileuploader.ListFilesWithPrefix(handleDump.prefix, handleDump.bucket, handleDump.startAfterKey, continuationToken)
		if err != nil {
			panic(fmt.Errorf("Failed to fetch File names.%w", err))
		}

		if len(s3Objects.Objects) == 0 {
			break
		}

		pkgLogger.Infof(`Fetched files list from %v (lastModifiedAt: %v) to %v (lastModifiedAt: %v)`, s3Objects.Objects[0].Key, s3Objects.Objects[0].LastModifiedTime, s3Objects.Objects[len(s3Objects.Objects)-1].Key, s3Objects.Objects[len(s3Objects.Objects)-1].LastModifiedTime)

		for _, object := range s3Objects.Objects {
			if strings.Contains(object.Key, "rudder-proc-err-logs") {
				if object.LastModifiedTime.Before(handleDump.startTime) || (object.LastModifiedTime.Sub(handleDump.endTime).Hours() > 1) {
					pkgLogger.Debugf("skipping S3 object: %v ObjectLastModifiedTime: %v", object.Key, object.LastModifiedTime)
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

		continuationToken = s3Objects.ContinuationToken

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
			err = handleDump.DbHandle.Store(jobs)
			if err != nil {
				panic(fmt.Errorf("Failed to write proc_error_dumps locations to DB with error: %w", err))
			}
		}

		if !s3Objects.Truncated {
			break
		}
	}

	pkgLogger.Info("Dumps loader job is done")
	handleDump.done = true
}

func (handle *DumpsLoaderHandleT) handleRecovery() {
	// remove dangling executing
	handle.DbHandle.DeleteExecuting()
}

// SetUp sets up dumpsloader.
func (handle *DumpsLoaderHandleT) SetUp(db *jobsdb.HandleT, tablePrefix string) {
	var err error
	handle.DbHandle = db
	handle.handleRecovery()

	lastJob := handle.DbHandle.GetLastJob()
	handle.startAfterKey = gjson.GetBytes(lastJob.EventPayload, "location").String()

	handle.bucket = strings.TrimSpace(config.GetEnv("S3_DUMPS_BUCKET", ""))
	if handle.bucket == "" {
		panic("Bucket is not configured.")
	}

	handle.prefix = strings.TrimSpace(config.GetEnv("S3_DUMPS_BUCKET_PREFIX", ""))
	handle.tablePrefix = tablePrefix
	handle.procError = &ProcErrorRequestHandler{tablePrefix: tablePrefix}
	handle.gwReplay = &GWReplayRequestHandler{tablePrefix: tablePrefix}
	startTimeStr := strings.TrimSpace(config.GetEnv("START_TIME", "2000-10-02T15:04:05.000Z"))
	handle.startTime, err = time.Parse(misc.RFC3339Milli, startTimeStr)
	if err != nil {
		panic(fmt.Errorf("invalid START_TIME. Err: %w", err))
	}

	endTimeStr := strings.TrimSpace(config.GetEnv("END_TIME", ""))
	if endTimeStr == "" {
		handle.endTime = time.Now()
	} else {
		handle.endTime, err = time.Parse(misc.RFC3339Milli, endTimeStr)
		if err != nil {
			panic(fmt.Errorf("invalid END_TIME. Err: %w", err))
		}
	}

	if tablePrefix == "gw" {
		go handle.gwReplay.fetchDumpsList(handle)
	} else {
		go handle.procError.fetchDumpsList(handle)
	}
}
