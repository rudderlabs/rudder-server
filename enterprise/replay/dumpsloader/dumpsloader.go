package dumpsloader

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type DumpsLoader interface {
	Start()
	Stop()
	IsDone() bool
}

// DumpsLoaderHandleT - dumps-loader handle
type dumpsLoaderHandle struct {
	ctx      context.Context
	cancel   context.CancelFunc
	log      logger.Logger
	dbHandle *jobsdb.Handle
	uploader filemanager.FileManager
	done     bool
	config   dumpsConfig
}

type dumpsConfig struct {
	prefix        string
	bucket        string
	startAfterKey string
	startTime     time.Time
	endTime       time.Time
}

// procErrorRequestHandler is an empty struct to capture Proc Error re-stream request handling functionality
type procErrorRequestHandler struct {
	tablePrefix string
	handle      *dumpsLoaderHandle
}

// gwReplayRequestHandler is an empty struct to capture Gateway replay handling functionality
type gwReplayRequestHandler struct {
	tablePrefix string
	handle      *dumpsLoaderHandle
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
		panic(fmt.Errorf("failed to write dumps locations to DB with error: %w", err))
	}
}

// Setup sets up dumps-loader.
func Setup(ctx context.Context, config *config.Config, db *jobsdb.Handle, tablePrefix string, uploader filemanager.FileManager, bucket string, log logger.Logger) (DumpsLoader, error) {
	ctx, cancel := context.WithCancel(ctx)
	dumpHandler := &dumpsLoaderHandle{
		ctx:      ctx,
		cancel:   cancel,
		log:      log,
		dbHandle: db,
		uploader: uploader,
		config: dumpsConfig{
			prefix:        strings.TrimSpace(config.GetString("JOBS_REPLAY_BACKUP_PREFIX", "")),
			bucket:        bucket,
			startAfterKey: gjson.GetBytes(db.GetLastJob(ctx).EventPayload, "location").String(),
		},
	}
	startTime, endTime, err := utils.GetStartAndEndTime(config)
	if err != nil {
		return nil, err
	}
	dumpHandler.config.startTime = startTime
	dumpHandler.config.endTime = endTime
	switch tablePrefix {
	case "gw":
		return &gwReplayRequestHandler{tablePrefix: tablePrefix, handle: dumpHandler}, nil
	default:
		return &procErrorRequestHandler{tablePrefix: tablePrefix, handle: dumpHandler}, nil
	}
}
