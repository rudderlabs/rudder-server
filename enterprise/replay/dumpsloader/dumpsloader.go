package dumpsloader

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type DumpsLoader interface {
	Start()
	Stop() error
}

// DumpsLoaderHandleT - dumps-loader handle
type dumpsLoader struct {
	ctx      context.Context
	cancel   context.CancelFunc
	log      logger.Logger
	dbHandle *jobsdb.Handle
	uploader filemanager.FileManager
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
	g           errgroup.Group
	tablePrefix string
	*dumpsLoader
}

// gwReplayRequestHandler is an empty struct to capture Gateway replay handling functionality
type gwReplayRequestHandler struct {
	g           errgroup.Group
	tablePrefix string
	*dumpsLoader
}

type OrderedJobs struct {
	SortIndex int
	Job       *jobsdb.JobT
}

func storeJobs(ctx context.Context, objects []OrderedJobs, dbHandle *jobsdb.Handle, log logger.Logger) error {
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
		return fmt.Errorf("failed to write dumps locations to DB with error: %w", err)
	}
	return nil
}

// Setup sets up dumps-loader.
func Setup(ctx context.Context, config *config.Config, db *jobsdb.Handle, tablePrefix string, uploader filemanager.FileManager, bucket string, log logger.Logger) (DumpsLoader, error) {
	ctx, cancel := context.WithCancel(ctx)
	dumpHandler := &dumpsLoader{
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
	g := errgroup.Group{}
	switch tablePrefix {
	case "gw":
		return &gwReplayRequestHandler{g, tablePrefix, dumpHandler}, nil
	default:
		return &procErrorRequestHandler{g, tablePrefix, dumpHandler}, nil
	}
}
