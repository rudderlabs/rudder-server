package jobsdb

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// NewReadCursor decorates jobsDB, which must be h or a decorator of it, with a watermark per pipeline (the
// filters of a read) below which every job of the pipeline is terminal. Reads of non-terminal states skip the
// sealed datasets below the watermark persisted last.
//
// A read that includes unprocessed moves the watermark up to the lowest of:
//  1. the first job it returns;
//  2. the first job id of the live dataset when the read started, since stores commit out of job id order,
//     but only into the live dataset;
//  3. the lowest pending job of the pipeline.
//
// This is safe provided that:
//   - every status write goes through the returned JobsDB, with the custom value, workspace and parameters of
//     its job;
//   - the executing jobs of a previous run are recovered before the first read;
//   - the other non-terminal states that a previous run can leave behind are read together with unprocessed,
//     except importing, which the batch router checks before every read of the rest.
//
// Read-excluded partitions need nothing: none of their jobs is pending here once the exclusion is removed. A
// status or a read that the pipelines cannot account for disables the decorator, see [readCursor.disable].
func NewReadCursor(jobsDB JobsDB, h *Handle) JobsDB {
	rc := &readCursor{
		JobsDB:    jobsDB,
		h:         h,
		log:       h.logger.Child("readCursor"),
		pipelines: make(map[pipelineKey]*pipeline),
	}
	rc.conf.persistInterval = h.config.GetReloadableDurationVar(1, time.Minute, h.configKeys("readCursor.persistInterval")...)
	rc.conf.persistMinSkippedDatasets = h.config.GetReloadableIntVar(4, 1, h.configKeys("readCursor.persistMinSkippedDatasets")...)
	rc.disabledStat = h.stats.NewTaggedStat("jobsdb_read_cursor_disabled", stats.GaugeType, stats.Tags{"tablePrefix": h.tablePrefix})
	rc.disabledStat.Gauge(0)
	return rc
}

// readCursor is the decorator returned by [NewReadCursor].
type readCursor struct {
	JobsDB         // the decorated jobsdb, which every call goes through
	h      *Handle // the handle under JobsDB, for its dataset list, configuration and database
	log    logger.Logger
	conf   struct {
		persistInterval           config.ValueLoader[time.Duration] // how often the watermarks are persisted
		persistMinSkippedDatasets config.ValueLoader[int]           // see [readCursor.persist]
	}
	disabled     atomic.Bool       // see [readCursor.disable]
	disabledStat stats.Measurement // 1 once disabled

	pipelinesMu sync.RWMutex              // protects the fields below
	pipelines   map[pipelineKey]*pipeline // the pipelines of reads and of the persisted watermarks
	shapes      []pipelineKeyShape        // the shapes of the pipelines' keys, for finding the pipelines of a status

	lifecycle struct { // the persist loop, run between Start and Stop
		mu      sync.Mutex
		started bool
		cancel  context.CancelFunc
		group   *errgroup.Group
	}
}

// disable makes the decorator pass every call through until a restart, since its watermarks can no longer
// account for every pending job: a status without the custom value, workspace or parameters of its job (or the
// source or destination that a pipeline filters on), or a read that filters on more than a pipeline key holds.
// It logs a warning, sets the jobsdb_read_cursor_disabled gauge, and the next persist truncates the persisted
// watermarks and ends the persist loop.
func (rc *readCursor) disable(err error) {
	if rc.disabled.CompareAndSwap(false, true) {
		rc.log.Warnn("Disabling read cursors", obskit.Error(err))
		rc.disabledStat.Gauge(1)
	}
}

// getPipeline returns the pipeline of a key and creates one if missing
func (rc *readCursor) getPipeline(key pipelineKey) *pipeline {
	rc.pipelinesMu.RLock()
	p, ok := rc.pipelines[key]
	rc.pipelinesMu.RUnlock()
	if ok {
		return p
	}
	rc.pipelinesMu.Lock()
	defer rc.pipelinesMu.Unlock()
	if p, ok := rc.pipelines[key]; ok {
		return p
	}
	p = &pipeline{pending: roaring64.New()}
	rc.pipelines[key] = p
	if shape := (pipelineKeyShape{CustomVal: key.CustomVal != "", FilterType: key.FilterType}); !slices.Contains(rc.shapes, shape) {
		rc.shapes = append(rc.shapes, shape)
	}
	return p
}
