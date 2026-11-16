package jobsdb

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type pipeline struct {
	applied atomic.Int64 // the watermark persisted last, which reads skip below

	mu        sync.RWMutex      // protects the fields below
	watermark int64             // every job of the pipeline below this id is in a terminal state
	pending   *roaring64.Bitmap // at least the jobs of the pipeline with a non-terminal status and no terminal one yet
}

func (p *pipeline) addPending(jobIDs []int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, jobID := range jobIDs {
		p.pending.Add(uint64(jobID))
	}
}

func (p *pipeline) removePending(jobIDs []int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, jobID := range jobIDs {
		p.pending.Remove(uint64(jobID))
	}
}

// advance moves the watermark up to jobID, but not past the lowest pending job.
func (p *pipeline) advance(jobID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.pending.IsEmpty() {
		jobID = min(jobID, int64(p.pending.Minimum()))
	}
	p.watermark = max(p.watermark, jobID)
}

func (p *pipeline) getWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.watermark
}

// filterType is the job attribute that a pipeline filters on. It is part of the persisted keys.
type filterType string

const (
	noneFilterType        filterType = "none"
	workspaceFilterType   filterType = "workspace"
	sourceFilterType      filterType = "source"
	destinationFilterType filterType = "destination"
)

const pipelineKeySeparator = "|"

// pipelineKey is the filters of a pipeline.
type pipelineKey struct {
	CustomVal   string     // the custom value of the jobs, if the pipeline filters on one
	FilterType  filterType // none, workspace, source or destination
	FilterValue string     // the workspace, source or destination id, empty for none
	Consumer    string     // multi-consumer handles only
}

// String returns the key as persisted.
func (k pipelineKey) String() string {
	return strings.Join([]string{k.CustomVal, string(k.FilterType), k.FilterValue, k.Consumer}, pipelineKeySeparator)
}

// parsePipelineKey parses a key returned by [pipelineKey.String]. A key whose filters contain the separator
// has more fields, so it does not parse.
func parsePipelineKey(s string) (pipelineKey, bool) {
	fields := strings.Split(s, pipelineKeySeparator)
	if len(fields) != 4 {
		return pipelineKey{}, false
	}
	return pipelineKey{CustomVal: fields[0], FilterType: filterType(fields[1]), FilterValue: fields[2], Consumer: fields[3]}, true
}

// pipelineKeyShape is the filters of a pipeline key, without their values.
type pipelineKeyShape struct {
	CustomVal  bool
	FilterType filterType
}
