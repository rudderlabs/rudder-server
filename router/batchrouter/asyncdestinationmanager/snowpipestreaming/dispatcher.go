package snowpipestreaming

import (
	"context"
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// asyncManager is what batchrouter calls; both the classic [Manager] and [ManagerV2] implement it.
type asyncManager = common.AsyncDestinationManager

// Dispatcher sends each call to the classic or the high-performance manager. It has no logic of its own:
//   - Upload follows the destination's high-performance flag, read once per upload.
//   - Poll and GetUploadStats follow the version recorded on the imports, so that an import always finishes on the
//     manager that started it, even if the flag flips in between.
//
// batchrouter doesn't fetch a new batch while an import is in flight, so a flag flip always lands between batches.
type Dispatcher struct {
	v1     asyncManager
	v2     asyncManager
	useHP  config.ValueLoader[bool]
	logger logger.Logger
}

// NewManager returns the [Dispatcher] for a Snowpipe Streaming destination.
//
// SnowpipeStreaming.highPerformance.enabled selects the high-performance path, and can be set per destination
// (SnowpipeStreaming.<destinationID>.highPerformance.enabled) or per workspace (SnowpipeStreaming.<workspaceID>.highPerformance.enabled).
func NewManager(conf *config.Config, log logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) common.AsyncDestinationManager {
	v1 := New(conf, log, statsFactory, destination)
	return &Dispatcher{
		v1: v1,
		v2: NewV2(conf, statsFactory, destination, v1),
		useHP: conf.GetReloadableBoolVar(false,
			"SnowpipeStreaming."+destination.ID+".highPerformance.enabled",
			"SnowpipeStreaming."+destination.WorkspaceID+".highPerformance.enabled",
			"SnowpipeStreaming.highPerformance.enabled",
		),
		logger: v1.logger,
	}
}

func (d *Dispatcher) Transform(job *jobsdb.JobT) (string, error) {
	return d.v1.Transform(job)
}

func (d *Dispatcher) Upload(ctx context.Context, asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	if d.useHP.Load() {
		return d.v2.Upload(ctx, asyncDest)
	}
	return d.v1.Upload(ctx, asyncDest)
}

func (d *Dispatcher) Poll(ctx context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
	version, err := importVersion(pollInput.ImportId)
	if err != nil {
		d.logger.Errorn("Refusing to poll imports of mixed versions", logger.NewStringField("importID", pollInput.ImportId), obskit.Error(err))
		return common.PollStatusResponse{StatusCode: http.StatusBadRequest, Complete: true, HasFailed: true, Error: err.Error()}
	}
	if version == versionHP {
		return d.v2.Poll(ctx, pollInput)
	}
	return d.v1.Poll(ctx, pollInput)
}

func (d *Dispatcher) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	version, err := importVersion(input.FailedJobParameters)
	if err != nil {
		return common.GetUploadStatsResponse{StatusCode: http.StatusBadRequest, Error: err.Error()}
	}
	if version == versionHP {
		return d.v2.GetUploadStats(input)
	}
	return d.v1.GetUploadStats(input)
}

// importVersion returns the version of a batch's imports. Imports without a version are classic ones.
// A batch is uploaded by one manager, so mixed versions mean corrupted state and are refused rather than guessed.
// Input that can't be parsed is left to the classic manager, which reports it.
func importVersion(importInfos string) (int, error) {
	if !gjson.Valid(importInfos) {
		return 0, nil
	}
	infos := gjson.Parse(importInfos).Array()
	if len(infos) == 0 {
		return 0, nil
	}
	version := infos[0].Get("version").Int() // 0 when absent
	for _, info := range infos[1:] {
		if v := info.Get("version").Int(); v != version {
			return 0, fmt.Errorf("imports of mixed versions: %d and %d", version, v)
		}
	}
	return int(version), nil
}
