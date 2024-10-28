package snowpipestreaming

import (
	"context"
	"fmt"
	"net/http"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stringify"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// Poll checks the status of multiple imports using the import ID from pollInput.
// It returns a PollStatusResponse indicating if any imports are still in progress or if any have failed or succeeded.
// If any imports have failed, it deletes the channels for those imports.
func (m *Manager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	m.logger.Infon("Polling started")

	var infos []*importInfo
	err := json.Unmarshal([]byte(pollInput.ImportId), &infos)
	if err != nil {
		return common.PollStatusResponse{
			InProgress: false,
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Errorf("failed to unmarshal import id: %w", err).Error(),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var anyoneInProgress bool
	for _, info := range infos {
		inProgress, err := m.pollForImportInfo(ctx, info)
		if err != nil {
			info.Failed = true
			info.Reason = err.Error()
			continue
		}
		anyoneInProgress = anyoneInProgress || inProgress
	}
	if anyoneInProgress {
		return common.PollStatusResponse{InProgress: true}
	}

	failedInfos := lo.Filter(infos, func(info *importInfo, index int) bool {
		return info.Failed
	})
	for _, info := range failedInfos {
		m.logger.Warnn("Failed to poll channel offset",
			logger.NewStringField("channelId", info.ChannelID),
			logger.NewStringField("offset", info.Offset),
			logger.NewStringField("table", info.Table),
			logger.NewStringField("reason", info.Reason),
		)

		if deleteErr := m.deleteChannel(ctx, info.Table, info.ChannelID); deleteErr != nil {
			m.logger.Warnn("Failed to delete channel",
				logger.NewStringField("channelId", info.ChannelID),
				logger.NewStringField("table", info.Table),
				obskit.Error(deleteErr),
			)
		}
	}

	var successJobsCount, failedJobsCount int
	for _, info := range infos {
		if info.Failed {
			failedJobsCount += info.Count
		} else {
			successJobsCount += info.Count
		}
	}
	m.stats.jobs.failed.Count(failedJobsCount)
	m.stats.jobs.succeeded.Count(successJobsCount)

	statusResponse := common.PollStatusResponse{
		InProgress: false,
		StatusCode: http.StatusOK,
		Complete:   true,
	}
	if len(failedInfos) > 0 {
		statusResponse.HasFailed = true
		statusResponse.FailedJobURLs = stringify.Any(infos)
	} else {
		statusResponse.HasFailed = false
		statusResponse.HasWarning = false
	}
	return statusResponse
}

func (m *Manager) pollForImportInfo(ctx context.Context, info *importInfo) (bool, error) {
	log := m.logger.Withn(
		logger.NewStringField("channelId", info.ChannelID),
		logger.NewStringField("offset", info.Offset),
		logger.NewStringField("table", info.Table),
	)
	log.Infon("Polling for import info")

	statusRes, err := m.api.Status(ctx, info.ChannelID)
	if err != nil {
		return false, fmt.Errorf("getting status: %w", err)
	}
	log.Infon("Polled import info",
		logger.NewBoolField("success", statusRes.Success),
		logger.NewStringField("polledOffset", statusRes.Offset),
		logger.NewBoolField("valid", statusRes.Valid),
		logger.NewBoolField("completed", statusRes.Offset == info.Offset),
	)
	if !statusRes.Valid || !statusRes.Success {
		return false, errInvalidStatusResponse
	}
	return statusRes.Offset != info.Offset, nil
}
