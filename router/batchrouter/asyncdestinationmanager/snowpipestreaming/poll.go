package snowpipestreaming

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stringify"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (m *Manager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	m.logger.Infon("Polling started", logger.NewStringField("importId", pollInput.ImportId))

	var uploadInfos []uploadInfo
	err := json.Unmarshal([]byte(pollInput.ImportId), &uploadInfos)
	if err != nil {
		return common.PollStatusResponse{
			InProgress: false,
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Sprintf("failed to unmarshal import id: %v", err),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(m.config.maxConcurrentPollWorkers.Load())

	for i, info := range uploadInfos {
		g.Go(func() error {
			if err := m.pollUploadInfo(ctx, info); err != nil {
				uploadInfos[i].Failed = true
				uploadInfos[i].Reason = err.Error()
				m.logger.Warnn("Failed to poll channel offset",
					logger.NewStringField("channelId", info.ChannelID),
					logger.NewStringField("offset", info.Offset),
					logger.NewStringField("table", info.Table),
					obskit.Error(err),
				)
			}
			return nil
		})
	}
	_ = g.Wait()

	if err := g.Wait(); err != nil {
		return common.PollStatusResponse{
			InProgress:    false,
			StatusCode:    http.StatusOK,
			Complete:      true,
			HasFailed:     true,
			FailedJobURLs: stringify.Any(uploadInfos),
		}
	}

	return common.PollStatusResponse{
		InProgress: false,
		StatusCode: http.StatusOK,
		Complete:   true,
		HasFailed:  false,
		HasWarning: false,
	}
}

func (m *Manager) pollUploadInfo(ctx context.Context, info uploadInfo) error {
	log := m.logger.Withn(
		logger.NewStringField("channelId", info.ChannelID),
		logger.NewStringField("offset", info.Offset),
		logger.NewStringField("table", info.Table),
	)
	log.Infon("Polling for channel")

	for {
		statusRes, err := m.api.Status(ctx, info.ChannelID)
		if err != nil {
			return fmt.Errorf("getting status: %v", err)
		}
		if !statusRes.Valid || !statusRes.Success {
			return errInvalidStatusResponse
		}
		if statusRes.Offset == info.Offset {
			log.Infon("Polling completed")
			return nil
		}
		log.Infon("Polling in progress. Sleeping before next poll.",
			logger.NewStringField("statusOffset", statusRes.Offset),
			logger.NewBoolField("statusSuccess", statusRes.Success),
			logger.NewBoolField("statusValid", statusRes.Valid),
			logger.NewDurationField("pollFrequency", m.config.pollFrequency),
		)
		time.Sleep(m.config.pollFrequency)
	}
}
