package snowpipestreaming

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

type apiAdapter struct {
	stats struct {
		createChannelCount        stats.Counter
		deleteChannelCount        stats.Counter
		insertCount               stats.Counter
		statusCount               stats.Counter
		createChannelResponseTime stats.Timer
		deleteChannelResponseTime stats.Timer
		insertResponseTime        stats.Timer
		statusResponseTime        stats.Timer
	}

	api
}

func newApiAdapter(api api, statsFactory stats.Stats, destination *backendconfig.DestinationT) *apiAdapter {
	adapter := &apiAdapter{}
	adapter.api = api

	tags := stats.Tags{
		"module":        "batch_router",
		"workspaceId":   destination.WorkspaceID,
		"destType":      destination.DestinationDefinition.Name,
		"destinationId": destination.ID,
	}
	adapter.stats.createChannelCount = statsFactory.NewTaggedStat("snowpipestreaming_create_channel_count", stats.CountType, tags)
	adapter.stats.deleteChannelCount = statsFactory.NewTaggedStat("snowpipestreaming_delete_channel_count", stats.CountType, tags)
	adapter.stats.insertCount = statsFactory.NewTaggedStat("snowpipestreaming_insert_count", stats.CountType, tags)
	adapter.stats.statusCount = statsFactory.NewTaggedStat("snowpipestreaming_status_count", stats.CountType, tags)
	adapter.stats.createChannelResponseTime = statsFactory.NewTaggedStat("snowpipestreaming_create_channel_response_time", stats.TimerType, tags)
	adapter.stats.deleteChannelResponseTime = statsFactory.NewTaggedStat("snowpipestreaming_delete_channel_response_time", stats.TimerType, tags)
	adapter.stats.insertResponseTime = statsFactory.NewTaggedStat("snowpipestreaming_insert_response_time", stats.TimerType, tags)
	adapter.stats.statusResponseTime = statsFactory.NewTaggedStat("snowpipestreaming_status_response_time", stats.TimerType, tags)

	return adapter
}

func (a *apiAdapter) CreateChannel(ctx context.Context, req *model.CreateChannelRequest) (*model.ChannelResponse, error) {
	defer a.stats.createChannelCount.Increment()
	defer a.stats.createChannelResponseTime.RecordDuration()()
	return a.api.CreateChannel(ctx, req)
}

func (a *apiAdapter) DeleteChannel(ctx context.Context, channelID string, sync bool) error {
	defer a.stats.deleteChannelCount.Increment()
	defer a.stats.deleteChannelResponseTime.RecordDuration()()
	return a.api.DeleteChannel(ctx, channelID, sync)
}

func (a *apiAdapter) Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error) {
	defer a.stats.insertCount.Increment()
	defer a.stats.insertResponseTime.RecordDuration()()
	return a.api.Insert(ctx, channelID, insertRequest)
}

func (a *apiAdapter) Status(ctx context.Context, channelID string) (*model.StatusResponse, error) {
	defer a.stats.statusCount.Increment()
	defer a.stats.statusResponseTime.RecordDuration()()
	return a.api.Status(ctx, channelID)
}
