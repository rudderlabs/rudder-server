package snowpipestreaming

import (
	"context"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

const (
	createChannelAPI = "create_channel"
	deleteChannelAPI = "delete_channel"
	insertAPI        = "insert"
	statusAPI        = "status"
)

func newApiAdapter(
	logger logger.Logger,
	statsFactory stats.Stats,
	api api,
	destination *backendconfig.DestinationT,
) api {
	return &apiAdapter{
		logger:       logger,
		statsFactory: statsFactory,
		api:          api,
		destination:  destination,
	}
}

func (a *apiAdapter) defaultTags(apiName string) stats.Tags {
	return stats.Tags{
		"module":        "batch_router",
		"workspaceId":   a.destination.WorkspaceID,
		"destType":      a.destination.DestinationDefinition.Name,
		"destinationId": a.destination.ID,
		"api":           apiName,
	}
}

func (a *apiAdapter) CreateChannel(ctx context.Context, req *model.CreateChannelRequest) (*model.ChannelResponse, error) {
	log := a.logger.Withn(
		logger.NewStringField("rudderIdentifier", req.RudderIdentifier),
		logger.NewStringField("partition", req.Partition),
		logger.NewStringField("database", req.TableConfig.Database),
		logger.NewStringField("namespace", req.TableConfig.Schema),
		logger.NewStringField("table", req.TableConfig.Table),
	)
	log.Infon("Creating channel")

	tags := a.defaultTags(createChannelAPI)
	defer a.recordDuration(tags)()

	resp, err := a.api.CreateChannel(ctx, req)
	if err != nil {
		tags["success"] = "false"
		return nil, err
	}
	a.logger.Infon("Create channel response",
		logger.NewBoolField("success", resp.Success),
		logger.NewStringField("channelID", resp.ChannelID),
		logger.NewStringField("channelName", resp.ChannelName),
		logger.NewBoolField("valid", resp.Valid),
		logger.NewBoolField("deleted", resp.Deleted),
		logger.NewStringField("error", resp.Error),
		logger.NewStringField("code", resp.Code),
		logger.NewStringField("message", resp.SnowflakeAPIMessage),
	)
	tags["success"] = strconv.FormatBool(resp.Success)
	tags["code"] = resp.Code
	return resp, nil
}

func (a *apiAdapter) DeleteChannel(ctx context.Context, channelID string, sync bool) error {
	a.logger.Infon("Deleting channel",
		logger.NewStringField("channelId", channelID),
		logger.NewBoolField("sync", sync),
	)

	tags := a.defaultTags(deleteChannelAPI)
	defer a.recordDuration(tags)()

	err := a.api.DeleteChannel(ctx, channelID, sync)
	if err != nil {
		tags["success"] = "false"
		return err
	}
	tags["success"] = "true"
	return nil
}

func (a *apiAdapter) Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error) {
	a.logger.Debugn("Inserting data",
		logger.NewStringField("channelId", channelID),
		logger.NewIntField("rows", int64(len(insertRequest.Rows))),
		logger.NewStringField("offset", insertRequest.Offset),
	)

	tags := a.defaultTags(insertAPI)
	defer a.recordDuration(tags)()

	resp, err := a.api.Insert(ctx, channelID, insertRequest)
	if err != nil {
		tags["success"] = "false"
		return nil, err
	}
	tags["success"] = strconv.FormatBool(resp.Success)
	tags["code"] = resp.Code
	return resp, nil
}

func (a *apiAdapter) GetStatus(ctx context.Context, channelID string) (*model.StatusResponse, error) {
	a.logger.Debugn("Getting status",
		logger.NewStringField("channelId", channelID),
	)

	tags := a.defaultTags(statusAPI)
	defer a.recordDuration(tags)()

	resp, err := a.api.GetStatus(ctx, channelID)
	if err != nil {
		tags["success"] = "false"
		return nil, err
	}
	tags["success"] = strconv.FormatBool(resp.Success)
	return resp, nil
}

func (a *apiAdapter) recordDuration(tags stats.Tags) func() {
	return a.statsFactory.NewTaggedStat("snowpipe_streaming_api_response_time", stats.TimerType, tags).RecordDuration()
}
