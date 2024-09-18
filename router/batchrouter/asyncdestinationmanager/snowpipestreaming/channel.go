package snowpipestreaming

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	internalapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) createChannel(
	ctx context.Context,
	asyncDest *common.AsyncDestinationStruct,
	destConf destConfig,
	tableName string,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	if response, ok := m.channelCache.Load(tableName); ok {
		return response.(*model.ChannelResponse), nil
	}

	req := &model.CreateChannelRequest{
		RudderIdentifier: asyncDest.Destination.ID,
		Partition:        m.config.instanceID,
		AccountConfig: model.AccountConfig{
			Account:              destConf.Account,
			User:                 destConf.User,
			Role:                 destConf.Role,
			PrivateKey:           whutils.FormatPemContent(destConf.PrivateKey),
			PrivateKeyPassphrase: destConf.PrivateKeyPassphrase,
		},
		TableConfig: model.TableConfig{
			Database: destConf.Database,
			Schema:   destConf.Namespace,
			Table:    tableName,
		},
	}

	resp, err := m.api.CreateChannel(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating channel: %v", err)
	}
	if resp.Success {
		m.channelCache.Store(tableName, resp)
		return resp, nil
	}

	switch resp.Code {
	case internalapi.ErrSchemaDoesNotExistOrNotAuthorized:
		resp, err = m.handleSchemaError(ctx, req, eventSchema)
		if err != nil {
			return nil, fmt.Errorf("handling schema error: %v", err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("creating channel for schema error: %s", resp.Error)
		}
		m.channelCache.Store(tableName, resp)
		return resp, nil
	case internalapi.ErrTableDoesNotExistOrNotAuthorized:
		resp, err = m.handleTableError(ctx, req, eventSchema)
		if err != nil {
			return nil, fmt.Errorf("handling table error: %v", err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("creating channel for table error: %s", resp.Error)
		}
		m.channelCache.Store(tableName, resp)
		return resp, nil
	default:
		return nil, fmt.Errorf("creating channel: %v", err)
	}
}

func (m *Manager) handleSchemaError(
	ctx context.Context,
	channelReq *model.CreateChannelRequest,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	m.stats.channelSchemaCreationErrorCount.Increment()

	snowflakeManager, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake manager: %v", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err := snowflakeManager.CreateSchema(ctx); err != nil {
		return nil, fmt.Errorf("creating schema: %v", err)
	}
	if err := snowflakeManager.CreateTable(ctx, channelReq.TableConfig.Table, eventSchema); err != nil {
		return nil, fmt.Errorf("creating table: %v", err)
	}
	return m.api.CreateChannel(ctx, channelReq)
}

func (m *Manager) handleTableError(
	ctx context.Context,
	channelReq *model.CreateChannelRequest,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	m.stats.channelTableCreationErrorCount.Increment()

	snowflakeManager, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake manager: %v", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err := snowflakeManager.CreateTable(ctx, channelReq.TableConfig.Table, eventSchema); err != nil {
		return nil, fmt.Errorf("creating table: %v", err)
	}
	return m.api.CreateChannel(ctx, channelReq)
}

func (m *Manager) recreateChannel(
	ctx context.Context,
	asyncDest *common.AsyncDestinationStruct,
	destConf destConfig,
	tableName string,
	eventSchema whutils.ModelTableSchema,
	existingChannelResponse *model.ChannelResponse,
) (*model.ChannelResponse, error) {
	if err := m.deleteChannel(ctx, tableName, existingChannelResponse.ChannelID); err != nil {
		return nil, fmt.Errorf("deleting channel: %v", err)
	}

	channelResponse, err := m.createChannel(ctx, asyncDest, destConf, tableName, eventSchema)
	if err != nil {
		return nil, fmt.Errorf("recreating channel: %v", err)
	}
	return channelResponse, nil
}

func (m *Manager) deleteChannel(ctx context.Context, tableName string, channelID string) error {
	m.channelCache.Delete(tableName)
	if err := m.api.DeleteChannel(ctx, channelID, true); err != nil {
		return fmt.Errorf("deleting channel: %v", err)
	}
	return nil
}

func (m *Manager) createSnowflakeManager(ctx context.Context, namespace string) (manager.Manager, error) {
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}
	modelWarehouse.Destination.Config["useKeyPairAuth"] = true // Since we are currently only supporting key pair auth

	sf, err := manager.New(whutils.SNOWFLAKE, m.conf, m.logger, m.statsFactory)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake manager: %v", err)
	}
	err = sf.Setup(ctx, modelWarehouse, &whutils.NopUploader{})
	if err != nil {
		return nil, fmt.Errorf("setting up snowflake manager: %v", err)
	}
	return sf, nil
}
