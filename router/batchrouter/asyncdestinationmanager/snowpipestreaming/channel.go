package snowpipestreaming

import (
	"context"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"

	internalapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errAuthz   = errors.New("snowpipe authorization error")
	errBackoff = errors.New("snowpipe backoff error")
)

// initializeChannelWithSchema creates a new channel for the given table if it doesn't exist.
// If the channel already exists, it checks for new columns and adds them to the table.
// It returns the channel response after creating or recreating the channel.
func (m *Manager) initializeChannelWithSchema(
	ctx context.Context,
	destinationID string,
	destConf *destConfig,
	tableName string,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	channelResponse, err := m.createChannel(ctx, destinationID, destConf, tableName, eventSchema)
	if err != nil {
		return nil, fmt.Errorf("creating channel for table %s: %w", tableName, err)
	}

	columnInfos := findNewColumns(eventSchema, channelResponse.SnowpipeSchema)
	if len(columnInfos) > 0 {
		if err := m.addColumns(ctx, destConf.Namespace, tableName, columnInfos); err != nil {
			return nil, fmt.Errorf("adding columns for table %s: %w", tableName, err)
		}

		channelResponse, err = m.recreateChannel(ctx, destinationID, destConf, tableName, eventSchema, channelResponse.ChannelID)
		if err != nil {
			return nil, fmt.Errorf("recreating channel for table %s: %w", tableName, err)
		}
	}
	return channelResponse, nil
}

func findNewColumns(eventSchema, snowpipeSchema whutils.ModelTableSchema) []whutils.ColumnInfo {
	var newColumns []whutils.ColumnInfo
	for column, dataType := range eventSchema {
		if _, exists := snowpipeSchema[column]; !exists {
			newColumns = append(newColumns, whutils.ColumnInfo{
				Name: column,
				Type: dataType,
			})
		}
	}
	return newColumns
}

// addColumns adds columns to a Snowflake table one at a time, as ALTER TABLE does not support IF NOT EXISTS with multiple columns.
func (m *Manager) addColumns(ctx context.Context, namespace, tableName string, columns []whutils.ColumnInfo) error {
	m.logger.Infon("Adding columns", logger.NewStringField("table", tableName))

	snowflakeManager, err := m.createSnowflakeManager(ctx, namespace)
	if err != nil {
		return fmt.Errorf("creating snowflake manager: %w", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err = snowflakeManager.AddColumns(ctx, tableName, columns); err != nil {
		return fmt.Errorf("adding column: %w, %w", errAuthz, err)
	}
	return nil
}

// createChannel creates a new channel for importing data to Snowpipe.
// If the channel already exists in the cache, it returns the cached response. Otherwise, it sends a request to create a new channel.
// It also handles errors related to missing schemas and tables.
func (m *Manager) createChannel(
	ctx context.Context,
	rudderIdentifier string,
	destConf *destConfig,
	tableName string,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	if response, ok := m.channelCache.Load(tableName); ok {
		return response.(*model.ChannelResponse), nil
	}

	req := &model.CreateChannelRequest{
		RudderIdentifier: rudderIdentifier,
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
		return nil, fmt.Errorf("creating channel: %w", err)
	}
	if resp.Success {
		m.channelCache.Store(tableName, resp)
		return resp, nil
	}

	switch resp.Code {
	case internalapi.ErrSchemaDoesNotExistOrNotAuthorized:
		resp, err = m.handleSchemaError(ctx, req, eventSchema)
		if err != nil {
			return nil, fmt.Errorf("creating channel for schema error: %w", err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("creating channel for schema error with code %s, message: %s and error: %s", resp.Code, resp.SnowflakeAPIMessage, resp.Error)
		}
		m.channelCache.Store(tableName, resp)
		return resp, nil
	case internalapi.ErrTableDoesNotExistOrNotAuthorized:
		resp, err = m.handleTableError(ctx, req, eventSchema)
		if err != nil {
			return nil, fmt.Errorf("creating channel for table error: %w", err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("creating channel for table error with code %s, message: %s and error: %s", resp.Code, resp.SnowflakeAPIMessage, resp.Error)
		}
		m.channelCache.Store(tableName, resp)
		return resp, nil
	default:
		return nil, fmt.Errorf("creating channel with code %s, message: %s and error: %s", resp.Code, resp.SnowflakeAPIMessage, resp.Error)
	}
}

// handleSchemaError handles errors related to missing schemas.
// It creates the necessary schema and table, then attempts to create the channel again.
func (m *Manager) handleSchemaError(
	ctx context.Context,
	channelReq *model.CreateChannelRequest,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	m.logger.Infon("Handling schema error",
		logger.NewStringField("schema", channelReq.TableConfig.Schema),
		logger.NewStringField("table", channelReq.TableConfig.Table),
	)

	snowflakeManager, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake manager: %w", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err := snowflakeManager.CreateSchema(ctx); err != nil {
		return nil, fmt.Errorf("creating schema: %w, %w", errAuthz, err)
	}
	if err := snowflakeManager.CreateTable(ctx, channelReq.TableConfig.Table, eventSchema); err != nil {
		return nil, fmt.Errorf("creating table: %w, %w", errAuthz, err)
	}
	return m.api.CreateChannel(ctx, channelReq)
}

// handleTableError handles errors related to missing tables.
// It creates the necessary table and then attempts to create the channel again.
func (m *Manager) handleTableError(
	ctx context.Context,
	channelReq *model.CreateChannelRequest,
	eventSchema whutils.ModelTableSchema,
) (*model.ChannelResponse, error) {
	m.logger.Infon("Handling table error",
		logger.NewStringField("schema", channelReq.TableConfig.Schema),
		logger.NewStringField("table", channelReq.TableConfig.Table),
	)

	snowflakeManager, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake manager: %w", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err := snowflakeManager.CreateTable(ctx, channelReq.TableConfig.Table, eventSchema); err != nil {
		return nil, fmt.Errorf("creating table: %w, %w", errAuthz, err)
	}
	return m.api.CreateChannel(ctx, channelReq)
}

// recreateChannel deletes an existing channel and then creates a new one.
func (m *Manager) recreateChannel(
	ctx context.Context,
	destinationID string,
	destConf *destConfig,
	tableName string,
	eventSchema whutils.ModelTableSchema,
	existingChannelID string,
) (*model.ChannelResponse, error) {
	m.logger.Infon("Recreating channel",
		logger.NewStringField("destinationID", destinationID),
		logger.NewStringField("tableName", tableName),
	)

	if err := m.deleteChannel(ctx, tableName, existingChannelID); err != nil {
		return nil, fmt.Errorf("deleting channel: %w", err)
	}

	channelResponse, err := m.createChannel(ctx, destinationID, destConf, tableName, eventSchema)
	if err != nil {
		return nil, fmt.Errorf("recreating channel: %w", err)
	}
	return channelResponse, nil
}

// deleteChannel removes a channel from the cache and deletes it from the Snowpipe.
func (m *Manager) deleteChannel(ctx context.Context, tableName, channelID string) error {
	m.channelCache.Delete(tableName)
	if err := m.api.DeleteChannel(ctx, channelID, true); err != nil {
		return fmt.Errorf("deleting channel: %w", err)
	}
	return nil
}

func (m *Manager) createSnowflakeManager(ctx context.Context, namespace string) (manager.Manager, error) {
	if m.isInBackoff() {
		return nil, fmt.Errorf("skipping snowflake manager creation due to backoff with error %s: %w", m.backoff.error, errBackoff)
	}
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}
	modelWarehouse.Destination.Config["useKeyPairAuth"] = true // Since we are currently only supporting key pair auth

	return m.managerCreator(ctx, modelWarehouse, m.appConfig, m.logger, m.statsFactory)
}
