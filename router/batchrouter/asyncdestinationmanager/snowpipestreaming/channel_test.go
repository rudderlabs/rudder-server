package snowpipestreaming

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	internalapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// fakeWarehouseManager implements only the manager.Manager methods used by the channel code.
type fakeWarehouseManager struct {
	manager.Manager

	addColumns  func(tableName string, columns []whutils.ColumnInfo) error
	createTable func(tableName string, columnMap whutils.ModelTableSchema) error

	addColumnsCalls  int
	createTableCalls int
}

func (f *fakeWarehouseManager) CreateSchema(context.Context) error { return nil }

func (f *fakeWarehouseManager) CreateTable(_ context.Context, tableName string, columnMap whutils.ModelTableSchema) error {
	f.createTableCalls++
	return f.createTable(tableName, columnMap)
}

func (f *fakeWarehouseManager) AddColumns(_ context.Context, tableName string, columns []whutils.ColumnInfo) error {
	f.addColumnsCalls++
	return f.addColumns(tableName, columns)
}

func (f *fakeWarehouseManager) Cleanup(context.Context) {}

func TestInitializeChannelWithSchema(t *testing.T) {
	destination := &backendconfig.DestinationT{
		ID:                    "test-destination",
		WorkspaceID:           "test-workspace",
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "SNOWPIPE_STREAMING"},
		Config:                make(map[string]any),
	}
	destConf := &destConfig{Namespace: "TEST_NAMESPACE"}

	// eventSchema has a column (CUSTOM) that the cached channel schema does not know about,
	// e.g. because jsonPaths was enabled for the table.
	eventSchema := whutils.ModelTableSchema{"ID": "int", "RECEIVED_AT": "datetime", "CUSTOM": "json"}
	staleChannel := &model.ChannelResponse{
		Success:        true,
		ChannelID:      "stale-channel",
		SnowpipeSchema: whutils.ModelTableSchema{"ID": "int", "RECEIVED_AT": "datetime"},
	}
	errTableDoesNotExist := errors.New("002003 (42S02): SQL compilation error: Table 'TEST_NAMESPACE.EVENTS' does not exist or not authorized")

	newManager := func(t *testing.T, fake *fakeWarehouseManager) *Manager {
		t.Helper()
		sm := New(config.New(), logger.NOP, stats.NOP, destination)
		sm.managerCreator = func(context.Context, whutils.ModelWarehouse, *config.Config, logger.Logger, stats.Stats) (manager.Manager, error) {
			return fake, nil
		}
		return sm
	}

	// droppedTableAPI mimics the Snowpipe service for a table dropped externally: until the stale channel is
	// deleted it keeps returning it (if staleFromService), afterwards it reports the table as missing until it is recreated.
	droppedTableAPI := func(staleFromService bool, tableCreated *bool, deletedChannels *[]string, createChannelCalls *int) *mockAPI {
		return &mockAPI{
			deleteChannelOutputMap: map[string]func() error{
				"stale-channel": func() error {
					*deletedChannels = append(*deletedChannels, "stale-channel")
					return nil
				},
			},
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"EVENTS": func() (*model.ChannelResponse, error) {
					*createChannelCalls++
					switch {
					case *tableCreated:
						return &model.ChannelResponse{Success: true, ChannelID: "fresh-channel", SnowpipeSchema: eventSchema}, nil
					case staleFromService && len(*deletedChannels) == 0:
						return staleChannel, nil
					default:
						return &model.ChannelResponse{Success: false, Code: internalapi.ErrTableDoesNotExistOrNotAuthorized}, nil
					}
				},
			},
		}
	}

	for _, tc := range []struct {
		name                       string
		cachedLocally              bool
		staleFromService           bool
		expectedCreateChannelCalls int
	}{
		{name: "stale channel cached locally", cachedLocally: true, expectedCreateChannelCalls: 2},
		{name: "stale channel returned by the snowpipe service", staleFromService: true, expectedCreateChannelCalls: 3},
	} {
		t.Run(tc.name+" for a dropped table recreates the table", func(t *testing.T) {
			var (
				tableCreated       bool
				deletedChannels    []string
				createChannelCalls int
			)
			fake := &fakeWarehouseManager{
				addColumns: func(string, []whutils.ColumnInfo) error { return errTableDoesNotExist },
				createTable: func(tableName string, columnMap whutils.ModelTableSchema) error {
					require.Equal(t, "EVENTS", tableName)
					require.Equal(t, eventSchema, columnMap)
					tableCreated = true
					return nil
				},
			}
			sm := newManager(t, fake)
			if tc.cachedLocally {
				sm.channelCache.Store("EVENTS", staleChannel)
			}
			sm.api = droppedTableAPI(tc.staleFromService, &tableCreated, &deletedChannels, &createChannelCalls)

			resp, err := sm.initializeChannelWithSchema(context.Background(), destination.ID, destConf, "EVENTS", eventSchema)
			require.NoError(t, err)
			require.Equal(t, "fresh-channel", resp.ChannelID)
			require.Equal(t, []string{"stale-channel"}, deletedChannels)
			require.Equal(t, 1, fake.addColumnsCalls)
			require.Equal(t, 1, fake.createTableCalls)
			require.Equal(t, tc.expectedCreateChannelCalls, createChannelCalls)

			cached, ok := sm.channelCache.Load("EVENTS")
			require.True(t, ok)
			require.Equal(t, "fresh-channel", cached.(*model.ChannelResponse).ChannelID)
		})
	}

	t.Run("adding columns keeps failing after retrying once with a fresh channel", func(t *testing.T) {
		fake := &fakeWarehouseManager{
			addColumns: func(string, []whutils.ColumnInfo) error { return errors.New("insufficient privileges") },
		}
		sm := newManager(t, fake)

		var deletedChannels []string
		createChannelCalls := 0
		sm.api = &mockAPI{
			deleteChannelOutputMap: map[string]func() error{
				"stale-channel": func() error {
					deletedChannels = append(deletedChannels, "stale-channel")
					return nil
				},
			},
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"EVENTS": func() (*model.ChannelResponse, error) {
					createChannelCalls++
					return staleChannel, nil
				},
			},
		}

		_, err := sm.initializeChannelWithSchema(context.Background(), destination.ID, destConf, "EVENTS", eventSchema)
		require.ErrorIs(t, err, errAbort)
		require.ErrorContains(t, err, "insufficient privileges")
		require.Equal(t, 2, fake.addColumnsCalls)
		require.Equal(t, 2, createChannelCalls)
		require.Equal(t, []string{"stale-channel"}, deletedChannels)
	})

	t.Run("deleting the channel fails after adding columns failed", func(t *testing.T) {
		fake := &fakeWarehouseManager{
			addColumns: func(string, []whutils.ColumnInfo) error { return errTableDoesNotExist },
		}
		sm := newManager(t, fake)
		sm.channelCache.Store("EVENTS", staleChannel)
		sm.api = &mockAPI{
			deleteChannelOutputMap: map[string]func() error{
				"stale-channel": func() error { return errors.New("service unavailable") },
			},
		}

		_, err := sm.initializeChannelWithSchema(context.Background(), destination.ID, destConf, "EVENTS", eventSchema)
		require.Error(t, err)
		require.NotErrorIs(t, err, errAbort, "a failed deletion should be retried, not aborted")
		require.ErrorContains(t, err, "service unavailable")
		require.Equal(t, 1, fake.addColumnsCalls)
	})
}

func TestHandleChannelRecoveryPostBulkStatus(t *testing.T) {
	destination := &backendconfig.DestinationT{
		ID:                    "test-destination",
		WorkspaceID:           "test-workspace",
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "SNOWPIPE_STREAMING"},
		Config:                make(map[string]any),
	}

	t.Run("unsuccessful channel recreation is not cached", func(t *testing.T) {
		sm := New(config.New(), logger.NOP, stats.NOP, destination)
		sm.channelCache.Store("EVENTS", &model.ChannelResponse{Success: true, ChannelID: "invalid-channel"})
		sm.api = &mockAPI{
			deleteChannelOutputMap: map[string]func() error{
				"invalid-channel": func() error { return nil },
			},
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"EVENTS": func() (*model.ChannelResponse, error) {
					return &model.ChannelResponse{
						Success:             false,
						Code:                internalapi.ErrTableDoesNotExistOrNotAuthorized,
						SnowflakeAPIMessage: "Table does not exist",
					}, nil
				},
			},
			// No bulk status output: requesting the status of the (empty) recreated channel ID would panic.
			getBulkStatusOutputMap: map[string]func() (*model.BulkStatusResponse, error){},
		}

		_, err := sm.handleChannelRecoveryPostBulkStatus(context.Background(), &importInfo{
			ChannelID: "invalid-channel",
			Table:     "EVENTS",
			Offset:    "1",
		}, true)
		require.ErrorContains(t, err, "recreating channel with code ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED")

		_, ok := sm.channelCache.Load("EVENTS")
		require.False(t, ok)
	})
}
