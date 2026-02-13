package snowpipestreaming

import (
	"context"
	"fmt"
	"maps"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	internalapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type mockAPI struct {
	channelReq             *model.CreateChannelRequest
	createChannelOutputMap map[string]func() (*model.ChannelResponse, error)
	deleteChannelOutputMap map[string]func() error
	insertOutputMap        map[string]func() (*model.InsertResponse, error)
	getStatusOutputMap     map[string]func() (*model.StatusResponse, error)
}

func (m *mockAPI) CreateChannel(_ context.Context, channelReq *model.CreateChannelRequest) (*model.ChannelResponse, error) {
	m.channelReq = channelReq
	return m.createChannelOutputMap[channelReq.TableConfig.Table]()
}

func (m *mockAPI) DeleteChannel(_ context.Context, channelID string, _ bool) error {
	return m.deleteChannelOutputMap[channelID]()
}

func (m *mockAPI) Insert(_ context.Context, channelID string, _ *model.InsertRequest) (*model.InsertResponse, error) {
	return m.insertOutputMap[channelID]()
}

func (m *mockAPI) GetStatus(_ context.Context, channelID string) (*model.StatusResponse, error) {
	return m.getStatusOutputMap[channelID]()
}

type mockManager struct {
	manager.Manager
	createSchemaErr error
}

func newMockManager(m manager.Manager) *mockManager {
	return &mockManager{
		Manager: m,
	}
}

func (m *mockManager) CreateSchema(context.Context) error {
	return m.createSchemaErr
}

func (m *mockManager) CreateTable(context.Context, string, whutils.ModelTableSchema) error {
	return nil
}

type mockValidator struct {
	err error
}

func (m *mockValidator) Validate(_ context.Context, _ *backendconfig.DestinationT) *validations.DestinationValidationResponse {
	if m.err != nil {
		return &validations.DestinationValidationResponse{
			Success: false,
			Error:   m.err.Error(),
		}
	}
	return &validations.DestinationValidationResponse{
		Success: true,
	}
}

var (
	usersChannelResponse = &model.ChannelResponse{
		ChannelID: "test-users-channel",
		SnowpipeSchema: map[string]string{
			"ID":          "int",
			"NAME":        "string",
			"AGE":         "int",
			"RECEIVED_AT": "datetime",
		},
	}
	productsChannelResponse = &model.ChannelResponse{
		ChannelID: "test-products-channel",
		SnowpipeSchema: map[string]string{
			"ID":          "int",
			"PRODUCT_ID":  "string",
			"PRICE":       "float",
			"IN_STOCK":    "boolean",
			"RECEIVED_AT": "datetime",
		},
	}
	rudderDiscardsChannelResponse = &model.ChannelResponse{
		ChannelID:      "test-rudder-discards-channel",
		SnowpipeSchema: discardsSchema(),
	}
)

// nolint: unparam
func TestSnowpipeStreaming(t *testing.T) {
	destination := &backendconfig.DestinationT{
		ID:          "test-destination",
		WorkspaceID: "test-workspace",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "SNOWPIPE_STREAMING",
		},
		Config: make(map[string]any),
	}
	validations.Init()

	t.Run("Upload with invalid file path", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		output := sm.Upload(&common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.NotEmpty(t, output.AbortReason)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 1, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "aborted",
		}).LastValue())
	})

	t.Run("Upload with invalid record in file", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		output := sm.Upload(&common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/invalid_records.txt",
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.NotEmpty(t, output.AbortReason)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 1, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "aborted",
		}).LastValue())
	})

	t.Run("Upload not able to create discards channel", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		mockApi := &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"RUDDER_DISCARDS": func() (*model.ChannelResponse, error) {
					return nil, assert.AnError
				},
			},
		}
		sm.api = mockApi
		dest := &backendconfig.DestinationT{
			ID: destination.ID,
			Config: map[string]any{
				"enableIceberg": true,
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     dest,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.NotEmpty(t, output.AbortReason)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 1, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "aborted",
		}).LastValue())
		require.Equal(t, &model.CreateChannelRequest{
			RudderIdentifier: dest.ID,
			Partition:        "1",
			TableConfig: model.TableConfig{
				Schema:        "STRINGEMPTY",
				Table:         "RUDDER_DISCARDS",
				EnableIceberg: true,
			},
		}, mockApi.channelReq)
	})
	t.Run("Upload not able to create event channel", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"USERS": func() (*model.ChannelResponse, error) {
					return nil, assert.AnError
				},
			},
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1002, 1004}, output.ImportingJobIDs)
		require.Equal(t, 2, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1004","table":"PRODUCTS","failed":false,"reason":"","count":2}], "importCount":2}`, string(output.ImportingParameters))
		require.Equal(t, []int64{1001, 1003}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 2, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 2, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Upload insert status failed", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return nil
				},
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload insert status failed with deletion error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return assert.AnError
				},
				"test-products-channel": func() error {
					return assert.AnError
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Upload with unauthorized schema error should add backoff", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.api = &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"USERS": func() (*model.ChannelResponse, error) {
					return &model.ChannelResponse{Code: internalapi.ErrSchemaDoesNotExistOrNotAuthorized}, nil
				},
			},
		}
		managerCreatorCallCount := 0
		sm.managerCreator = func(_ context.Context, _ whutils.ModelWarehouse, _ *config.Config, _ logger.Logger, _ stats.Stats) (manager.Manager, error) {
			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			managerCreatorCallCount++
			mm := newMockManager(sf)
			mm.createSchemaErr = fmt.Errorf("failed to create schema")
			return mm, nil
		}
		sm.config.backoff.initialInterval = config.SingleValueLoader(time.Second * 10)
		asyncDestStruct := &common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_user_records.txt",
		}
		require.False(t, sm.isInBackoff())
		output1 := sm.Upload(asyncDestStruct)
		require.Equal(t, 2, output1.FailedCount)
		require.Equal(t, 0, output1.AbortCount)
		require.Equal(t, 1, managerCreatorCallCount)
		require.True(t, sm.isInBackoff())

		sm.Upload(asyncDestStruct)
		// client is not created again due to backoff error
		require.Equal(t, 1, managerCreatorCallCount)
		require.True(t, sm.isInBackoff())

		sm.now = func() time.Time {
			return timeutil.Now().Add(time.Second * 5)
		}
		require.True(t, sm.isInBackoff())
		sm.now = func() time.Time {
			return timeutil.Now().Add(time.Second * 20)
		}
		require.False(t, sm.isInBackoff())
		sm.Upload(asyncDestStruct)
		// client created again since backoff duration has been exceeded
		require.Equal(t, 2, managerCreatorCallCount)
		require.True(t, sm.isInBackoff())

		sm.managerCreator = func(_ context.Context, _ whutils.ModelWarehouse, _ *config.Config, _ logger.Logger, _ stats.Stats) (manager.Manager, error) {
			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			managerCreatorCallCount++
			return newMockManager(sf), nil
		}
		sm.now = func() time.Time {
			return timeutil.Now().Add(time.Second * 50)
		}
		sm.Upload(asyncDestStruct)
		require.Equal(t, 3, managerCreatorCallCount)
		require.False(t, sm.isInBackoff())
	})

	t.Run("destination config validation", func(t *testing.T) {
		testCases := []struct {
			name                 string
			validationError      error
			expectedFailedReason string
		}{
			{
				name:                 "should return validation error",
				validationError:      fmt.Errorf("missing permissions to do xyz"),
				expectedFailedReason: "missing permissions to do xyz",
			},
			{
				name:                 "should not return any error",
				validationError:      nil,
				expectedFailedReason: "failed to create schema",
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sm := New(config.New(), logger.NOP, stats.NOP, destination)
				sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
				sm.api = &mockAPI{
					createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
						"USERS": func() (*model.ChannelResponse, error) {
							return &model.ChannelResponse{Code: internalapi.ErrSchemaDoesNotExistOrNotAuthorized}, nil
						},
					},
				}
				sm.managerCreator = func(_ context.Context, _ whutils.ModelWarehouse, _ *config.Config, _ logger.Logger, _ stats.Stats) (manager.Manager, error) {
					sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
					mm := newMockManager(sf)
					mm.createSchemaErr = fmt.Errorf("failed to create schema")
					return mm, nil
				}
				sm.validator = &mockValidator{err: tc.validationError}
				asyncDestStruct := &common.AsyncDestinationStruct{
					Destination: destination,
					FileName:    "testdata/successful_user_records.txt",
				}
				output := sm.Upload(asyncDestStruct)
				require.Equal(t, 2, output.FailedCount)
				require.Equal(t, 0, output.AbortCount)
				require.Contains(t, output.FailedReason, tc.expectedFailedReason)
			})
		}
	})

	t.Run("Upload with discards table authorization error should mark the job as failed", func(t *testing.T) {
		testCases := []struct {
			name                 string
			validationError      error
			expectedFailedReason string
		}{
			{
				name:                 "authorization error",
				validationError:      fmt.Errorf("authorization error"),
				expectedFailedReason: "failed to validate snowpipe credentials: authorization error",
			},
			{
				name:                 "other error",
				validationError:      nil,
				expectedFailedReason: "failed to create schema",
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sm := New(config.New(), logger.NOP, stats.NOP, destination)
				sm.api = &mockAPI{
					createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
						"RUDDER_DISCARDS": func() (*model.ChannelResponse, error) {
							return &model.ChannelResponse{Code: internalapi.ErrSchemaDoesNotExistOrNotAuthorized}, nil
						},
					},
				}
				sm.managerCreator = func(_ context.Context, _ whutils.ModelWarehouse, _ *config.Config, _ logger.Logger, _ stats.Stats) (manager.Manager, error) {
					sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
					mm := newMockManager(sf)
					mm.createSchemaErr = fmt.Errorf("failed to create schema")
					return mm, nil
				}
				sm.validator = &mockValidator{err: tc.validationError}
				output := sm.Upload(&common.AsyncDestinationStruct{
					ImportingJobIDs: []int64{1},
					Destination:     destination,
					FileName:        "testdata/successful_user_records.txt",
				})
				require.Equal(t, 1, output.FailedCount)
				require.Equal(t, 0, output.AbortCount)
				require.Contains(t, output.FailedReason, tc.expectedFailedReason)
				require.Empty(t, output.AbortReason)
				require.Equal(t, true, sm.isInBackoff())
			})
		}
	})

	t.Run("Upload insert error for all events", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return nil, assert.AnError
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return nil, assert.AnError
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return nil
				},
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Upload discards inserts status failed", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", &model.ChannelResponse{
			ChannelID: "test-users-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"NAME":        "int",
				"AGE":         "int",
				"RECEIVED_AT": "datetime",
			},
		})
		sm.channelCache.Store("PRODUCTS", &model.ChannelResponse{
			ChannelID: "test-products-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"PRODUCT_ID":  "int",
				"PRICE":       "float",
				"IN_STOCK":    "boolean",
				"RECEIVED_AT": "datetime",
			},
		})

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return nil
				},
				"test-products-channel": func() error {
					return nil
				},
				"test-rudder-discards-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload discards inserts status failed with deletion error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", &model.ChannelResponse{
			ChannelID: "test-users-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"NAME":        "int",
				"AGE":         "int",
				"RECEIVED_AT": "datetime",
			},
		})
		sm.channelCache.Store("PRODUCTS", &model.ChannelResponse{
			ChannelID: "test-products-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"PRODUCT_ID":  "int",
				"PRICE":       "float",
				"IN_STOCK":    "boolean",
				"RECEIVED_AT": "datetime",
			},
		})

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return assert.AnError
				},
				"test-products-channel": func() error {
					return assert.AnError
				},
				"test-rudder-discards-channel": func() error {
					return assert.AnError
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload discards inserts error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", &model.ChannelResponse{
			ChannelID: "test-users-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"NAME":        "int",
				"AGE":         "int",
				"RECEIVED_AT": "datetime",
			},
		})
		sm.channelCache.Store("PRODUCTS", &model.ChannelResponse{
			ChannelID: "test-products-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"PRODUCT_ID":  "int",
				"PRICE":       "float",
				"IN_STOCK":    "boolean",
				"RECEIVED_AT": "datetime",
			},
		})

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return nil, assert.AnError
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return nil
				},
				"test-products-channel": func() error {
					return nil
				},
				"test-rudder-discards-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Nil(t, output.ImportingJobIDs)
		require.Zero(t, output.ImportingCount)
		require.Nil(t, output.ImportingParameters)
		require.Equal(t, []int64{1001, 1003, 1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Upload sort based on latest job id", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_sort_records.txt",
		})
		require.Equal(t, []int64{1002, 1003, 1001, 1004}, output.ImportingJobIDs)
		require.Equal(t, 4, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}], "importCount":4}`, string(output.ImportingParameters))
		require.Nil(t, output.FailedJobIDs)
		require.Zero(t, output.FailedCount)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload latest discards offset", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", &model.ChannelResponse{
			ChannelID: "test-users-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"NAME":        "int",
				"AGE":         "int",
				"RECEIVED_AT": "datetime",
			},
		})
		sm.channelCache.Store("PRODUCTS", &model.ChannelResponse{
			ChannelID: "test-products-channel",
			SnowpipeSchema: map[string]string{
				"ID":          "int",
				"PRODUCT_ID":  "int",
				"PRICE":       "float",
				"IN_STOCK":    "boolean",
				"RECEIVED_AT": "datetime",
			},
		})

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_sort_records.txt",
		})
		require.Equal(t, []int64{1002, 1003, 1001, 1004}, output.ImportingJobIDs)
		require.Equal(t, 4, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2},{"channelId":"test-rudder-discards-channel","offset":"1004","table":"RUDDER_DISCARDS","failed":false,"reason":"","count":4}], "importCount":4}`, string(output.ImportingParameters))
		require.Nil(t, output.FailedJobIDs)
		require.Zero(t, output.FailedCount)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload failed some events", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: false, Errors: []model.InsertError{{Message: assert.AnError.Error()}}}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1001, 1003}, output.ImportingJobIDs)
		require.Equal(t, 2, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-users-channel","offset":"1003","table":"USERS","failed":false,"reason":"","count":2}], "importCount": 2}`, string(output.ImportingParameters))
		require.Equal(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, assert.AnError.Error())
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 2, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.EqualValues(t, 2, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Upload with some events succeeding and some aborting", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)

		sm.api = &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"PRODUCTS": func() (*model.ChannelResponse, error) {
					return &model.ChannelResponse{
						Success:              false,
						SnowflakeAPIMessage:  "Unknown error occurred",
						SnowflakeAPIHttpCode: internalapi.ApiStatusUnsupportedColumn,
					}, nil
				},
			},
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_records.txt",
		})

		// Verify output: USERS table succeeds (jobs 1001, 1003), PRODUCTS table aborts (jobs 1002, 1004)
		require.Equal(t, common.AsyncUploadOutput{
			ImportingJobIDs:     []int64{1001, 1003},
			ImportingCount:      2,
			ImportingParameters: []byte(`{"importId":[{"channelId":"test-users-channel","offset":"1003","table":"USERS","failed":false,"reason":"","count":2}],"importCount":2}`),
			AbortJobIDs:         []int64{1002, 1004},
			AbortCount:          2,
			AbortReason:         output.AbortReason,
			DestinationID:       "test-destination",
		}, output)
		require.Contains(t, output.AbortReason, "Unknown error occurred")

		// Verify stats
		require.Equal(t, map[string]float64{
			"importing": 2,
			"aborted":   2,
		}, map[string]float64{
			"importing": statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
				"module":        "batch_router",
				"workspaceId":   "test-workspace",
				"destType":      "SNOWPIPE_STREAMING",
				"destinationId": "test-destination",
				"status":        "importing",
			}).LastValue(),
			"aborted": statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
				"module":        "batch_router",
				"workspaceId":   "test-workspace",
				"destType":      "SNOWPIPE_STREAMING",
				"destinationId": "test-destination",
				"status":        "aborted",
			}).LastValue(),
		})
	})
	t.Run("Upload success", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)
		sm.channelCache.Store("PRODUCTS", productsChannelResponse)

		sm.api = &mockAPI{
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_sort_records.txt",
		})
		require.Equal(t, []int64{1002, 1003, 1001, 1004}, output.ImportingJobIDs)
		require.Equal(t, 4, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}],"importCount":4}`, string(output.ImportingParameters))
		require.Nil(t, output.FailedJobIDs)
		require.Zero(t, output.FailedCount)
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "importing",
		}).LastValue())
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Upload success but with a 404 error in the first call to insert", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.channelCache.Store("RUDDER_DISCARDS", rudderDiscardsChannelResponse)
		sm.channelCache.Store("USERS", usersChannelResponse)

		productsCount := 0
		productSchema := map[string]string{
			"ID":          "int",
			"PRODUCT_ID":  "string",
			"PRICE":       "float",
			"IN_STOCK":    "boolean",
			"RECEIVED_AT": "datetime",
		}

		sm.api = &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				// For the first call to createChannel, we return a channel for which the insert will return a 404 error
				// For the second call to createChannel, we return a channel for which the insert will be successful
				"PRODUCTS": func() (*model.ChannelResponse, error) {
					if productsCount == 0 {
						productsCount++
						return &model.ChannelResponse{
							Success:        true,
							ChannelID:      "invalid-test-products-channel",
							SnowpipeSchema: productSchema,
						}, nil
					}
					return &model.ChannelResponse{
						Success:        true,
						ChannelID:      "test-products-channel",
						SnowpipeSchema: productSchema,
					}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"invalid-test-products-channel": func() error {
					return nil
				},
			},
			insertOutputMap: map[string]func() (*model.InsertResponse, error){
				"test-users-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"test-products-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
				"invalid-test-products-channel": func() (*model.InsertResponse, error) {
					return nil, internalapi.ErrChannelNotFound
				},
				"test-rudder-discards-channel": func() (*model.InsertResponse, error) {
					return &model.InsertResponse{Success: true}, nil
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    "testdata/successful_sort_records.txt",
		})
		require.Equal(t, []int64{1002, 1003, 1001, 1004}, output.ImportingJobIDs)
		require.Equal(t, 4, output.ImportingCount)
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}],"importCount":4}`, string(output.ImportingParameters))
		require.Nil(t, output.FailedJobIDs)
		require.Zero(t, output.FailedCount)
	})

	t.Run("Poll with invalid importID", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		output := sm.Poll(common.AsyncPoll{
			ImportId: "invalid",
		})
		require.False(t, output.InProgress)
		require.True(t, output.Complete)
		require.True(t, output.HasFailed)
		require.Equal(t, http.StatusBadRequest, output.StatusCode)
		require.NotEmpty(t, output.Error)
	})

	t.Run("Poll in progress", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0", LatestInsertedOffset: "1003"}, nil
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0", LatestInsertedOffset: "1004"}, nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.True(t, output.InProgress)
	})

	t.Run("Poll status failed", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: false, Success: false, Offset: "0"}, nil
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: false, Success: false, Offset: "0"}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return nil
				},
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.True(t, output.Complete)
		require.True(t, output.HasFailed)
		require.JSONEq(t, `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":true,"reason":"invalid status response with valid: false, success: false","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":true,"reason":"invalid status response with valid: false, success: false","count":2}]`, output.FailedJobParameters)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Poll status failed with deletion error", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: false, Success: false, Offset: "0"}, nil
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: false, Success: false, Offset: "0"}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-users-channel": func() error {
					return assert.AnError
				},
				"test-products-channel": func() error {
					return assert.AnError
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.True(t, output.Complete)
		require.True(t, output.HasFailed)
		require.JSONEq(t, `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":true,"reason":"invalid status response with valid: false, success: false","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":true,"reason":"invalid status response with valid: false, success: false","count":2}]`, output.FailedJobParameters)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})
	t.Run("Poll error", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return nil, assert.AnError
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return nil, assert.AnError
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-products-channel": func() error {
					return nil
				},
				"test-users-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.True(t, output.Complete)
		require.True(t, output.HasFailed)
		require.JSONEq(t, `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":true,"reason":"getting status: assert.AnError general error for testing","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":true,"reason":"getting status: assert.AnError general error for testing","count":2}]`, output.FailedJobParameters)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Poll success", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: true, Success: true, Offset: "1003"}, nil
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: true, Success: true, Offset: "1004"}, nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.True(t, output.Complete)
		require.False(t, output.HasFailed)
		require.False(t, output.HasWarning)
		require.Empty(t, output.FailedJobParameters)
		require.EqualValues(t, 4, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "succeeded",
		}).LastValue())
		require.Zero(t, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
	})

	t.Run("Poll 404 channel not found - successful recreation", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS"}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		getStatusCallCount := 0
		createChannelCallCount := 0
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					getStatusCallCount++
					return nil, internalapi.ErrChannelNotFound
				},
				"recreated-products-channel": func() (*model.StatusResponse, error) {
					getStatusCallCount++
					return &model.StatusResponse{Success: true, Valid: true, Offset: "1003"}, nil
				},
			},
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"PRODUCTS": func() (*model.ChannelResponse, error) {
					createChannelCallCount++
					return &model.ChannelResponse{Success: true, ChannelID: "recreated-products-channel"}, nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, 2, getStatusCallCount)
		require.Equal(t, 1, createChannelCallCount)
	})

	t.Run("Poll 404 channel not found - recreation fails", func(t *testing.T) {
		importID := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS"}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)
		createChannelCallCount := 0

		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-products-channel": func() (*model.StatusResponse, error) {
					return nil, internalapi.ErrChannelNotFound
				},
			},
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"PRODUCTS": func() (*model.ChannelResponse, error) {
					createChannelCallCount++
					return nil, fmt.Errorf("failed to recreate channel")
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-products-channel": func() error {
					return nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, 1, createChannelCallCount)
	})

	t.Run("Poll caching", func(t *testing.T) {
		importID := `[{"channelId":"test-channel-1","offset":"1","table":"1","failed":false,"reason":"","count":1},{"channelId":"test-channel-2","offset":"2","table":"2","failed":false,"reason":"","count":2},{"channelId":"test-channel-3","offset":"3","table":"3","failed":false,"reason":"","count":3},{"channelId":"test-channel-4","offset":"4","table":"4","failed":false,"reason":"","count":4}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		statusCalls := 0
		sm := New(config.New(), logger.NOP, statsStore, destination)
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-channel-1": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: false, Success: true}, nil
				},
				"test-channel-2": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: true, Success: false}, nil
				},
				"test-channel-3": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0", LatestInsertedOffset: "3"}, nil
				},
				"test-channel-4": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: true, Success: true, Offset: "4", LatestInsertedOffset: "4"}, nil
				},
			},
		}
		output := sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.True(t, output.InProgress)

		t.Log("Polling again should not call getStatus for channels 1, 2 and 4 since they already reached the terminal state")
		sm.api = &mockAPI{
			getStatusOutputMap: map[string]func() (*model.StatusResponse, error){
				"test-channel-3": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: true, Success: true, Offset: "3"}, nil
				},
			},
			deleteChannelOutputMap: map[string]func() error{
				"test-channel-1": func() error {
					return nil
				},
				"test-channel-2": func() error {
					return nil
				},
			},
		}
		output = sm.Poll(common.AsyncPoll{
			ImportId: importID,
		})
		require.False(t, output.InProgress)
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.True(t, output.Complete)
		require.True(t, output.HasFailed)
		require.Equal(t, `[{"channelId":"test-channel-1","offset":"1","table":"1","failed":true,"reason":"invalid status response with valid: false, success: true","count":1},{"channelId":"test-channel-2","offset":"2","table":"2","failed":true,"reason":"invalid status response with valid: true, success: false","count":2},{"channelId":"test-channel-3","offset":"3","table":"3","failed":false,"reason":"","count":3},{"channelId":"test-channel-4","offset":"4","table":"4","failed":false,"reason":"","count":4}]`, output.FailedJobParameters)
		require.EqualValues(t, 3, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "failed",
		}).LastValue())
		require.EqualValues(t, 7, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
			"status":        "succeeded",
		}).LastValue())
		require.EqualValues(t, 1, statsStore.Get("snowpipe_streaming_polling_in_progress", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "SNOWPIPE_STREAMING",
			"destinationId": "test-destination",
		}).LastValue())
		require.Equal(t, 5, statusCalls) // 4 channels + 1 for polling in progress
	})

	t.Run("GetUploadStats", func(t *testing.T) {
		tests := []struct {
			name                  string
			failedJobParameters   string
			importingList         []*jobsdb.JobT
			expectedStatusCode    int
			expectedError         bool
			expectedFailedKeys    []int64
			expectedFailedReasons map[int64]string
			expectedSucceededKeys []int64
		}{
			{
				name:                "invalid importInfo",
				failedJobParameters: "invalid",
				expectedStatusCode:  http.StatusBadRequest,
				expectedError:       true,
			},
			{
				name:                "success and failed events",
				failedJobParameters: `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":true,"reason":"users polling failed","count":2}]`,
				importingList: []*jobsdb.JobT{
					{
						JobID:        1001,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1002,
						EventPayload: []byte(`{"metadata":{"table":"PRODUCTS"}}`),
					},
					{
						JobID:        1003,
						EventPayload: []byte(`{"metadata":{"table":"PRODUCTS"}}`),
					},
					{
						JobID:        1004,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
				},
				expectedStatusCode:    http.StatusOK,
				expectedFailedKeys:    []int64{1001, 1004},
				expectedFailedReasons: map[int64]string{1001: "users polling failed", 1004: "users polling failed"},
				expectedSucceededKeys: []int64{1002, 1003},
			},
			{
				name:                "failed events with specific job ID range",
				failedJobParameters: `[{"channelId":"test-users-channel","offset":"1010","table":"USERS","failed":true,"reason":"partial failure","count":5,"failedJobIds":{"start":1008,"end":1009}}]`,
				importingList: []*jobsdb.JobT{
					{
						JobID:        1006,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1008,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1009,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1010,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
				},
				expectedStatusCode:    http.StatusOK,
				expectedFailedKeys:    []int64{1008, 1009},
				expectedFailedReasons: map[int64]string{1008: "partial failure", 1009: "partial failure"},
				expectedSucceededKeys: []int64{1006, 1010},
			},
			{
				name:                "failed events with job ID range - all jobs outside range",
				failedJobParameters: `[{"channelId":"test-users-channel","offset":"1010","table":"USERS","failed":true,"reason":"range failure","count":3,"failedJobIds":{"start":2000,"end":2005}}]`,
				importingList: []*jobsdb.JobT{
					{
						JobID:        1001,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1002,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
				},
				expectedStatusCode:    http.StatusOK,
				expectedFailedKeys:    nil,
				expectedFailedReasons: map[int64]string{},
				expectedSucceededKeys: []int64{1001, 1002},
			},
			{
				name:                "failed events with job ID range - all jobs inside range",
				failedJobParameters: `[{"channelId":"test-users-channel","offset":"1010","table":"USERS","failed":true,"reason":"complete range failure","count":3,"failedJobIds":{"start":1001,"end":1003}}]`,
				importingList: []*jobsdb.JobT{
					{
						JobID:        1001,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1002,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        1003,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
				},
				expectedStatusCode:    http.StatusOK,
				expectedFailedKeys:    []int64{1001, 1002, 1003},
				expectedFailedReasons: map[int64]string{1001: "complete range failure", 1002: "complete range failure", 1003: "complete range failure"},
				expectedSucceededKeys: nil,
			},
			{
				name:                "mixed success, failed without range, and failed with range",
				failedJobParameters: `[{"channelId":"test-products-channel","offset":"1005","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1010","table":"USERS","failed":true,"reason":"all users failed","count":2},{"channelId":"test-orders-channel","offset":"1015","table":"ORDERS","failed":true,"reason":"partial orders failure","count":3,"failedJobIds":{"start":2008,"end":2009}}]`,
				importingList: []*jobsdb.JobT{
					{
						JobID:        1001,
						EventPayload: []byte(`{"metadata":{"table":"PRODUCTS"}}`),
					},
					{
						JobID:        1002,
						EventPayload: []byte(`{"metadata":{"table":"USERS"}}`),
					},
					{
						JobID:        2007,
						EventPayload: []byte(`{"metadata":{"table":"ORDERS"}}`),
					},
					{
						JobID:        2008,
						EventPayload: []byte(`{"metadata":{"table":"ORDERS"}}`),
					},
					{
						JobID:        2009,
						EventPayload: []byte(`{"metadata":{"table":"ORDERS"}}`),
					},
					{
						JobID:        2010,
						EventPayload: []byte(`{"metadata":{"table":"ORDERS"}}`),
					},
				},
				expectedStatusCode:    http.StatusOK,
				expectedFailedKeys:    []int64{1002, 2008, 2009},
				expectedFailedReasons: map[int64]string{1002: "all users failed", 2008: "partial orders failure", 2009: "partial orders failure"},
				expectedSucceededKeys: []int64{1001, 2007, 2010},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				statsStore, err := memstats.New()
				require.NoError(t, err)

				sm := New(config.New(), logger.NOP, statsStore, destination)
				output := sm.GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: tt.failedJobParameters,
					ImportingList:       tt.importingList,
				})

				require.Equal(t, tt.expectedStatusCode, output.StatusCode)

				if tt.expectedError {
					require.NotEmpty(t, output.Error)
				} else {
					require.Equal(t, tt.expectedFailedKeys, output.Metadata.FailedKeys)
					require.Equal(t, tt.expectedFailedReasons, output.Metadata.FailedReasons)
					require.Equal(t, tt.expectedSucceededKeys, output.Metadata.SucceededKeys)
				}
			})
		}
	})

	t.Run("processPollImportInfos", func(t *testing.T) {
		tests := []struct {
			name                   string
			infos                  []*importInfo
			prePopulatedCache      map[string]*importInfo
			mockGetStatusResponses map[string]func() (*model.StatusResponse, error)
			expectedInProgress     bool
			expectedCacheSize      int
			expectedFailedChannels []string
			expectedResetInfos     []string                 // ChannelIDs that should have Failed=false and FailedJobIds=nil
			expectedFailedJobIds   map[string]*failedJobIds // ChannelID -> expected FailedJobIds for failed channels
		}{
			{
				name:               "empty infos slice",
				infos:              []*importInfo{},
				expectedInProgress: false,
				expectedCacheSize:  0,
			},
			{
				name: "single info already processed (cached)",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				prePopulatedCache: map[string]*importInfo{
					"test-channel-1": {
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
						Failed:    false,
					},
				},
				expectedInProgress: false,
				expectedCacheSize:  1,
			},
			{
				name: "single info - getImportStatus returns error",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return nil, fmt.Errorf("API error")
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
			},
			{
				name: "single info - in progress (flushing)",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "50",  // Less than expected "100"
							LatestInsertedOffset: "100", // Greater than committed - flushing in progress
						}, nil
					},
				},
				expectedInProgress: true,
				expectedCacheSize:  0, // In progress items not cached
				expectedResetInfos: []string{"test-channel-1"},
			},
			{
				name: "single info - completed successfully",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "100", // Equal to expected
							LatestInsertedOffset: "100",
						}, nil
					},
				},
				expectedInProgress: false,
				expectedCacheSize:  1,
				expectedResetInfos: []string{"test-channel-1"},
			},
			{
				name: "single info - events lost scenario",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "50", // Less than expected
							LatestInsertedOffset: "50", // Equal to committed - events lost
						}, nil
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
				expectedFailedJobIds: map[string]*failedJobIds{
					"test-channel-1": {
						Start: 51,
						End:   100,
					},
				},
			},
			{
				name: "multiple infos - mixed scenarios",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
					{
						ChannelID: "test-channel-2", // Pre-cached
						Offset:    "200",
						Table:     "PRODUCTS",
						Count:     3,
					},
					{
						ChannelID: "test-channel-3",
						Offset:    "300",
						Table:     "ORDERS",
						Count:     7,
					},
					{
						ChannelID: "test-channel-4",
						Offset:    "400",
						Table:     "INVENTORY",
						Count:     2,
					},
				},
				prePopulatedCache: map[string]*importInfo{
					"test-channel-2": {
						ChannelID: "test-channel-2",
						Offset:    "200",
						Table:     "PRODUCTS",
						Count:     3,
						Failed:    false,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return nil, fmt.Errorf("network error")
					},
					"test-channel-3": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "250",
							LatestInsertedOffset: "300", // In progress
						}, nil
					},
					"test-channel-4": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "400", // Completed
							LatestInsertedOffset: "400",
						}, nil
					},
				},
				expectedInProgress:     true, // Because channel-3 is in progress
				expectedCacheSize:      3,    // channel-1 (failed) + channel-2 (pre-existing) + channel-4 (completed)
				expectedFailedChannels: []string{"test-channel-1"},
				expectedResetInfos:     []string{"test-channel-3", "test-channel-4"},
			},
			{
				name: "all infos in progress",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
					{
						ChannelID: "test-channel-2",
						Offset:    "200",
						Table:     "PRODUCTS",
						Count:     3,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "50",
							LatestInsertedOffset: "100", // In progress
						}, nil
					},
					"test-channel-2": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "150",
							LatestInsertedOffset: "200", // In progress
						}, nil
					},
				},
				expectedInProgress: true,
				expectedCacheSize:  0, // None cached when in progress
				expectedResetInfos: []string{"test-channel-1", "test-channel-2"},
			},
			{
				name: "all infos completed or failed",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
					{
						ChannelID: "test-channel-2",
						Offset:    "200",
						Table:     "PRODUCTS",
						Count:     3,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "100", // Completed
							LatestInsertedOffset: "100",
						}, nil
					},
					"test-channel-2": func() (*model.StatusResponse, error) {
						return nil, fmt.Errorf("failed to get status")
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      2,
				expectedFailedChannels: []string{"test-channel-2"},
				expectedResetInfos:     []string{"test-channel-1"},
			},
			{
				name: "info state reset - failed info becomes successful",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
						Failed:    true,
						Reason:    "previous error",
						FailedJobIds: &failedJobIds{
							Start: math.MinInt64 + 1,
							End:   10,
						},
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "100",
							LatestInsertedOffset: "100",
						}, nil
					},
				},
				expectedInProgress: false,
				expectedCacheSize:  1,
				expectedResetInfos: []string{"test-channel-1"},
			},
			{
				name: "invalid status response - valid=false",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:   false,
							Success: true,
							Offset:  "100",
						}, nil
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
			},
			{
				name: "invalid status response - success=false",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:   true,
							Success: false,
							Offset:  "100",
						}, nil
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
			},
			{
				name: "empty Offset - treated as in progress",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "", // Empty offset
							LatestInsertedOffset: "100",
						}, nil
					},
				},
				expectedInProgress: true,
				expectedCacheSize:  0,
				expectedResetInfos: []string{"test-channel-1"},
			},
			{
				name: "both Offset and LatestInsertedOffset empty - events lost",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "", // Empty offset
							LatestInsertedOffset: "", // Empty LatestInsertedOffset
						}, nil
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
				expectedFailedJobIds: map[string]*failedJobIds{
					"test-channel-1": {
						Start: math.MinInt64 + 1,
						End:   100,
					},
				},
			},
			{
				name: "empty Offset with LatestInsertedOffset less than expected - events lost",
				infos: []*importInfo{
					{
						ChannelID: "test-channel-1",
						Offset:    "100",
						Table:     "USERS",
						Count:     5,
					},
				},
				mockGetStatusResponses: map[string]func() (*model.StatusResponse, error){
					"test-channel-1": func() (*model.StatusResponse, error) {
						return &model.StatusResponse{
							Valid:                true,
							Success:              true,
							Offset:               "",   // Empty offset
							LatestInsertedOffset: "75", // Less than expected
						}, nil
					},
				},
				expectedInProgress:     false,
				expectedCacheSize:      1,
				expectedFailedChannels: []string{"test-channel-1"},
				expectedFailedJobIds: map[string]*failedJobIds{
					"test-channel-1": {
						Start: math.MinInt64 + 1,
						End:   100,
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				statsStore, err := memstats.New()
				require.NoError(t, err)
				sm := New(config.New(), logger.NOP, statsStore, destination)

				// Pre-populate cache if needed
				if tt.prePopulatedCache != nil {
					maps.Copy(sm.polledImportInfoMap, tt.prePopulatedCache)
				}

				// Set up mock API if needed
				if tt.mockGetStatusResponses != nil {
					sm.api = &mockAPI{
						getStatusOutputMap: tt.mockGetStatusResponses,
					}
				}

				anyInProgress := sm.processPollImportInfos(context.Background(), tt.infos)

				// Assert expected in progress state
				assert.Equal(t, tt.expectedInProgress, anyInProgress, "Expected inProgress state mismatch")

				// Assert expected cache size
				assert.Len(t, sm.polledImportInfoMap, tt.expectedCacheSize, "Expected cache size mismatch")

				// Assert expected failed channels are marked as failed in cache
				for _, channelID := range tt.expectedFailedChannels {
					cachedInfo, exists := sm.polledImportInfoMap[channelID]
					assert.True(t, exists, "Expected failed channel %s to be in cache", channelID)
					assert.True(t, cachedInfo.Failed, "Expected channel %s to be marked as failed", channelID)
					assert.NotEmpty(t, cachedInfo.Reason, "Expected channel %s to have failure reason", channelID)
					assert.Equal(t, tt.expectedFailedJobIds[channelID], cachedInfo.FailedJobIds, "Expected failed job ids mismatch")
				}

				// Assert expected reset infos have Failed=false and FailedJobIds=nil
				for _, channelID := range tt.expectedResetInfos {
					var targetInfo *importInfo
					for _, info := range tt.infos {
						if info.ChannelID == channelID {
							targetInfo = info
							break
						}
					}
					assert.NotNil(t, targetInfo, "Could not find info with channelID %s", channelID)
					assert.False(t, targetInfo.Failed, "Expected info %s to have Failed=false", channelID)
					assert.Nil(t, targetInfo.FailedJobIds, "Expected info %s to have FailedJobIds=nil", channelID)
				}
			})
		}
	})
}

func TestFindNewColumns(t *testing.T) {
	tests := []struct {
		name           string
		eventSchema    whutils.ModelTableSchema
		snowpipeSchema whutils.ModelTableSchema
		expected       []whutils.ColumnInfo
	}{
		{
			name: "new column with different data type in event schema",
			eventSchema: whutils.ModelTableSchema{
				"new_column":      "STRING",
				"existing_column": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column": "INT",
			},
			expected: []whutils.ColumnInfo{
				{Name: "new_column", Type: "STRING"},
			},
		},
		{
			name: "new and existing columns with multiple data types",
			eventSchema: whutils.ModelTableSchema{
				"new_column1":     "STRING",
				"new_column2":     "BOOLEAN",
				"existing_column": "INT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column":         "INT",
				"another_existing_column": "FLOAT",
			},
			expected: []whutils.ColumnInfo{
				{Name: "new_column1", Type: "STRING"},
				{Name: "new_column2", Type: "BOOLEAN"},
			},
		},
		{
			name: "all columns in event schema are new",
			eventSchema: whutils.ModelTableSchema{
				"new_column1": "STRING",
				"new_column2": "BOOLEAN",
				"new_column3": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{},
			expected: []whutils.ColumnInfo{
				{Name: "new_column1", Type: "STRING"},
				{Name: "new_column2", Type: "BOOLEAN"},
				{Name: "new_column3", Type: "FLOAT"},
			},
		},
		{
			name: "case sensitivity check",
			eventSchema: whutils.ModelTableSchema{
				"ColumnA": "STRING",
				"columna": "BOOLEAN",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"columna": "BOOLEAN",
			},
			expected: []whutils.ColumnInfo{
				{Name: "ColumnA", Type: "STRING"},
			},
		},
		{
			name: "all columns match with identical types",
			eventSchema: whutils.ModelTableSchema{
				"existing_column1": "STRING",
				"existing_column2": "FLOAT",
			},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column1": "STRING",
				"existing_column2": "FLOAT",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name:        "event schema is empty, Snowpipe schema has columns",
			eventSchema: whutils.ModelTableSchema{},
			snowpipeSchema: whutils.ModelTableSchema{
				"existing_column": "STRING",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name: "Snowpipe schema is nil",
			eventSchema: whutils.ModelTableSchema{
				"new_column": "STRING",
			},
			snowpipeSchema: nil,
			expected: []whutils.ColumnInfo{
				{Name: "new_column", Type: "STRING"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findNewColumns(tt.eventSchema, tt.snowpipeSchema)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestDestConfig_Decode(t *testing.T) {
	tests := []struct {
		name      string
		input     map[string]any
		expected  destConfig
		wantError bool
	}{
		{
			name: "Valid Input",
			input: map[string]any{
				"account":              "test-account",
				"warehouse":            "test-warehouse",
				"database":             "test-database",
				"user":                 "test-user",
				"role":                 "test-role",
				"privateKey":           "test-key",
				"privateKeyPassphrase": "test-passphrase",
				"namespace":            "test-namespace",
				"enableIceberg":        true,
			},
			expected: destConfig{
				Account:              "test-account",
				Warehouse:            "test-warehouse",
				Database:             "test-database",
				User:                 "test-user",
				Role:                 "test-role",
				PrivateKey:           "test-key",
				PrivateKeyPassphrase: "test-passphrase",
				Namespace:            "TEST_NAMESPACE",
				EnableIceberg:        true,
			},
			wantError: false,
		},
		{
			name: "Invalid Input",
			input: map[string]any{
				"account": 123, // Invalid type
			},
			expected:  destConfig{},
			wantError: true,
		},
		{
			name:  "Empty Map",
			input: map[string]any{},
			expected: destConfig{
				Namespace: "STRINGEMPTY",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c destConfig
			err := c.Decode(tt.input)

			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, c)
			}
		})
	}
}
