package snowpipestreaming

import (
	"context"
	"fmt"
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
	createChannelOutputMap map[string]func() (*model.ChannelResponse, error)
	deleteChannelOutputMap map[string]func() error
	insertOutputMap        map[string]func() (*model.InsertResponse, error)
	getStatusOutputMap     map[string]func() (*model.StatusResponse, error)
}

func (m *mockAPI) CreateChannel(_ context.Context, channelReq *model.CreateChannelRequest) (*model.ChannelResponse, error) {
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
		Config: make(map[string]interface{}),
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
		sm.api = &mockAPI{
			createChannelOutputMap: map[string]func() (*model.ChannelResponse, error){
				"RUDDER_DISCARDS": func() (*model.ChannelResponse, error) {
					return nil, assert.AnError
				},
			},
		}
		output := sm.Upload(&common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
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
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1004","table":"PRODUCTS","failed":false,"reason":"","count":2}]}`, string(output.ImportingParameters))
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
		require.Equal(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]}`, string(output.ImportingParameters))
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
		require.JSONEq(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2},{"channelId":"test-rudder-discards-channel","offset":"1004","table":"RUDDER_DISCARDS","failed":false,"reason":"","count":4}]}`, string(output.ImportingParameters))
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
		require.Equal(t, `{"importId":[{"channelId":"test-users-channel","offset":"1003","table":"USERS","failed":false,"reason":"","count":2}]}`, string(output.ImportingParameters))
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
		require.Equal(t, `{"importId":[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":false,"reason":"","count":2}]}`, string(output.ImportingParameters))
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
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0"}, nil
				},
				"test-users-channel": func() (*model.StatusResponse, error) {
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0"}, nil
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
					return &model.StatusResponse{Valid: true, Success: true, Offset: "0"}, nil
				},
				"test-channel-4": func() (*model.StatusResponse, error) {
					statusCalls += 1
					return &model.StatusResponse{Valid: true, Success: true, Offset: "4"}, nil
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

	t.Run("GetUploadStats with invalid importInfo", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		output := sm.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: "invalid",
		})
		require.Equal(t, http.StatusBadRequest, output.StatusCode)
		require.NotEmpty(t, output.Error)
	})
	t.Run("GetUploadStats success and failed events", func(t *testing.T) {
		failedParameters := `[{"channelId":"test-products-channel","offset":"1003","table":"PRODUCTS","failed":false,"reason":"","count":2},{"channelId":"test-users-channel","offset":"1004","table":"USERS","failed":true,"reason":"users polling failed","count":2}]`
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := New(config.New(), logger.NOP, statsStore, destination)
		output := sm.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: failedParameters,
			ImportingList: []*jobsdb.JobT{
				{
					JobID:        1001,
					EventPayload: []byte(`{"metadata":{"table":"USERS","columns":{"ID":"int","NAME":"string","AGE":"int","RECEIVED_AT":"datetime"}},"data":{"ID":1,"NAME":"Alice","AGE":30,"RECEIVED_AT":"2023-05-12T04:36:50.199Z"}}`),
				},
				{
					JobID:        1002,
					EventPayload: []byte(`{"metadata":{"table":"PRODUCTS","columns":{"ID":"int","PRODUCT_ID":"string","PRICE":"float","IN_STOCK":"boolean","RECEIVED_AT":"datetime"}},"data":{"ID":2,"PRODUCT_ID":"PROD456","PRICE":20.99,"IN_STOCK":true,"RECEIVED_AT":"2023-05-12T04:36:50.199Z"}}`),
				},
				{
					JobID:        1003,
					EventPayload: []byte(`{"metadata":{"table":"PRODUCTS","columns":{"ID":"int","PRODUCT_ID":"string","PRICE":"float","IN_STOCK":"boolean","RECEIVED_AT":"datetime"}},"data":{"ID":2,"PRODUCT_ID":"PROD456","PRICE":20.99,"IN_STOCK":true,"RECEIVED_AT":"2023-05-12T04:36:50.199Z"}}`),
				},
				{
					JobID:        1004,
					EventPayload: []byte(`{"metadata":{"table":"USERS","columns":{"ID":"int","NAME":"string","AGE":"int","RECEIVED_AT":"datetime"}},"data":{"ID":1,"NAME":"Alice","AGE":30,"RECEIVED_AT":"2023-05-12T04:36:50.199Z"}}`),
				},
			},
		})
		require.Equal(t, http.StatusOK, output.StatusCode)
		require.Equal(t, []int64{1001, 1004}, output.Metadata.FailedKeys)
		require.Equal(t, map[int64]string{1001: "users polling failed", 1004: "users polling failed"}, output.Metadata.FailedReasons)
		require.Equal(t, []int64{1002, 1003}, output.Metadata.SucceededKeys)
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
		input     map[string]interface{}
		expected  destConfig
		wantError bool
	}{
		{
			name: "Valid Input",
			input: map[string]interface{}{
				"account":              "test-account",
				"warehouse":            "test-warehouse",
				"database":             "test-database",
				"user":                 "test-user",
				"role":                 "test-role",
				"privateKey":           "test-key",
				"privateKeyPassphrase": "test-passphrase",
				"namespace":            "test-namespace",
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
			},
			wantError: false,
		},
		{
			name: "Invalid Input",
			input: map[string]interface{}{
				"account": 123, // Invalid type
			},
			expected:  destConfig{},
			wantError: true,
		},
		{
			name:  "Empty Map",
			input: map[string]interface{}{},
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
