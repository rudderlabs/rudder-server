package bqstreamv2

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type mockIntegrationManager struct {
	fetchSchemaOutput    func() (whutils.ModelSchema, error)
	createSchemaOutput   func() error
	createTableOutputMap map[string]func() error
	addColumnsOutputMap  map[string]func() error
}

func (m *mockIntegrationManager) FetchSchema(_ context.Context) (whutils.ModelSchema, error) {
	if m.fetchSchemaOutput != nil {
		return m.fetchSchemaOutput()
	}
	return whutils.ModelSchema{}, nil
}

func (m *mockIntegrationManager) CreateSchema(_ context.Context) error {
	if m.createSchemaOutput != nil {
		return m.createSchemaOutput()
	}
	return nil
}

func (m *mockIntegrationManager) CreateTable(_ context.Context, tableName string, _ whutils.ModelTableSchema) error {
	if m.createTableOutputMap != nil {
		return m.createTableOutputMap[tableName]()
	}
	return nil
}

func (m *mockIntegrationManager) AddColumns(_ context.Context, tableName string, _ []whutils.ColumnInfo) error {
	if m.addColumnsOutputMap != nil {
		return m.addColumnsOutputMap[tableName]()
	}
	return nil
}

func (m *mockIntegrationManager) Cleanup(_ context.Context) {
}

type mockStreamWriterFactory struct {
	newStreamWriterOutputMap map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error)
}

func (m *mockStreamWriterFactory) NewStreamWriter(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error) {
	return m.newStreamWriterOutputMap[tableName](ctx, destConf, tableName, tableSchema)
}

type mockStreamWriter struct {
	appendRowsOutput func(ctx context.Context, data [][]byte) (AppendResult, error)
	closeOutput      func() error
}

func (m *mockStreamWriter) AppendRows(ctx context.Context, data [][]byte) (AppendResult, error) {
	return m.appendRowsOutput(ctx, data)
}

func (m *mockStreamWriter) Close() error {
	if m.closeOutput == nil {
		return nil
	}
	return m.closeOutput()
}

type mockAppendResult struct {
	getResultOutput func(ctx context.Context) (int64, error)
}

func (m *mockAppendResult) GetResult(ctx context.Context) (int64, error) {
	return m.getResultOutput(ctx)
}

func TestBQStreamV2(t *testing.T) {
	destination := &backendconfig.DestinationT{
		ID:          "test-destination",
		WorkspaceID: "test-workspace",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "BQSTREAM_V2",
		},
		Config: make(map[string]any),
	}
	validations.Init()

	noOpStreamWriterFn := func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { //nolint:unparam
		output := &mockStreamWriter{
			appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
				output := &mockAppendResult{
					getResultOutput: func(ctx context.Context) (int64, error) {
						return 0, nil
					},
				}
				return output, nil
			},
			closeOutput: func() error {
				return nil
			},
		}
		return output, nil
	}

	t.Run("Upload: Invalid file path", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "opening async file")
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 1, statsStore.Get("bqstream_v2_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "BQSTREAM_V2",
			"destinationId": "test-destination",
			"status":        "aborted",
		}).LastValue())
	})

	t.Run("Upload: Invalid records", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/invalid_records.txt",
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "unmarshalling event line")
		require.Equal(t, "test-destination", output.DestinationID)
		require.EqualValues(t, 1, statsStore.Get("bqstream_v2_jobs", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "BQSTREAM_V2",
			"destinationId": "test-destination",
			"status":        "aborted",
		}).LastValue())
	})

	t.Run("Upload: error fetching schema during manager creation", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return nil, fmt.Errorf("failed to create integration manager")
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Equal(t, 1, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create integration manager")
	})

	t.Run("Upload: error fetching schema", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					return nil, fmt.Errorf("failed to fetch schema")
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Equal(t, 1, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to fetch schema")
	})

	t.Run("Upload: error fetching schema as abortable error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					return nil, status.Errorf(codes.PermissionDenied, "failed to fetch schema")
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "failed to fetch schema")
	})

	t.Run("Upload: error fetching schema during creation as abortable error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					return whutils.ModelSchema{}, nil
				},
				createSchemaOutput: func() error {
					return status.Errorf(codes.PermissionDenied, "failed to create schema")
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "creating schema in warehouse")
	})

	t.Run("Upload: error fetching schema during creation", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					return whutils.ModelSchema{}, nil
				},
				createSchemaOutput: func() error {
					return fmt.Errorf("failed to create schema")
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Equal(t, 1, output.FailedCount)
		require.Contains(t, output.FailedReason, "creating schema in warehouse")
	})

	t.Run("Upload: ignore already exist error during schema creation", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createSchemaOutput: func() error {
					e := googleapi.Error{
						Code:    409,
						Message: "already exists in schema",
					}
					return &e
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
	})

	t.Run("Upload: error creating discards table", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createTableOutputMap: map[string]func() error{
					"rudder_discards": func() error {
						return fmt.Errorf("failed to create discards table")
					},
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1}, output.FailedJobIDs)
		require.Equal(t, 1, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create discards table")
	})

	t.Run("Upload: error creating discards table as abortable error", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createTableOutputMap: map[string]func() error{
					"rudder_discards": func() error {
						return status.Errorf(codes.PermissionDenied, "failed to create discards table")
					},
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "failed to create discards table")
	})

	t.Run("Upload: ignore already exist error during discards table creation", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createTableOutputMap: map[string]func() error{
					"rudder_discards": func() error {
						e := googleapi.Error{
							Code:    409,
							Message: "already exists in schema",
						}
						return &e
					},
					"products": func() error { return nil },
					"users":    func() error { return nil },
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
	})

	t.Run("Upload: error adding columns to discards table", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"rudder_discards": {
							"table_name":   "string",
							"row_id":       "string",
							"column_name":  "string",
							"column_value": "string",
							"received_at":  "datetime",
							"uuid_ts":      "datetime",
						},
					}
					return output, nil
				},
				addColumnsOutputMap: map[string]func() error{
					"rudder_discards": func() error {
						return fmt.Errorf("failed to add columns to discards table")
					},
				},
			}
			return output, nil
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1}, output.FailedJobIDs)
		require.Equal(t, 1, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to add columns to discards table")
	})

	t.Run("Upload: error creating events table", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createTableOutputMap: map[string]func() error{
					"products": func() error {
						return fmt.Errorf("failed to create products table")
					},
					"users":           func() error { return nil },
					"rudder_discards": func() error { return nil },
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create products table")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: ignore already exist error during events table creation", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				createTableOutputMap: map[string]func() error{
					"products": func() error {
						e := googleapi.Error{
							Code:    409,
							Message: "already exists in schema",
						}
						return &e
					},
					"users": func() error {
						e := googleapi.Error{
							Code:    409,
							Message: "already exists in schema",
						}
						return &e
					},
					"rudder_discards": func() error { return nil },
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Contains(t, output.FailedReason, "no warehouse schema found for table")

		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"product_id": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
			}
			return output, nil
		}
		output = sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
	})

	t.Run("Upload: error adding columns to events table", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"product_id": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
				addColumnsOutputMap: map[string]func() error{
					"products": func() error {
						return fmt.Errorf("failed to add columns to products table")
					},
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to add columns to products table")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error creating discards writer", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"rudder_discards": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) {
					return nil, fmt.Errorf("failed to create discards writer")
				},
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create discards writer")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error appending discarded rows", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"rudder_discards": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { // nolint:unparam
					output := &mockStreamWriter{
						appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
							return nil, fmt.Errorf("failed to append discarded rows")
						},
					}
					return output, nil
				},
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to append discarded rows")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error getting result of discarded rows", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"rudder_discards": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { // nolint:unparam
					output := &mockStreamWriter{
						appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
							output := &mockAppendResult{
								getResultOutput: func(ctx context.Context) (int64, error) {
									return 0, fmt.Errorf("failed to get result of discarded rows")
								},
							}
							return output, nil
						},
					}
					return output, nil
				},
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to get result of discarded rows")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error creating events writer", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users": noOpStreamWriterFn,
				"products": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) {
					return nil, fmt.Errorf("failed to create events writer")
				},
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create events writer")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error appending events rows", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users": noOpStreamWriterFn,
				"products": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { // nolint:unparam
					output := &mockStreamWriter{
						appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
							return nil, fmt.Errorf("failed to append products rows")
						},
					}
					return output, nil
				},
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to append products rows")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: error getting result of events rows", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users": noOpStreamWriterFn,
				"products": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { // nolint:unparam
					output := &mockStreamWriter{
						appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
							output := &mockAppendResult{
								getResultOutput: func(ctx context.Context) (int64, error) {
									return 0, fmt.Errorf("failed to get result of products rows")
								},
							}
							return output, nil
						},
					}
					return output, nil
				},
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to get result of products rows")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
	})

	t.Run("Upload: duplicate ids found in the events", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_duplicate_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008}, output.SucceededJobIDs)
		require.EqualValues(t, 2, statsStore.Get("bqstream_v2_duplicate_events", stats.Tags{
			"module":        "batch_router",
			"workspaceId":   "test-workspace",
			"destType":      "BQSTREAM_V2",
			"destinationId": "test-destination",
			"reason":        "batch",
		}).LastValue())
	})

	t.Run("Upload: splits events into multiple requests if they exceed max insert request size", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		maxInsertRequestSizeBytes := 8 * bytesize.MB

		conf := config.New()
		conf.Set("BQStreamV2.maxBufferCapacity", int64(64*1024*1024))

		sm := NewManager(conf, logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}

		appendCalls := atomic.Int64{}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) { //nolint:unparam
					output := &mockStreamWriter{
						appendRowsOutput: func(ctx context.Context, data [][]byte) (AppendResult, error) {
							appendCalls.Add(1)
							outputResult := &mockAppendResult{
								getResultOutput: func(ctx context.Context) (int64, error) {
									return 0, nil
								},
							}
							return outputResult, nil
						},
					}
					return output, nil
				},
			},
		}

		f, err := os.CreateTemp("", "bqstream-v2-chunking-*.txt")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(f.Name()) })
		t.Cleanup(func() { _ = f.Close() })

		baseTS := "2023-05-12T04:36:50.199Z"
		largeName := strings.Repeat("a", int(maxInsertRequestSizeBytes-int64(20000)))

		_, err = fmt.Fprintf(f,
			`{"message":{"metadata":{"table":"users","columns":{"id":"int","name":"string","age":"int","received_at":"datetime"}},"data":{"id":1,"name":"%s","age":30,"received_at":"%s"}},"metadata":{"job_id":1001}}`+"\n",
			largeName, baseTS,
		)
		require.NoError(t, err)
		_, err = fmt.Fprintf(f,
			`{"message":{"metadata":{"table":"users","columns":{"id":"int","name":"string","age":"int","received_at":"datetime"}},"data":{"id":1,"name":"%s","age":30,"received_at":"%s"}},"metadata":{"job_id":1002}}`+"\n",
			largeName, baseTS,
		)
		require.NoError(t, err)
		_, err = fmt.Fprintf(f,
			`{"message":{"metadata":{"table":"users","columns":{"id":"int","name":"string","age":"int","received_at":"datetime"}},"data":{"id":1,"name":"%s","age":30,"received_at":"%s"}},"metadata":{"job_id":1003}}`+"\n",
			largeName, baseTS,
		)
		require.NoError(t, err)
		_, err = fmt.Fprintf(f,
			`{"message":{"metadata":{"table":"users","columns":{"id":"int","name":"string","age":"int","received_at":"datetime"}},"data":{"id":3,"name":"small","age":30,"received_at":"%s"}},"metadata":{"job_id":1004}}`+"\n",
			baseTS,
		)
		require.NoError(t, err)
		require.NoError(t, f.Sync())

		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination: destination,
			FileName:    f.Name(),
			Count:       3,
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
		require.Equal(t, int64(3), appendCalls.Load())
	})

	t.Run("Upload: table schema is cached upon fetching from warehouse", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		fetchSchemaCalls := atomic.Int64{}

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					fetchSchemaCalls.Add(1)
					return whutils.ModelSchema{
						"users":    {"id": "int", "name": "string", "age": "int", "received_at": "datetime"},
						"products": {"id": "int", "name": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}, nil
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}

		for range 10 {
			output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1},
				Destination:     destination,
				FileName:        "testdata/successful_records.txt",
			})
			require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
		}
		require.Equal(t, int64(1), fetchSchemaCalls.Load())
	})

	t.Run("Upload: no need to fetch schema from warehouse if it is already cached", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		now := time.Now()
		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.schemaCache.Set("users", whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, now)
		sm.schemaCache.Set("products", whutils.ModelTableSchema{"id": "int", "name": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, now)
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}

		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
	})

	t.Run("Upload: need to fetch schema from warehouse even if one of the tables is not found in the cache", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		fetchSchemaCalls := atomic.Int64{}

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					fetchSchemaCalls.Add(1)
					return whutils.ModelSchema{
						"users":    {"id": "int", "name": "string", "age": "int", "received_at": "datetime"},
						"products": {"id": "int", "name": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}, nil
				},
			}
			return output, nil
		}
		sm.schemaCache.Set("users", whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, timeutil.Now())
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}

		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
		require.Equal(t, int64(1), fetchSchemaCalls.Load())
	})

	t.Run("Upload: upon creation of events table, table schema is cached", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		now := timeutil.Now()

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
		require.EqualValues(t, int64(3), sm.schemaCache.Len())
		productsSchema, ok := sm.schemaCache.Get("products", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "product_id": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, productsSchema)
		usersSchema, ok := sm.schemaCache.Get("users", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, usersSchema)
		discardsSchema, ok := sm.schemaCache.Get("rudder_discards", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"column_name": "string", "column_value": "string", "reason": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime"}, discardsSchema)
	})

	t.Run("Upload: upon addition of columns to events table, table schema is cached with the modified schema", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		now := timeutil.Now()

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			output := &mockIntegrationManager{
				fetchSchemaOutput: func() (whutils.ModelSchema, error) {
					output := whutils.ModelSchema{
						"products": {"product_id": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"},
					}
					return output, nil
				},
			}
			return output, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
		require.EqualValues(t, int64(3), sm.schemaCache.Len())
		productsSchema, ok := sm.schemaCache.Get("products", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "product_id": "string", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, productsSchema)
		usersSchema, ok := sm.schemaCache.Get("users", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, usersSchema)
		discardsSchema, ok := sm.schemaCache.Get("rudder_discards", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"column_name": "string", "column_value": "string", "reason": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime"}, discardsSchema)
	})

	t.Run("Upload: invalidation of table schema cache upon error creating discards writer", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		now := timeutil.Now()

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.schemaCache.Set("products", whutils.ModelTableSchema{"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, now)
		sm.schemaCache.Set("users", whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, now)
		sm.schemaCache.Set("rudder_discards", whutils.ModelTableSchema{"column_name": "string", "column_value": "string", "reason": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime"}, now)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"rudder_discards": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) {
					return nil, fmt.Errorf("failed to create discards writer")
				},
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create discards writer")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
		require.EqualValues(t, int64(2), sm.schemaCache.Len())
		productsSchema, ok := sm.schemaCache.Get("products", timeutil.Now())
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, productsSchema)
		usersSchema, ok := sm.schemaCache.Get("users", timeutil.Now())
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, usersSchema)
	})

	t.Run("Upload: invalidation of table schema cache upon error creating events writer", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		now := timeutil.Now()

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.schemaCache.Set("products", whutils.ModelTableSchema{"id": "int", "product_id": "boolean", "price": "float", "in_stock": "boolean", "received_at": "datetime"}, now)
		sm.schemaCache.Set("users", whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, now)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"users":           noOpStreamWriterFn,
				"rudder_discards": noOpStreamWriterFn,
				"products": func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error) {
					return nil, fmt.Errorf("failed to create events writer")
				},
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1002, 1004}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "failed to create events writer")
		require.ElementsMatch(t, []int64{1001, 1003}, output.SucceededJobIDs)
		require.EqualValues(t, int64(2), sm.schemaCache.Len())
		usersSchema, ok := sm.schemaCache.Get("users", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"id": "int", "name": "string", "age": "int", "received_at": "datetime"}, usersSchema)
		discardsSchema, ok := sm.schemaCache.Get("rudder_discards", now)
		require.True(t, ok)
		require.Equal(t, whutils.ModelTableSchema{"column_name": "string", "column_value": "string", "reason": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime"}, discardsSchema)
	})

	t.Run("Upload: successful", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		sm := NewManager(config.New(), logger.NOP, statsStore, destination)
		sm.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
			return &mockIntegrationManager{}, nil
		}
		sm.streamWriterFactory = &mockStreamWriterFactory{
			newStreamWriterOutputMap: map[string]func(_ context.Context, _ destConfig, _ string, _ whutils.ModelTableSchema) (StreamWriter, error){
				"users":    noOpStreamWriterFn,
				"products": noOpStreamWriterFn,
			},
		}
		output := sm.Upload(context.Background(), &common.AsyncDestinationStruct{
			ImportingJobIDs: []int64{1},
			Destination:     destination,
			FileName:        "testdata/successful_records.txt",
		})
		require.ElementsMatch(t, []int64{1001, 1002, 1003, 1004}, output.SucceededJobIDs)
	})
}
