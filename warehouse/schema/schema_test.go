package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func schemaKey(sourceID, destinationID, namespace string) string {
	return fmt.Sprintf("%s_%s_%s", sourceID, destinationID, namespace)
}

type mockSchemaRepo struct {
	err       error
	schemaMap map[string]model.WHSchema
}

func (m *mockSchemaRepo) GetForNamespace(_ context.Context, sourceID, destinationID, namespace string) (model.WHSchema, error) {
	if m.err != nil {
		return model.WHSchema{}, m.err
	}

	key := schemaKey(sourceID, destinationID, namespace)
	return m.schemaMap[key], nil
}

func (m *mockSchemaRepo) Insert(_ context.Context, schema *model.WHSchema) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}

	key := schemaKey(schema.SourceID, schema.DestinationID, schema.Namespace)
	m.schemaMap[key] = *schema
	return 0, nil
}

type mockStagingFileRepo struct {
	schemas []model.Schema
	err     error
}

func (m *mockStagingFileRepo) GetSchemasByIDs(context.Context, []int64) ([]model.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.schemas, nil
}

type mockFetchSchemaRepo struct {
	schemaInWarehouse             model.Schema
	unrecognizedSchemaInWarehouse model.Schema
	err                           error
}

func (m *mockFetchSchemaRepo) FetchSchema(context.Context) (model.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.schemaInWarehouse, nil
}

func TestSchema_UpdateLocalSchema(t *testing.T) {
	workspaceID := "test-workspace-id"
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	warehouseType := warehouseutils.RS
	tableName := "test_table"

	schemaInWarehouse := model.Schema{
		tableName: model.TableSchema{
			"warehouse_test_int":       "int",
			"warehouse_test_str":       "string",
			"warehouse_test_bool":      "boolean",
			"warehouse_test_float":     "float",
			"warehouse_test_timestamp": "timestamp",
			"warehouse_test_date":      "date",
			"warehouse_test_datetime":  "datetime",
		},
	}

	testCases := []struct {
		name          string
		mockSchema    model.WHSchema
		mockSchemaErr error
		wantSchema    model.Schema
		wantError     error
	}{
		{
			name:          "no schema in db",
			mockSchema:    model.WHSchema{},
			mockSchemaErr: nil,
			wantSchema:    nil,
			wantError:     nil,
		},
		{
			name:          "error in fetching schema from db",
			mockSchema:    model.WHSchema{},
			mockSchemaErr: errors.New("test error"),
			wantSchema:    nil,
			wantError:     errors.New("test error"),
		},
		{
			name: "schema in db",
			mockSchema: model.WHSchema{
				Schema: model.Schema{
					tableName: model.TableSchema{
						"column1": "string",
						"column2": "int",
						"column3": "float",
						"column4": "datetime",
						"column5": "json",
					},
				},
			},
			mockSchemaErr: nil,
			wantSchema: model.Schema{
				tableName: model.TableSchema{
					"column1": "string",
					"column2": "int",
					"column3": "float",
					"column4": "datetime",
					"column5": "json",
				},
			},
			wantError: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockRepo := &mockSchemaRepo{
				err:       tc.mockSchemaErr,
				schemaMap: map[string]model.WHSchema{},
			}

			statsStore, err := memstats.New()
			require.NoError(t, err)

			s := schema{
				warehouse: model.Warehouse{
					WorkspaceID: workspaceID,
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destinationID,
					},
					Namespace: namespace,
					Type:      warehouseType,
				},
				schemaRepo:        mockRepo,
				schemaInWarehouse: schemaInWarehouse,
			}
			tags := stats.Tags{
				"module":        "warehouse",
				"workspaceId":   s.warehouse.WorkspaceID,
				"destType":      s.warehouse.Destination.DestinationDefinition.Name,
				"sourceId":      s.warehouse.Source.ID,
				"destinationId": s.warehouse.Destination.ID,
			}
			s.stats.schemaSize = statsStore.NewTaggedStat("warehouse_schema_size", stats.HistogramType, tags)

			ctx := context.Background()

			err = s.UpdateLocalSchema(ctx, tc.mockSchema.Schema)
			if tc.wantError == nil {
				require.NoError(t, err)
				require.Equal(t, tc.wantSchema, s.localSchema)
				require.Equal(t, tc.wantSchema, mockRepo.schemaMap[schemaKey(sourceID, destinationID, namespace)].Schema)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
				require.Empty(t, s.localSchema)
				require.Empty(t, mockRepo.schemaMap[schemaKey(sourceID, destinationID, namespace)].Schema)
			}
			marshalledSchema, err := json.Marshal(tc.mockSchema.Schema)
			require.NoError(t, err)
			require.EqualValues(t, float64(len(marshalledSchema)), statsStore.Get("warehouse_schema_size", tags).LastValue())

			err = s.UpdateLocalSchemaWithWarehouse(ctx)
			if tc.wantError == nil {
				require.NoError(t, err)
				require.Equal(t, schemaInWarehouse, s.localSchema)
				require.Equal(t, schemaInWarehouse, mockRepo.schemaMap[schemaKey(sourceID, destinationID, namespace)].Schema)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
				require.Empty(t, s.localSchema)
				require.Empty(t, mockRepo.schemaMap[schemaKey(sourceID, destinationID, namespace)].Schema)
				require.EqualValues(t, float64(241), statsStore.Get("warehouse_schema_size", tags).LastValue())
			}
			marshalledSchema, err = json.Marshal(schemaInWarehouse)
			require.NoError(t, err)
			require.EqualValues(t, float64(len(marshalledSchema)), statsStore.Get("warehouse_schema_size", tags).LastValue())
		})
	}
}

func TestSchema_FetchSchemaFromWarehouse(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
	workspaceID := "test-workspace-id"
	tableName := "test-table"
	updatedTable := "updated_test_table"
	updatedSchema := model.TableSchema{
		"updated_test_int":       "int",
		"updated_test_str":       "string",
		"updated_test_bool":      "boolean",
		"updated_test_float":     "float",
		"updated_test_timestamp": "timestamp",
		"updated_test_date":      "date",
		"updated_test_datetime":  "datetime",
	}

	testCases := []struct {
		name           string
		mockSchema     model.Schema
		mockErr        error
		expectedSchema model.Schema
		wantError      error
	}{
		{
			name:           "no schema in warehouse",
			mockSchema:     model.Schema{},
			expectedSchema: model.Schema{},
		},
		{
			name:       "error in fetching schema from warehouse",
			mockSchema: model.Schema{},
			mockErr:    errors.New("test error"),
			wantError:  errors.New("fetching schema from warehouse: test error"),
		},
		{
			name: "no deprecated columns",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
					"test_datetime":  "datetime",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
					"test_datetime":  "datetime",
				},
			},
		},
		{
			name: "invalid deprecated column format",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
		},
		{
			name: "valid deprecated column format",
			mockSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
					"test-deprecated-546a4f59-c303-474e-b2c7-cf37361b5c2f": "int",
					"test-deprecated-c60bf1e9-7cbd-42d4-8a7d-af01f5ff7d8b": "string",
					"test-deprecated-bc3bbc6d-42c9-4d2c-b0e6-bf5820914b09": "boolean",
				},
			},
			expectedSchema: model.Schema{
				tableName: model.TableSchema{
					"test_int":                 "int",
					"test_str":                 "string",
					"test_bool":                "boolean",
					"test_float":               "float",
					"test_timestamp":           "timestamp",
					"test_date":                "date",
					"test_datetime":            "datetime",
					"test-deprecated-column":   "int",
					"test-deprecated-column-1": "string",
					"test-deprecated-column-2": "boolean",
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockRepo := mockFetchSchemaRepo{
				schemaInWarehouse:             tc.mockSchema,
				unrecognizedSchemaInWarehouse: tc.mockSchema,
				err:                           tc.mockErr,
			}

			s := &schema{
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destinationID,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: destType,
						},
					},
					WorkspaceID: workspaceID,
					Namespace:   namespace,
				},
				log: logger.NOP,
			}

			ctx := context.Background()

			require.Empty(t, s.GetTableSchemaInWarehouse(ctx, tableName))
			require.True(t, s.IsWarehouseSchemaEmpty(ctx))

			err := s.FetchSchemaFromWarehouse(ctx, &mockRepo)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
			}
			require.Equal(t, tc.expectedSchema, s.schemaInWarehouse)
			require.Equal(t, tc.expectedSchema[tableName], s.GetTableSchemaInWarehouse(ctx, tableName))
			columnsCount, err := s.GetColumnsCountInWarehouseSchema(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedSchema[tableName]), columnsCount)
			if len(tc.expectedSchema) > 0 {
				require.False(t, s.IsWarehouseSchemaEmpty(ctx))
			} else {
				require.True(t, s.IsWarehouseSchemaEmpty(ctx))
			}
			err = s.UpdateWarehouseTableSchema(ctx, updatedTable, updatedSchema)
			require.NoError(t, err)
			require.Equal(t, updatedSchema, s.GetTableSchemaInWarehouse(ctx, updatedTable))
			require.False(t, s.IsWarehouseSchemaEmpty(ctx))
		})
	}
}

func TestSchema_TableSchemaDiff(t *testing.T) {
	testCases := []struct {
		name              string
		tableName         string
		currentSchema     model.Schema
		uploadTableSchema model.TableSchema
		expected          warehouseutils.TableSchemaDiff
	}{
		{
			name:          "empty current and upload schema",
			tableName:     "test-table",
			currentSchema: model.Schema{},
			expected: warehouseutils.TableSchemaDiff{
				ColumnMap:        model.TableSchema{},
				UpdatedSchema:    model.TableSchema{},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:          "empty current schema",
			tableName:     "test-table",
			currentSchema: model.Schema{},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: true,
				ColumnMap: model.TableSchema{
					"test-column": "test-value",
				},
				UpdatedSchema: model.TableSchema{
					"test-column": "test-value",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "override existing column",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-1",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value-2",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           false,
				TableToBeCreated: false,
				ColumnMap:        model.TableSchema{},
				UpdatedSchema: model.TableSchema{
					"test-column": "test-value-1",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "union of current and upload schema",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "test-value-2",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap: model.TableSchema{
					"test-column": "test-value-2",
				},
				UpdatedSchema: model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
					"test-column":   "test-value-2",
				},
				AlteredColumnMap: model.TableSchema{},
			},
		},
		{
			name:      "override text column with string column",
			tableName: "test-table",
			currentSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column":   "string",
					"test-column-2": "test-value-2",
				},
			},
			uploadTableSchema: model.TableSchema{
				"test-column": "text",
			},
			expected: warehouseutils.TableSchemaDiff{
				Exists:           true,
				TableToBeCreated: false,
				ColumnMap:        model.TableSchema{},
				UpdatedSchema: model.TableSchema{
					"test-column-2": "test-value-2",
					"test-column":   "text",
				},
				AlteredColumnMap: model.TableSchema{
					"test-column": "text",
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := schema{
				schemaInWarehouse: tc.currentSchema,
			}
			diff, err := s.TableSchemaDiff(context.Background(), tc.tableName, tc.uploadTableSchema)
			require.NoError(t, err)
			require.EqualValues(t, diff, tc.expected)
		})
	}
}

func TestSchema_HasLocalSchemaChanged(t *testing.T) {
	testCases := []struct {
		name              string
		localSchema       model.Schema
		schemaInWarehouse model.Schema
		expected          bool
	}{
		{
			name:              "When both schemas are empty and skipDeepEquals is false",
			localSchema:       model.Schema{},
			schemaInWarehouse: model.Schema{},
			expected:          false,
		},
		{
			name:        "When local schema is empty and skipDeepEquals is false",
			localSchema: model.Schema{},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value",
				},
			},
			expected: true,
		},
		{
			name: "same table, same column, different datatype and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-1",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: true,
		},
		{
			name: "same table, different columns and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column-1": "test-value-1",
					"test-column-2": "test-value-2",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "test-value-2",
				},
			},
			expected: true,
		},
		{
			name: "different table and skipDeepEquals is false",
			localSchema: model.Schema{
				"test-table": model.TableSchema{
					"test-column": "string",
				},
			},
			schemaInWarehouse: model.Schema{
				"test-table-1": model.TableSchema{
					"test-column": "text",
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &schema{
				warehouse: model.Warehouse{
					Type: warehouseutils.SNOWFLAKE,
				},
				schemaInWarehouse: tc.schemaInWarehouse,
			}
			require.Equal(t, tc.expected, s.hasSchemaChanged(tc.localSchema))
		})
	}
}

func TestSchema_ConsolidateStagingFilesUsingLocalSchema(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
	workspaceID := "test-workspace-id"

	stagingFiles := lo.RepeatBy(100, func(index int) *model.StagingFile {
		return &model.StagingFile{
			ID: int64(index),
		}
	})

	ctx := context.Background()

	testsCases := []struct {
		name                string
		warehouseType       string
		warehouseSchema     model.Schema
		mockSchemas         []model.Schema
		mockErr             error
		expectedSchema      model.Schema
		wantError           error
		idResolutionEnabled bool
	}{
		{
			name:          "error fetching staging schema",
			warehouseType: warehouseutils.RS,
			mockSchemas:   []model.Schema{},
			mockErr:       errors.New("test error"),
			wantError:     errors.New("getting staging files schema: test error"),
		},

		{
			name:          "discards schema for bigquery",
			warehouseType: warehouseutils.BQ,
			mockSchemas:   []model.Schema{},
			expectedSchema: model.Schema{
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"loaded_at":    "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "discards schema for all destinations",
			warehouseType: warehouseutils.RS,
			mockSchemas:   []model.Schema{},
			expectedSchema: model.Schema{
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "users and identifies should have similar schema except for id and user_id",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "string",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
				},
			},
			expectedSchema: model.Schema{
				"identifies": model.TableSchema{
					"id":               "string",
					"user_id":          "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
				},
				"users": model.TableSchema{
					"id":               "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "users have extra properties as compared to identifies",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "string",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
						"extra_property_1": "string",
						"extra_property_2": "string",
					},
				},
			},
			expectedSchema: model.Schema{
				"identifies": model.TableSchema{
					"id":               "string",
					"user_id":          "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
				},
				"users": model.TableSchema{
					"id":               "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
					"extra_property_1": "string",
					"extra_property_2": "string",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "users without identifies",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"users": model.TableSchema{
						"id":               "string",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
				},
			},
			expectedSchema: model.Schema{
				"users": model.TableSchema{
					"id":               "string",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "unknown table in warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table-1": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "unknown properties in warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_warehouse_int":       "int",
					"test_warehouse_str":       "string",
					"test_warehouse_bool":      "boolean",
					"test_warehouse_float":     "float",
					"test_warehouse_timestamp": "timestamp",
					"test_warehouse_date":      "date",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "single staging schema with empty warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "single staging schema with warehouse schema and text data type override",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_text":      "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_text":      "text",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:                "id resolution without merge schema",
			warehouseType:       warehouseutils.BQ,
			idResolutionEnabled: true,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"loaded_at":    "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:                "id resolution with merge schema",
			warehouseType:       warehouseutils.BQ,
			idResolutionEnabled: true,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
					"rudder_identity_merge_rules": model.TableSchema{},
					"rudder_identity_mappings":    model.TableSchema{},
				},
			},
			expectedSchema: model.Schema{
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"loaded_at":    "datetime",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
				"rudder_identity_mappings": model.TableSchema{
					"merge_property_type":  "string",
					"merge_property_value": "string",
					"rudder_id":            "string",
					"updated_at":           "datetime",
				},
				"rudder_identity_merge_rules": model.TableSchema{
					"merge_property_1_type":  "string",
					"merge_property_1_value": "string",
					"merge_property_2_type":  "string",
					"merge_property_2_value": "string",
				},
				"test-table": model.TableSchema{
					"test_bool":      "boolean",
					"test_date":      "date",
					"test_float":     "float",
					"test_int":       "int",
					"test_str":       "string",
					"test_timestamp": "timestamp",
				},
			},
		},
		{
			name:          "multiple staging schemas with empty warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table-1": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"test-table-2": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple staging schemas with empty warehouse schema and text datatype",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table-1": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_text":      "text",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"test-table-2": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple staging schemas with warehouse schema and text datatype",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "text",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-1": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_text":      "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table-2": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table-1": model.TableSchema{
					"test_int":  "warehouse_int",
					"test_str":  "warehouse_string",
					"test_text": "warehouse_text",
				},
				"test-table-2": model.TableSchema{
					"test_float":     "warehouse_float",
					"test_timestamp": "warehouse_timestamp",
					"test_date":      "warehouse_date",
				},
			},
			expectedSchema: model.Schema{
				"test-table-1": model.TableSchema{
					"test_int":       "warehouse_int",
					"test_str":       "warehouse_string",
					"test_text":      "warehouse_text",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"test-table-2": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "warehouse_float",
					"test_timestamp": "warehouse_timestamp",
					"test_date":      "warehouse_date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple schemas with same table and empty warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":  "int",
						"test_str":  "string",
						"test_bool": "boolean",
					},
				},
				{
					"test-table": model.TableSchema{
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple schemas with same table and warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":  "int",
						"test_str":  "string",
						"test_bool": "boolean",
					},
				},
				{
					"test-table": model.TableSchema{
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":   "warehouse_int",
					"test_float": "warehouse_float",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "warehouse_int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "warehouse_float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple schemas with preference to first schema and empty warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table": model.TableSchema{
						"test_int":       "new_int",
						"test_str":       "new_string",
						"test_bool":      "new_boolean",
						"test_float":     "new_float",
						"test_timestamp": "new_timestamp",
						"test_date":      "new_date",
					},
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "int",
					"test_str":       "string",
					"test_bool":      "boolean",
					"test_float":     "float",
					"test_timestamp": "timestamp",
					"test_date":      "date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "multiple schemas with preference to warehouse schema",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"test-table": model.TableSchema{
						"test_int":       "int",
						"test_str":       "string",
						"test_bool":      "boolean",
						"test_float":     "float",
						"test_timestamp": "timestamp",
						"test_date":      "date",
					},
				},
				{
					"test-table": model.TableSchema{
						"test_int":       "new_int",
						"test_str":       "new_string",
						"test_bool":      "new_boolean",
						"test_float":     "new_float",
						"test_timestamp": "new_timestamp",
						"test_date":      "new_date",
					},
				},
			},
			warehouseSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "warehouse_int",
					"test_str":       "warehouse_string",
					"test_bool":      "warehouse_boolean",
					"test_float":     "warehouse_float",
					"test_timestamp": "warehouse_timestamp",
					"test_date":      "warehouse_date",
				},
			},
			expectedSchema: model.Schema{
				"test-table": model.TableSchema{
					"test_int":       "warehouse_int",
					"test_str":       "warehouse_string",
					"test_bool":      "warehouse_boolean",
					"test_float":     "warehouse_float",
					"test_timestamp": "warehouse_timestamp",
					"test_date":      "warehouse_date",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
		{
			name:          "warehouse users have extra properties as compared to identifies",
			warehouseType: warehouseutils.RS,
			mockSchemas: []model.Schema{
				{
					"identifies": model.TableSchema{
						"id":               "string",
						"user_id":          "int",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
					"users": model.TableSchema{
						"id":               "string",
						"anonymous_id":     "string",
						"received_at":      "datetime",
						"sent_at":          "datetime",
						"timestamp":        "datetime",
						"source_id":        "string",
						"destination_id":   "string",
						"source_type":      "string",
						"destination_type": "string",
					},
				},
			},
			warehouseSchema: model.Schema{
				"identifies": model.TableSchema{
					"id":               "string",
					"user_id":          "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
				},
				"users": model.TableSchema{
					"id":               "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
					"extra_property_1": "string",
					"extra_property_2": "string",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
			expectedSchema: model.Schema{
				"identifies": model.TableSchema{
					"id":               "string",
					"user_id":          "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
					"extra_property_1": "string",
					"extra_property_2": "string",
				},
				"users": model.TableSchema{
					"id":               "int",
					"anonymous_id":     "string",
					"received_at":      "datetime",
					"sent_at":          "datetime",
					"timestamp":        "datetime",
					"source_id":        "string",
					"destination_id":   "string",
					"source_type":      "string",
					"destination_type": "string",
					"extra_property_1": "string",
					"extra_property_2": "string",
				},
				"rudder_discards": model.TableSchema{
					"column_name":  "string",
					"column_value": "string",
					"received_at":  "datetime",
					"row_id":       "string",
					"table_name":   "string",
					"uuid_ts":      "datetime",
					"reason":       "string",
				},
			},
		},
	}
	for _, tc := range testsCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockRepo := &mockStagingFileRepo{
				schemas: tc.mockSchemas,
				err:     tc.mockErr,
			}

			s := &schema{
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: sourceID,
					},
					Destination: backendconfig.DestinationT{
						ID: destinationID,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: destType,
						},
					},
					WorkspaceID: workspaceID,
					Namespace:   namespace,
					Type:        tc.warehouseType,
				},
				log:                              logger.NOP,
				stagingFileRepo:                  mockRepo,
				enableIDResolution:               tc.idResolutionEnabled,
				localSchema:                      tc.warehouseSchema,
				stagingFilesSchemaPaginationSize: 2,
			}

			uploadSchema, err := s.ConsolidateStagingFilesUsingLocalSchema(ctx, stagingFiles)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err, fmt.Sprintf("got error %v, want error %v", err, tc.wantError))
			}
			require.Equal(t, tc.expectedSchema, uploadSchema)
		})
	}
}

func TestSchema_SyncRemoteSchema(t *testing.T) {
	sourceID := "test_source_id"
	destinationID := "test_destination_id"
	namespace := "test_namespace"
	destType := warehouseutils.RS
	workspaceID := "test-workspace-id"
	uploadID := int64(1)
	tableName := "test_table_name"

	t.Run("should return error if unable to fetch local schema", func(t *testing.T) {
		s := &schema{
			warehouse: model.Warehouse{
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
				Destination: backendconfig.DestinationT{
					ID: destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destType,
					},
				},
				WorkspaceID: workspaceID,
				Namespace:   namespace,
				Type:        destType,
			},
			schemaRepo: &mockSchemaRepo{
				err:       errors.New("test error"),
				schemaMap: map[string]model.WHSchema{},
			},
			log: logger.NOP,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, &mockFetchSchemaRepo{}, uploadID)
		require.Error(t, err, "got error %v, want error %v", err, "fetching schema from local: test error")
		require.False(t, schemaChanged)
	})
	t.Run("should return error if unable to fetch remote schema", func(t *testing.T) {
		s := &schema{
			warehouse: model.Warehouse{
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
				Destination: backendconfig.DestinationT{
					ID: destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destType,
					},
				},
				WorkspaceID: workspaceID,
				Namespace:   namespace,
				Type:        destType,
			},
			schemaRepo: &mockSchemaRepo{
				err:       nil,
				schemaMap: map[string]model.WHSchema{},
			},
			log: logger.NOP,
		}

		mockFetchSchemaRepo := &mockFetchSchemaRepo{
			err: errors.New("test error"),
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.Error(t, err, "got error %v, want error %v", err, "fetching schema from warehouse: test error")
		require.False(t, schemaChanged)
	})
	t.Run("schema changed", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		testSchema := model.Schema{
			tableName: model.TableSchema{
				"test_int":       "int",
				"test_str":       "string",
				"test_bool":      "boolean",
				"test_float":     "float",
				"test_timestamp": "timestamp",
				"test_date":      "date",
				"test_datetime":  "datetime",
			},
		}
		schemaInWarehouse := model.Schema{
			tableName: model.TableSchema{
				"warehouse_test_int":       "int",
				"warehouse_test_str":       "string",
				"warehouse_test_bool":      "boolean",
				"warehouse_test_float":     "float",
				"warehouse_test_timestamp": "timestamp",
				"warehouse_test_date":      "date",
				"warehouse_test_datetime":  "datetime",
			},
		}

		key := schemaKey(sourceID, destinationID, namespace)
		mockSchemaRepo := &mockSchemaRepo{
			err: nil,
			schemaMap: map[string]model.WHSchema{
				key: {
					Schema: testSchema,
				},
			},
		}

		s := &schema{
			warehouse: model.Warehouse{
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
				Destination: backendconfig.DestinationT{
					ID: destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destType,
					},
				},
				WorkspaceID: workspaceID,
				Namespace:   namespace,
				Type:        destType,
			},
			schemaRepo: mockSchemaRepo,
			log:        logger.NOP,
		}
		tags := stats.Tags{
			"module":        "warehouse",
			"workspaceId":   s.warehouse.WorkspaceID,
			"destType":      s.warehouse.Destination.DestinationDefinition.Name,
			"sourceId":      s.warehouse.Source.ID,
			"destinationId": s.warehouse.Destination.ID,
		}
		s.stats.schemaSize = statsStore.NewTaggedStat("warehouse_schema_size", stats.HistogramType, tags)

		mockFetchSchemaRepo := &mockFetchSchemaRepo{
			err:                           nil,
			schemaInWarehouse:             schemaInWarehouse,
			unrecognizedSchemaInWarehouse: schemaInWarehouse,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.NoError(t, err)
		require.True(t, schemaChanged)
		require.Equal(t, schemaInWarehouse, s.localSchema)
		require.Equal(t, schemaInWarehouse, mockSchemaRepo.schemaMap[schemaKey(sourceID, destinationID, namespace)].Schema)
		require.Equal(t, schemaInWarehouse, s.schemaInWarehouse)

		marshalledSchema, err := json.Marshal(s.localSchema)
		require.NoError(t, err)
		require.EqualValues(t, float64(len(marshalledSchema)), statsStore.Get("warehouse_schema_size", tags).LastValue())
	})
	t.Run("schema not changed", func(t *testing.T) {
		testSchema := model.Schema{
			tableName: model.TableSchema{
				"test_int":       "int",
				"test_str":       "string",
				"test_bool":      "boolean",
				"test_float":     "float",
				"test_timestamp": "timestamp",
				"test_date":      "date",
				"test_datetime":  "datetime",
			},
		}

		key := schemaKey(sourceID, destinationID, namespace)
		mockSchemaRepo := &mockSchemaRepo{
			err: nil,
			schemaMap: map[string]model.WHSchema{
				key: {
					Schema: testSchema,
				},
			},
		}

		s := &schema{
			warehouse: model.Warehouse{
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
				Destination: backendconfig.DestinationT{
					ID: destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destType,
					},
				},
				WorkspaceID: workspaceID,
				Namespace:   namespace,
				Type:        destType,
			},
			schemaRepo: mockSchemaRepo,
			log:        logger.NOP,
		}

		mockFetchSchemaRepo := &mockFetchSchemaRepo{
			err:                           nil,
			schemaInWarehouse:             testSchema,
			unrecognizedSchemaInWarehouse: testSchema,
		}

		ctx := context.Background()

		schemaChanged, err := s.SyncRemoteSchema(ctx, mockFetchSchemaRepo, uploadID)
		require.NoError(t, err)
		require.False(t, schemaChanged)
		require.Equal(t, testSchema, s.localSchema)
		require.Equal(t, testSchema, s.schemaInWarehouse)
	})
}
