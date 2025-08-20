package schemarepository_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestLocalSchemaRepository(t *testing.T) {
	t.Run("Create table", func(t *testing.T) {
		testCases := []struct {
			name        string
			mockError   error
			localSchema model.Schema
			wantError   error
		}{
			{
				name:        "success",
				localSchema: model.Schema{},
			},
			{
				name: "table already exists",
				localSchema: model.Schema{
					"test_table": {
						"test_column_1": "test_type_1",
					},
				},
				wantError: fmt.Errorf("failed to create table: table %s already exists", "test_table"),
			},
			{
				name:        "error updating local schema",
				mockError:   fmt.Errorf("error updating local schema"),
				wantError:   fmt.Errorf("error updating local schema"),
				localSchema: model.Schema{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				warehouse := model.Warehouse{}
				uploader := newMockUploader(t, tc.mockError, tc.localSchema)

				s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
				require.NoError(t, err)

				ctx := context.Background()

				err = s.CreateTable(ctx, "test_table", model.TableSchema{
					"test_column_2": "test_type_2",
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Add columns", func(t *testing.T) {
		testCases := []struct {
			name        string
			mockError   error
			localSchema model.Schema
			wantError   error
		}{
			{
				name: "success",
				localSchema: model.Schema{
					"test_table": {
						"test_column_1": "test_type_1",
					},
				},
			},
			{
				name:      "table does not exists",
				wantError: fmt.Errorf("failed to add column: table %s does not exist", "test_table"),
			},
			{
				name: "error updating local schema",
				localSchema: model.Schema{
					"test_table": {
						"test_column_1": "test_type_1",
					},
				},
				mockError: fmt.Errorf("error updating local schema"),
				wantError: fmt.Errorf("error updating local schema"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				warehouse := model.Warehouse{}
				uploader := newMockUploader(t, tc.mockError, tc.localSchema)

				s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
				require.NoError(t, err)

				ctx := context.Background()

				err = s.AddColumns(ctx, "test_table", []warehouseutils.ColumnInfo{
					{
						Name: "test_column_2",
						Type: "test_type_2",
					},
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Alter columns", func(t *testing.T) {
		testCases := []struct {
			name        string
			mockError   error
			localSchema model.Schema
			wantError   error
		}{
			{
				name: "success",
				localSchema: model.Schema{
					"test_table": {
						"test_column_1": "test_type_1",
					},
				},
			},
			{
				name:      "table does not exists",
				wantError: fmt.Errorf("failed to add column: table %s does not exist", "test_table"),
			},
			{
				name: "column does not exists",
				localSchema: model.Schema{
					"test_table": {
						"test_column_2": "test_type_2",
					},
				},
				wantError: fmt.Errorf("failed to alter column: column %s does not exist in table %s", "test_column_1", "test_table"),
			},
			{
				name: "error updating local schema",
				localSchema: model.Schema{
					"test_table": {
						"test_column_1": "test_type_1",
					},
				},
				mockError: fmt.Errorf("error updating local schema"),
				wantError: fmt.Errorf("error updating local schema"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				warehouse := model.Warehouse{}
				uploader := newMockUploader(t, tc.mockError, tc.localSchema)

				s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
				require.NoError(t, err)

				ctx := context.Background()

				_, err = s.AlterColumn(ctx, "test_table", "test_column_1", "test_type_2")
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Concurrent access", func(t *testing.T) {
		warehouse := model.Warehouse{}
		uploader := newMockUploader(t, nil, model.Schema{})

		s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
		require.NoError(t, err)

		ctx := context.Background()
		wg := sync.WaitGroup{}

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = s.CreateTable(ctx, fmt.Sprintf("test_table_%d", i+1), model.TableSchema{
					"test_column_1": "test_type_1",
					"test_column_2": "test_type_2",
					"test_column_3": "test_type_3",
					"test_column_4": "test_type_4",
				})
				require.NoError(t, err)
			}()
		}
		wg.Wait()

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					err = s.AddColumns(ctx, fmt.Sprintf("test_table_%d", i+1), []warehouseutils.ColumnInfo{
						{
							Name: "test_column_" + strconv.Itoa(j+5),
							Type: "test_type_" + strconv.Itoa(j+5),
						},
					})
					require.NoError(t, err)
				}
			}()
		}
		wg.Wait()

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					_, err = s.AlterColumn(ctx, fmt.Sprintf("test_table_%d", i+1), "test_column_"+strconv.Itoa(j+5), "new_test_type_"+strconv.Itoa(j+5))
					require.NoError(t, err)
				}
			}()
		}
		wg.Wait()

		schema, err := s.FetchSchema(ctx, warehouse)
		require.NoError(t, err)
		require.Equal(t, 1000, len(schema))
		for i := 0; i < 1000; i++ {
			require.Equal(t, 9, len(schema[fmt.Sprintf("test_table_%d", i+1)]))
		}
		for i := 0; i < 1000; i++ {
			for j := 1; j < 5; j++ {
				require.Equal(t, "test_type_"+strconv.Itoa(j), schema[fmt.Sprintf("test_table_%d", i+1)]["test_column_"+strconv.Itoa(j)])
			}
			for j := 5; j < 10; j++ {
				require.Equal(t, "new_test_type_"+strconv.Itoa(j), schema[fmt.Sprintf("test_table_%d", i+1)]["test_column_"+strconv.Itoa(j)])
			}
		}
	})
}

func newMockUploader(
	t testing.TB,
	updateLocalSchemaErr error,
	localSchema model.Schema,
) *mockuploader.MockUploader {
	ctrl := gomock.NewController(t)
	u := mockuploader.NewMockUploader(ctrl)
	u.EXPECT().UpdateLocalSchema(gomock.Any(), gomock.Any()).Return(updateLocalSchemaErr).AnyTimes()
	u.EXPECT().GetLocalSchema(gomock.Any()).Return(localSchema, nil).AnyTimes()
	return u
}
