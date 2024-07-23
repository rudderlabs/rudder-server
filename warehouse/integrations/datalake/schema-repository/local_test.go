package schemarepository_test

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestLocalSchemaRepository_CreateTable(t *testing.T) {
	t.Parallel()

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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
}

func TestLocalSchemaRepository_AddColumns(t *testing.T) {
	t.Parallel()

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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
}

func TestLocalSchemaRepository_AlterColumn(t *testing.T) {
	t.Parallel()

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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
