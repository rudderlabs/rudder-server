package bqstreamallevents

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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

func TestCheckAndIgnoreAlreadyExistError(t *testing.T) {
	require.True(t, checkAndIgnoreAlreadyExistError(&googleapi.Error{Code: 409}))
	require.True(t, checkAndIgnoreAlreadyExistError(fmt.Errorf("creating table: %w", &googleapi.Error{Code: 400, Message: `column "x" already exists in schema`})))
	require.False(t, checkAndIgnoreAlreadyExistError(&googleapi.Error{Code: 400, Message: "bad request"}))
	require.False(t, checkAndIgnoreAlreadyExistError(errors.New("not a googleapi error")))
}

func TestShouldAbort(t *testing.T) {
	require.True(t, shouldAbort(status.Error(codes.PermissionDenied, "permission denied")))
	require.True(t, shouldAbort(status.Error(codes.Unauthenticated, "unauthenticated")))
	require.False(t, shouldAbort(status.Error(codes.Unavailable, "retryable")))
	require.False(t, shouldAbort(errors.New("plain error")))
}
