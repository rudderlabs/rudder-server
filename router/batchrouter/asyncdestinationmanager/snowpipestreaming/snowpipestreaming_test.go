package snowpipestreaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestFindNewColumns(t *testing.T) {
	tests := []struct {
		name           string
		eventSchema    whutils.ModelTableSchema
		snowPipeSchema whutils.ModelTableSchema
		expected       []whutils.ColumnInfo
	}{
		{
			name: "new column with different data type in event schema",
			eventSchema: whutils.ModelTableSchema{
				"new_column":      "STRING",
				"existing_column": "FLOAT",
			},
			snowPipeSchema: whutils.ModelTableSchema{
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
			snowPipeSchema: whutils.ModelTableSchema{
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
			snowPipeSchema: whutils.ModelTableSchema{},
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
			snowPipeSchema: whutils.ModelTableSchema{
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
			snowPipeSchema: whutils.ModelTableSchema{
				"existing_column1": "STRING",
				"existing_column2": "FLOAT",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name:        "event schema is empty, SnowPipe schema has columns",
			eventSchema: whutils.ModelTableSchema{},
			snowPipeSchema: whutils.ModelTableSchema{
				"existing_column": "STRING",
			},
			expected: []whutils.ColumnInfo{},
		},
		{
			name: "SnowPipe schema is nil",
			eventSchema: whutils.ModelTableSchema{
				"new_column": "STRING",
			},
			snowPipeSchema: nil,
			expected: []whutils.ColumnInfo{
				{Name: "new_column", Type: "STRING"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findNewColumns(tt.eventSchema, tt.snowPipeSchema)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestDestConfig_Decode(t *testing.T) {
	tests := []struct {
		name        string
		input       map[string]interface{}
		expected    destConfig
		expectedErr bool
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
			expectedErr: false,
		},
		{
			name: "Invalid Input",
			input: map[string]interface{}{
				"account": 123, // Invalid type
			},
			expected:    destConfig{},
			expectedErr: true,
		},
		{
			name:  "Empty Map",
			input: map[string]interface{}{},
			expected: destConfig{
				Namespace: "STRINGEMPTY",
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config destConfig
			err := config.Decode(tt.input)

			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, config)
			}
		})
	}
}
