package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestGetColumns(t *testing.T) {
	testCases := []struct {
		name        string
		destType    string
		data        map[string]any
		columnTypes map[string]string
		maxColumns  int32
		expected    map[string]any
		wantError   bool
	}{
		{
			name:     "Basic data types",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean",
			},
		},
		{
			name:     "Basic data types (BQ)",
			destType: whutils.BQ,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean", "loaded_at": "datetime",
			},
		},
		{
			name:     "Basic data types (SNOWFLAKE)",
			destType: whutils.SNOWFLAKE,
			data: map[string]any{
				"FIELD1": "value1", "FIELD2": 123, "FIELD3": true,
			},
			columnTypes: map[string]string{
				"FIELD1": "string", "FIELD2": "int",
			},
			maxColumns: 10,
			expected: map[string]any{
				"UUID_TS": "datetime", "FIELD1": "string", "FIELD2": "int", "FIELD3": "boolean",
			},
		},
		{
			name:     "Key not in columnTypes",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true,
			},
			columnTypes: map[string]string{},
			maxColumns:  10,
			expected: map[string]any{
				"uuid_ts": "datetime", "field1": "string", "field2": "int", "field3": "boolean",
			},
		},
		{
			name:     "Too many columns",
			destType: whutils.POSTGRES,
			data: map[string]any{
				"field1": "value1", "field2": 123, "field3": true, "field4": "extra",
			},
			columnTypes: map[string]string{
				"field1": "string", "field2": "int",
			},
			maxColumns: 3,
			expected:   nil,
			wantError:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			trans := &transformer{}
			trans.config.maxColumnsInEvent = config.SingleValueLoader(int(tc.maxColumns))

			columns, err := trans.getColumns(tc.destType, tc.data, tc.columnTypes)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, columns)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, columns)
		})
	}
}
