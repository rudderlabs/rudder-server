package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestTypeRegex(t *testing.T) {
	testCases := []struct {
		input, expected string
	}{
		{"", ""},
		{"VARCHAR", "VARCHAR"},
		{"NUMBER(38,0)", "NUMBER"},
		{"TIMESTAMP_TZ(9)", "TIMESTAMP_TZ"},
		{"VARCHAR(16777216)", "VARCHAR"},
		{"CustomType('abc', 'def')", "CustomType"},
		{"CustomType('abc', 'def')", "CustomType"},
		{"RandomType(anything!)", "RandomType"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expected, reType.ReplaceAllString(tc.input, "$1"))
	}
}

func TestChannelResponse_SnowPipeSchema(t *testing.T) {
	testCases := []struct {
		name        string
		tableSchema map[string]map[string]interface{}
		expected    whutils.ModelTableSchema
	}{
		{
			name: "Valid types with scale",
			tableSchema: map[string]map[string]interface{}{
				"column1": {"type": "VARCHAR(16777216)"},
				"column2": {"type": "NUMBER(2,0)", "scale": 2.0},
				"column3": {"type": "NUMBER(2,0)", "scale": 0.0},
				"column4": {"type": "NUMBER(2,0)", "scale": 0},
				"column5": {"type": "BOOLEAN"},
				"column6": {"type": "TIMESTAMP_TZ(9)", "scale": float64(9)},
			},
			expected: whutils.ModelTableSchema{
				"column1": "string",
				"column2": "float",
				"column3": "int",
				"column4": "int",
				"column5": "boolean",
				"column6": "datetime",
			},
		},
		{
			name: "Invalid type field",
			tableSchema: map[string]map[string]interface{}{
				"column1": {"type": 12345},
			},
			expected: whutils.ModelTableSchema{},
		},
		{
			name: "Missing scale for number",
			tableSchema: map[string]map[string]interface{}{
				"column1": {"type": "NUMBER(2,0)"},
			},
			expected: whutils.ModelTableSchema{
				"column1": "int",
			},
		},
		{
			name:        "Empty table schema",
			tableSchema: map[string]map[string]interface{}{},
			expected:    whutils.ModelTableSchema{},
		},
		{
			name: "Type with regex cleaning",
			tableSchema: map[string]map[string]interface{}{
				"column1": {"type": "VARCHAR(255)"},
			},
			expected: whutils.ModelTableSchema{
				"column1": "string",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &ChannelResponse{}
			c.TableSchema = tc.tableSchema
			require.Equal(t, tc.expected, c.SnowPipeSchema())
		})
	}
}
