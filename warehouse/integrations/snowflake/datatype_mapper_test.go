package snowflake

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalculateDataType(t *testing.T) {
	testCases := []struct {
		columnType   string
		numericScale sql.NullInt64
		expected     string
		exists       bool
	}{
		{"VARCHAR", sql.NullInt64{}, "string", true},
		{"INT", sql.NullInt64{}, "int", true},
		{"INT", sql.NullInt64{Int64: 2, Valid: true}, "float", true},
		{"UNKNOWN", sql.NullInt64{}, "", false},
	}

	for _, tc := range testCases {
		dataType, exists := CalculateDataType(tc.columnType, tc.numericScale.Int64)
		require.Equal(t, tc.expected, dataType)
		require.Equal(t, tc.exists, exists)
	}
}
